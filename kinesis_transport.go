package mercure

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/kinesis"
    "go.uber.org/zap"
    "net/url"
    "os"
    "strconv"
    "sync"
    "time"
)

func init() {
    RegisterTransportFactory("kinesis", NewKinesisTransport)
}

type KinesisTransport struct {
    sync.RWMutex
    logger                  Logger
    client                  *kinesis.Kinesis
    streamName              string
    streamArn               *string
    consumerArn             *string
    consumerNames           []string
    subscribers             map[*Subscriber]struct{}
    historySubscribers      map[*Subscriber]struct{}
    closed                  chan struct{}
    closedOnce              sync.Once
    lastEventID             string
    context                 context.Context
    shards                  *[]*kinesis.Shard
    shardCount              int
    isLocalKinesis          bool
    earliestDuration        time.Duration
    consumerDuration        time.Duration
    consumerSleep           time.Duration
    idleHistorySleep        time.Duration
    deleteConsumerAfter     time.Duration
    manageConsumersInterval time.Duration
}

func GetAwsCfg(isLocalKinesis bool, endpoint *string) (config *aws.Config) {
    config = &aws.Config{
        Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
    }

    if isLocalKinesis {
        config.Endpoint = endpoint
        config.DisableSSL = aws.Bool(true)
    }

    return config
}

func NewKinesisTransport(u *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) {
    var err error
    var isLocalKinesis bool

    if "mercure-transport" == u.Hostname() {
        isLocalKinesis = true
    } else {
        isLocalKinesis = false
    }

    newSession, err := session.NewSession(GetAwsCfg(isLocalKinesis, &u.Host))
    if err != nil {
        return nil, err
    }
    client := kinesis.New(newSession)

    transport := &KinesisTransport{
        logger:                  l,
        client:                  client,
        streamName:              os.Getenv("KINESIS_STREAM_NAME"),
        subscribers:             make(map[*Subscriber]struct{}),
        historySubscribers:      make(map[*Subscriber]struct{}),
        closed:                  make(chan struct{}),
        lastEventID:             fmt.Sprintf("%d", time.Now().UnixNano()),
        context:                 context.TODO(),
        isLocalKinesis:          isLocalKinesis,
        earliestDuration:        time.Minute * -5,
        consumerDuration:        time.Minute * 3,
        consumerSleep:           time.Millisecond * 500,
        idleHistorySleep:        time.Millisecond * 1000,
        deleteConsumerAfter:     time.Minute * 20,
        manageConsumersInterval: time.Minute * 10,
    }

    transport.loadShards()
    transport.LoadStreamArn()
    transport.ManageConsumers()
    transport.consumerArn = transport.GetStreamConsumer()

    if transport.consumerArn == nil {
        transport.logger.Error("Couldn't create stream consumer")
        panic("Couldn't create stream consumer")
    }

    go transport.SubscribeToMessageStream()
    go transport.ConsumeHistorySubscribers()

    go func() {
        for {
            time.Sleep(transport.manageConsumersInterval)
            transport.ManageConsumers()
        }
    }()

    return transport, nil
}

func (t *KinesisTransport) StringToTime(unix string) *time.Time {
    i, err := strconv.ParseInt(unix, 10, 64)
    if err != nil {
        t.logger.Error("Can't parse timestamp", zap.Error(err))
        return nil
    }

    parsedTime := time.Unix(0, i)

    return &parsedTime
}

func (t *KinesisTransport) TimeToString(time *time.Time) string {
    return fmt.Sprintf("%d", time.UnixNano())
}

func (t *KinesisTransport) LoadStreamArn() {
    result, err := t.client.DescribeStream(&kinesis.DescribeStreamInput{
        StreamName: aws.String(t.streamName),
    })

    if err != nil {
        t.logger.Error("Error while getting stream description", zap.Error(err))
    }

    t.streamArn = result.StreamDescription.StreamARN
}

func (t *KinesisTransport) GetConsumerDescription(consumerName string) (*kinesis.DescribeStreamConsumerOutput, error) {
    return t.client.DescribeStreamConsumer(&kinesis.DescribeStreamConsumerInput{
        StreamARN:    t.streamArn,
        ConsumerName: aws.String(consumerName),
    })
}

func (t *KinesisTransport) ManageConsumers() {
    for _, consumerName := range t.consumerNames {
        t.TagConsumer(consumerName)
    }
    t.CleanIdleConsumers()
}

func (t *KinesisTransport) CreateStreamConsumer(consumerName string) (consumerArn *string) {
    result, err := t.client.RegisterStreamConsumer(&kinesis.RegisterStreamConsumerInput{
        ConsumerName: aws.String(consumerName),
        StreamARN:    t.streamArn,
    })

    if err != nil {
        t.logger.Error("Error while registering consumer", zap.Error(err))
        return
    }

    for i := 0; i < 20; i++ {
        resp, err := t.GetConsumerDescription(consumerName)
        if err != nil || aws.StringValue(resp.ConsumerDescription.ConsumerStatus) != kinesis.ConsumerStatusActive {
            t.logger.Warn("Created consumer inactive. Waiting for 5 seconds", zap.Error(err))
            time.Sleep(time.Second * 5)
            continue
        }
    }

    return result.Consumer.ConsumerARN
}

func (t *KinesisTransport) DeleteStreamConsumer(consumerName string) {
    _, err := t.client.DeregisterStreamConsumer(&kinesis.DeregisterStreamConsumerInput{
        StreamARN:    t.streamArn,
        ConsumerName: aws.String(consumerName),
    })

    if err != nil {
        t.logger.Error("Error de-registering stream consumer", zap.Error(err))
        return
    }

    for i := 0; i < 20; i++ {
        _, err := t.GetConsumerDescription(consumerName)
        if err != nil {
            switch err.(type) {
            case *kinesis.ResourceNotFoundException:
                return
            }
        }
        t.logger.Warn("Deleted consumer still exists. Waiting for 5 seconds", zap.Error(err))
        time.Sleep(time.Second * 5)
        continue
    }
}

func (t *KinesisTransport) GetStreamConsumer() (consumerArn *string) {
    for i := 1; i < 20; i++ {
        consumerName := fmt.Sprintf("mercure-hub-consumer-%d", i)
        if t.IsConsumerFree(consumerName) {
            t.consumerNames = append(t.consumerNames, consumerName)
            t.TagConsumer(consumerName)
            consumerArn = t.CreateStreamConsumer(consumerName)
            break
        }
    }
    return consumerArn
}

func (t *KinesisTransport) IsConsumerFree(consumerName string) bool {
    result, err := t.client.ListTagsForStream(&kinesis.ListTagsForStreamInput{
        StreamName: aws.String(t.streamName),
    })

    if err != nil {
        t.logger.Error("Failed getting tags for stream", zap.Error(err))
        return false
    }

    for _, tag := range result.Tags {
        if *tag.Key == consumerName && tag.Value != nil {
            return false
        }
    }

    return true
}

func (t *KinesisTransport) GetIdleConsumerNames() (consumers []string) {
    result, err := t.client.ListTagsForStream(&kinesis.ListTagsForStreamInput{
        StreamName: aws.String(t.streamName),
    })

    if err != nil {
        t.logger.Error("Failed getting tags for stream", zap.Error(err))
        return consumers
    }

    for _, tag := range result.Tags {
        if tag.Key != nil && tag.Value != nil {
            if t.StringToTime(*tag.Value).Before(time.Now().Add(-t.deleteConsumerAfter)) {
                consumers = append(consumers, *tag.Key)
            }
        }
    }

    return consumers
}

func (t *KinesisTransport) CleanIdleConsumers() {
    for _, consumerName := range t.GetIdleConsumerNames() {
        t.DeleteStreamConsumer(consumerName)
        _, err := t.client.RemoveTagsFromStream(&kinesis.RemoveTagsFromStreamInput{
            StreamName: aws.String(t.streamName),
            TagKeys: []*string{
                &consumerName,
            },
        })

        if err != nil {
            t.logger.Error("Couldn't delete consumer", zap.String("TagKey", consumerName))
        }
    }
}

func (t *KinesisTransport) TagConsumer(consumerName string) {
    currentTime := time.Now()
    timestamp := t.TimeToString(&currentTime)
    _, err := t.client.AddTagsToStream(&kinesis.AddTagsToStreamInput{
        StreamName: aws.String(t.streamName),
        Tags: map[string]*string{
            consumerName: &timestamp,
        },
    })

    if err != nil {
        t.logger.Error("Failed to tag consumer", zap.Error(err))
    }
}

func (t *KinesisTransport) loadShards() {
    result, err := t.client.ListShards(&kinesis.ListShardsInput{
        StreamName: aws.String(t.streamName),
    })

    if err != nil {
        t.logger.Error("Failed getting shards", zap.Error(err))
        return
    }

    t.shardCount = len(result.Shards)
    t.shards = &result.Shards
}

func (t *KinesisTransport) getLastEventID() string {
    lastEventID := "0"

    return lastEventID
}

func (t *KinesisTransport) Dispatch(update *Update) error {
    select {
    case <-t.closed:
        return ErrClosedTransport
    default:
    }

    updateJSON, err := json.Marshal(*update)

    if err != nil {
        t.logger.Error("Error when marshaling update", zap.Error(err))
    }

    err = t.persist(update.Topics[0], updateJSON)

    if err != nil {
        return err
    }

    return nil
}

func (t *KinesisTransport) persist(topic string, updateJSON []byte) (err error) {
    _, err = t.client.PutRecord(
        &kinesis.PutRecordInput{
            StreamName:   aws.String(t.streamName),
            PartitionKey: aws.String(topic),
            Data:         updateJSON,
        },
    )

    if err != nil {
        t.logger.Error("Error when putting record", zap.Error(err))
        return err
    }

    return nil
}

func (t *KinesisTransport) AddSubscriber(s *Subscriber) error {
    select {
    case <-t.closed:
        return ErrClosedTransport
    default:
    }

    t.Lock()
    t.subscribers[s] = struct{}{}
    if s.RequestLastEventID != "" {
        t.historySubscribers[s] = struct{}{}
    }
    t.Unlock()

    return nil
}

func (t *KinesisTransport) GetSubscribers() (string, []*Subscriber, error) {
    t.RLock()
    defer t.RUnlock()
    subscribers := make([]*Subscriber, len(t.subscribers))

    i := 0
    for subscriber := range t.subscribers {
        subscribers[i] = subscriber
        i++
    }

    return t.lastEventID, subscribers, nil
}

func (t *KinesisTransport) GetHistorySubscribers() []*Subscriber {
    t.RLock()
    defer t.RUnlock()
    subscribers := make([]*Subscriber, len(t.subscribers))

    i := 0
    for subscriber := range t.historySubscribers {
        subscribers[i] = subscriber
        i++
    }
    t.historySubscribers = make(map[*Subscriber]struct{})

    return subscribers
}

func (t *KinesisTransport) dispatchHistory(subscribers []*Subscriber, toTime *time.Time) {
    var wg sync.WaitGroup

    for _, shard := range *t.shards {
        wg.Add(1)
        go func(wg *sync.WaitGroup, shard *kinesis.Shard) {
            defer wg.Done()
            t.ConsumeShard(shard.ShardId, subscribers, toTime)
        }(&wg, shard)
    }
    wg.Wait()

    for _, subscriber := range subscribers {
        if subscriber != nil {
            subscriber.HistoryDispatched(t.TimeToString(toTime))
        }
    }
}

func (t *KinesisTransport) Close() (err error) {
    select {
    case <-t.closed:
        // Already closed. Don't close again.
    default:
        t.closedOnce.Do(func() {
            t.Lock()
            defer t.Unlock()
            close(t.closed)
            for subscriber := range t.subscribers {
                t.CloseSubscriberChannel(subscriber)
            }
        })
    }
    return nil
}

func (t *KinesisTransport) CloseSubscriberChannel(subscriber *Subscriber) {
    t.RLock()
    defer t.RUnlock()

    delete(t.subscribers, subscriber)
    delete(t.historySubscribers, subscriber)
}

func (t *KinesisTransport) getShardIterator(shardId *string, iteratorType *string, from *time.Time) (*string, error) {
    input := &kinesis.GetShardIteratorInput{
        StreamName:        aws.String(t.streamName),
        ShardId:           shardId,
        ShardIteratorType: iteratorType,
    }

    if from != nil {
        input.Timestamp = from
    }

    result, err := t.client.GetShardIterator(input)

    if err != nil {
        return nil, err
    }

    return result.ShardIterator, nil
}

func (t *KinesisTransport) SubscribeToMessageStream() {
    var wg sync.WaitGroup
    timestamp := time.Now()

    for {
        select {
        case <-t.closed:
            return
        default:
            for _, shard := range *t.shards {
                wg.Add(1)
                go func(wg *sync.WaitGroup, shard *kinesis.Shard) {
                    defer wg.Done()
                    t.SubscribeToShard(shard, timestamp)
                }(&wg, shard)
            }
            wg.Wait()
            timestamp = time.Now()
            time.Sleep(t.consumerSleep)
        }
    }
}

func (t *KinesisTransport) SubscribeToShard(shard *kinesis.Shard, timestamp time.Time) {
    var lastEventId string

    ctx, cancelFn := context.WithTimeout(context.Background(), t.consumerDuration)
    defer cancelFn()

    result, err := t.client.SubscribeToShardWithContext(ctx, &kinesis.SubscribeToShardInput{
        ConsumerARN: t.consumerArn,
        ShardId:     shard.ShardId,
        StartingPosition: &kinesis.StartingPosition{
            Type:      aws.String(kinesis.ShardIteratorTypeAtTimestamp),
            Timestamp: aws.Time(timestamp),
        },
    })

    if err != nil {
        t.logger.Error("Error subscribing to shard", zap.Error(err))
        return
    }

    defer result.EventStream.Close()

    for event := range result.EventStream.Events() {
        switch e := event.(type) {
        case *kinesis.SubscribeToShardEvent:
            for _, record := range e.Records {
                var update *Update
                if err := json.Unmarshal(record.Data, &update); err != nil {
                    t.logger.Warn(fmt.Sprintf("Couldn't JSON Load Entry ID: %s", *record.SequenceNumber), zap.Error(err))
                    continue
                }

                lastEventId = t.TimeToString(record.ApproximateArrivalTimestamp)
                update.ID = lastEventId

                t.lastEventID = lastEventId
                _, subscribers, _ := t.GetSubscribers()
                for _, subscriber := range subscribers {
                    if !subscriber.Dispatch(update, false) {
                        t.CloseSubscriberChannel(subscriber)
                    }
                }
            }
        }
    }

    if err := result.EventStream.Err(); err != nil && !os.IsTimeout(err) {
        t.logger.Error("Error while consuming subscribed shard", zap.Error(err))
    }
}

func (t *KinesisTransport) ConsumeHistorySubscribers() {
    for {
        select {
        case <-t.closed:
            return
        default:
            if len(t.historySubscribers) == 0 {
                time.Sleep(t.idleHistorySleep)
                continue
            }
            t.dispatchHistory(t.GetHistorySubscribers(), t.StringToTime(t.lastEventID))
        }
    }
}

func (t *KinesisTransport) ConsumeShard(shardID *string, historySubscribers []*Subscriber, timeTo *time.Time) string {
    var timeFrom *time.Time
    var iteratorType, lastEventId string
    var currentIterator *string
    var err error

    defaultTimeFrom := time.Now().Add(t.earliestDuration)
    timeFrom = &defaultTimeFrom

    if !t.isLocalKinesis {
        iteratorType = kinesis.ShardIteratorTypeAtTimestamp
    } else {
        iteratorType = kinesis.ShardIteratorTypeTrimHorizon
    }

    currentIterator, err = t.getShardIterator(shardID, &iteratorType, timeFrom)

    if err != nil {
        t.logger.Error(fmt.Sprintf("Failed getting shard iterator, type: %s, shardID: %s", *shardID, iteratorType), zap.Error(err))
    }

    for currentIterator != nil {
        select {
        case <-t.closed:
            lastEventId = EarliestLastEventID
        default:
            recordsOutput, err := t.client.GetRecords(&kinesis.GetRecordsInput{
                ShardIterator: currentIterator,
                Limit:         aws.Int64(1000),
            })
            if err != nil {
                t.logger.Error("Failed getting records", zap.Error(err))
            }

            for _, record := range recordsOutput.Records {
                if t.isLocalKinesis && timeFrom != nil && record.ApproximateArrivalTimestamp.Before(*timeFrom) {
                    continue
                }

                var update *Update
                if err := json.Unmarshal(record.Data, &update); err != nil {
                    t.logger.Warn(fmt.Sprintf("Couldn't JSON Load Entry ID: %s", *record.SequenceNumber), zap.Error(err))
                    continue
                }

                lastEventId = t.TimeToString(record.ApproximateArrivalTimestamp)
                update.ID = lastEventId

                for _, subscriber := range historySubscribers {
                    if subscriber == nil || subscriber.RequestLastEventID == "" {
                        continue
                    }

                    subscriberFrom := timeFrom
                    if subscriber.RequestLastEventID != EarliestLastEventID {
                        subscriberFrom = t.StringToTime(subscriber.RequestLastEventID)
                    }

                    if subscriberFrom != nil && record.ApproximateArrivalTimestamp.Before(*subscriberFrom) {
                        continue
                    }

                    if timeTo != nil && record.ApproximateArrivalTimestamp.After(*timeTo) {
                        continue
                    }

                    if !subscriber.Dispatch(update, true) {
                        t.CloseSubscriberChannel(subscriber)
                    }
                }

            }

            currentIterator = recordsOutput.NextShardIterator

            return lastEventId
        }
    }
    return lastEventId
}

var (
    _ Transport            = (*KinesisTransport)(nil)
    _ TransportSubscribers = (*KinesisTransport)(nil)
)
