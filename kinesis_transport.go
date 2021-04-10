package mercure

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
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
	logger                    Logger
	client                    *kinesis.Client
	streamName                string
	subscribers               map[*Subscriber]struct{}
	historySubscribers        map[*Subscriber]struct{}
	closed                    chan struct{}
	closedOnce                sync.Once
	lastEventID               string
	context                   context.Context
	shards                    *[]types.Shard
	shardCount                int
	isLocalKinesis            bool
	earliestDuration          time.Duration
}

func getAwsCredentialsProvider() aws.CredentialsProviderFunc {
	return func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}, nil
	}
}

func GetAwsCfg() (aws.Config, error) {
	return awsConfig.LoadDefaultConfig(
		context.TODO(),
		awsConfig.WithCredentialsProvider(getAwsCredentialsProvider()),
	)
}

func GetAwsLocalCfg(u *url.URL) (aws.Config, error) {
	endpointResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:           "http://" + u.Host,
			SigningRegion: os.Getenv("AWS_REGION"),
		}, nil
	})

	return awsConfig.LoadDefaultConfig(
		context.TODO(),
		awsConfig.WithCredentialsProvider(getAwsCredentialsProvider()),
		awsConfig.WithEndpointResolver(endpointResolver),
	)
}

func NewKinesisTransport(u *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) {
	var cfg aws.Config
	var err error
	var isLocalKinesis bool

	if "mercure-transport" == u.Hostname() {
		cfg, err = GetAwsLocalCfg(u)
		isLocalKinesis = true
	} else {
		cfg, err = GetAwsCfg()
		isLocalKinesis = false
	}

	if err != nil {
		l.Error("Unable to load AWS configs", zap.Error(err))
		return nil, err
	}

	client := kinesis.NewFromConfig(cfg)

	transport := &KinesisTransport{
		logger:                    l,
		client:                    client,
		streamName:                os.Getenv("KINESIS_STREAM_NAME"),
		subscribers:               make(map[*Subscriber]struct{}),
		historySubscribers:        make(map[*Subscriber]struct{}),
		closed:                    make(chan struct{}),
		lastEventID:               fmt.Sprintf("%d", time.Now().UnixNano()),
		context:                   context.TODO(),
		isLocalKinesis:            isLocalKinesis,
		earliestDuration:          time.Minute * -5, // minutes
	}

	transport.loadShards()

	transport.SubscribeToMessageStream()
	go transport.ConsumeHistorySubscribers()

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

func (t *KinesisTransport) loadShards() {
	result, err := t.client.ListShards(t.context, &kinesis.ListShardsInput{
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
		t.context,
		&kinesis.PutRecordInput{
			StreamName:   aws.String(t.streamName),
			PartitionKey: &topic,
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
		go func(wg *sync.WaitGroup, shard types.Shard) {
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

func (t *KinesisTransport) getShardIterator(shardId *string, iteratorType types.ShardIteratorType, from *time.Time) (*string, error) {
	input := &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(t.streamName),
		ShardId:           shardId,
		ShardIteratorType: iteratorType,
	}

	if from != nil {
		input.Timestamp = from
	}

	result, err := t.client.GetShardIterator(t.context, input)

	if err != nil {
		return nil, err
	}

	return result.ShardIterator, nil
}

func (t *KinesisTransport) SubscribeToMessageStream() {
	for _, shard := range *t.shards {
		go func(shard types.Shard) {
			t.ConsumeShard(shard.ShardId, nil, nil)
		}(shard)
	}
}

func (t *KinesisTransport) ConsumeHistorySubscribers() {
	for {
		select {
		case <-t.closed:
			return
		default:
			if len(t.historySubscribers) == 0 {
				time.Sleep(time.Millisecond * 1100)
				continue
			}
			t.dispatchHistory(t.GetHistorySubscribers(), t.StringToTime(t.lastEventID))
		}
	}
}

func (t *KinesisTransport) ConsumeShard(shardID *string, historySubscribers []*Subscriber, timeTo *time.Time) string {
	var timeFrom *time.Time
	var lastEventId string
	var currentIterator *string
	var err error

	fromHistory := historySubscribers != nil
	iteratorType := types.ShardIteratorTypeLatest

	if fromHistory {
		defaultTimeFrom := time.Now().Add(t.earliestDuration)
		timeFrom = &defaultTimeFrom

		if !t.isLocalKinesis {
			iteratorType = types.ShardIteratorTypeAtTimestamp
		} else {
			iteratorType = types.ShardIteratorTypeTrimHorizon
		}
	}

	currentIterator, err = t.getShardIterator(shardID, iteratorType, timeFrom)

	if err != nil {
		t.logger.Error(fmt.Sprintf("Failed getting shard iterator, type: %s, shardID: %s", *shardID, iteratorType), zap.Error(err))
	}

	for currentIterator != nil {
		select {
		case <-t.closed:
			lastEventId = EarliestLastEventID
		default:
			recordsOutput, err := t.client.GetRecords(t.context, &kinesis.GetRecordsInput{
				ShardIterator: currentIterator,
				Limit: aws.Int32(1000),
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

				if fromHistory {
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
				} else {
					t.lastEventID = lastEventId
					_, subscribers, _ := t.GetSubscribers()
					for _, subscriber := range subscribers {
						if !subscriber.Dispatch(update, false) {
							t.CloseSubscriberChannel(subscriber)
						}
					}
				}

			}

			currentIterator = recordsOutput.NextShardIterator

			if len(recordsOutput.Records) == 0 {
				if fromHistory == true {
					// History is sent; no need to consume any more
					return lastEventId
				}
				time.Sleep(time.Millisecond * 1100)
			}
		}
	}
	return lastEventId
}

var (
	_ Transport            = (*KinesisTransport)(nil)
	_ TransportSubscribers = (*KinesisTransport)(nil)
)
