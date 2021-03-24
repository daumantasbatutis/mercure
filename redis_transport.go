package mercure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

func init() {
	RegisterTransportFactory("redis", NewRedisTransport)
}

const defaultRedisStreamCount = "20"
const defaultRedisSize = "0"
const redisIdKey = "a083bc6440b213e3bec4b7c92bc21f39"

func redisNilToNil(err error) error {
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

type RedisTransport struct {
	sync.RWMutex
	logger      Logger
	client      *redis.ClusterClient
	streamCount int64
	size        int64
	subscribers map[*Subscriber]struct{}
	closed      chan struct{}
	closedOnce  sync.Once
	lastSeq     string
	lastEventID string
	url         *url.URL
}

func parseValueFromQuery(q url.Values, paramName string, defaultValue string) (value string) {
	if q.Get(paramName) != "" {
		value := q.Get(paramName)
		q.Del(paramName)
		return value
	}
	return defaultValue
}

func createRedisClient(hosts []string, l Logger) (client *redis.ClusterClient, err error) {
	client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: hosts,
	})

	if _, err := client.Ping().Result(); err != nil {
		l.Error("Redis connection error", zap.Error(err))
		return nil, err
	}

	return client, err
}

func NewRedisTransport(u *url.URL, l Logger, tss *TopicSelectorStore) (Transport, error) {
	hosts := strings.Split(u.Host, ",")
	query := u.Query()

	streamCountParameter := parseValueFromQuery(query, "stream_count", defaultRedisStreamCount)
	sizeParameter := parseValueFromQuery(query, "size", defaultRedisSize)

	streamCount, err := strconv.ParseInt(streamCountParameter, 10, 64)
	if err != nil {
		l.Error("Unable to parse stream_count parameter from URL", zap.Error(err))
		return nil, err
	}

	size, err := strconv.ParseInt(sizeParameter, 10, 64)
	if err != nil {
		l.Error("Unable to parse size parameter", zap.Error(err))
		return nil, err
	}

	client, err := createRedisClient(hosts, l)
	if err != nil {
		return nil, err
	}

	transport := &RedisTransport{
		logger:      l,
		client:      client,
		streamCount: streamCount,
		size:        size,
		url:         u,
		subscribers: make(map[*Subscriber]struct{}),
		closed:      make(chan struct{}),
		lastSeq:     "+",
	}

	transport.lastEventID = transport.getLastEventID()

	for _, streamName := range transport.getStreamNames() {
		go transport.SubscribeToMessageStream(streamName)
	}

	return transport, nil
}

func (t *RedisTransport) getStreamName(topic string) string {
	hashHex := md5.Sum([]byte(topic))
	hashString := hex.EncodeToString(hashHex[:])
	hashInt, _ := strconv.ParseInt(hashString[:4], 16, 64)

	return fmt.Sprintf("updates_%d", int(hashInt) % int(t.streamCount))
}

func (t *RedisTransport) getStreamNames() []string {
	var streamNames []string
	i := 0
	for i < int(t.streamCount) {
		streamNames = append(streamNames, fmt.Sprintf("updates_%d", i))
		i++
	}
	return streamNames
}

func (t *RedisTransport) idEncrypt (id string, streamName string) string {
	block, err := aes.NewCipher([]byte(redisIdKey))
	if err != nil {
		return id
	}

	encrypted := make([]byte, aes.BlockSize+len(streamName + "||" + id))

	iv := encrypted[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return id
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(encrypted[aes.BlockSize:], []byte(streamName + "||" + id))

	return base64.StdEncoding.EncodeToString(encrypted)
}

func (t *RedisTransport) idDecrypt (encryptedId string) string {
	encryptedBytes, err := base64.StdEncoding.DecodeString(encryptedId)
	if err != nil {
		return encryptedId
	}

	block, err := aes.NewCipher([]byte(redisIdKey))
	if err != nil {
		return encryptedId
	}

	iv := encryptedBytes[:aes.BlockSize]

	encryptedBytes = encryptedBytes[aes.BlockSize:]
	stream := cipher.NewCFBDecrypter(block, iv)
	decryptedBytes := make([]byte, len(encryptedBytes))
	stream.XORKeyStream(decryptedBytes, encryptedBytes)

	return strings.Split(fmt.Sprintf("%s", decryptedBytes), "||")[1]
}

func (t *RedisTransport) getLastEventID() string {
	lastEventID := "0"
	for _, streamName := range t.getStreamNames() {
		messages, err := t.client.XRevRangeN(streamName, "+", "-", 1).Result()
		if err != nil {
			return lastEventID
		}

		for _, entry := range messages {
			if lastEventID < entry.ID {
				lastEventID = entry.ID
			}
		}
	}

	if lastEventID == "0" {
		lastEventID = EarliestLastEventID
	}

	return lastEventID
}

func (t *RedisTransport) Dispatch(update *Update) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	_, err := t.persist(update)
	if err != nil {
		return err
	}

	return nil
}

func (t *RedisTransport) persist(update *Update) (updateID string, err error) {
	updateID = ""
	streamName := t.getStreamName(update.Topics[0])
	updateJSON, err := json.Marshal(*update)

	updateID, err = t.client.XAdd(&redis.XAddArgs{
		Stream: streamName,
		MaxLenApprox: t.size,
		ID: "*",
		Values: map[string]interface{}{
			"data": updateJSON,
		},
	}).Result()

	if err != nil {
		return updateID, redisNilToNil(err)
	}
	update.ID = t.idEncrypt(updateID, streamName)

	return update.ID, nil
}

func (t *RedisTransport) AddSubscriber(s *Subscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	t.subscribers[s] = struct{}{}
	toSeq := t.lastSeq
	t.Unlock()

	if s.RequestLastEventID != "" {
		responseLastEventID := s.RequestLastEventID
		for _, streamName := range t.getStreamNames() {
			newResponseLastEventID := t.dispatchHistory(streamName, s, toSeq)

			if newResponseLastEventID > responseLastEventID {
				responseLastEventID = newResponseLastEventID
			}
		}
		s.HistoryDispatched(responseLastEventID)
		s.historySent = true
	}
	s.historySent = true
	return nil
}

func (t *RedisTransport) GetSubscribers() (string, []*Subscriber, error) {
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

func (t *RedisTransport) dispatchHistory(streamName string, s *Subscriber, toSeq string) string {
	if toSeq == "" {
		toSeq = "+"
	}

	fromSeq := s.RequestLastEventID
	responseLastEventID := s.RequestLastEventID

	if fromSeq == EarliestLastEventID {
		fromSeq = "-"
	}

	messages, err := t.client.XRange(streamName, t.idDecrypt(fromSeq), t.idDecrypt(toSeq)).Result()
	if err != nil {
		return responseLastEventID
	}

	for _, entry := range messages {
		message, ok := entry.Values["data"]
		if !ok {
			return responseLastEventID
		}

		var update *Update
		if err := json.Unmarshal([]byte(fmt.Sprintf("%v", message)), &update); err != nil {
			return responseLastEventID
		}

		update.ID = t.idEncrypt(entry.ID, streamName)

		if !s.Dispatch(update, true) {
			return responseLastEventID
		}
		responseLastEventID = entry.ID
	}

	return responseLastEventID
}

func (t *RedisTransport) Close() (err error) {
	select {
	case <-t.closed:
		// Already closed. Don't close again.
	default:
		t.closedOnce.Do(func() {
			t.Lock()
			defer t.Unlock()
			close(t.closed)
			for subscriber := range t.subscribers {
				t.closeSubscriberChannel(subscriber)
			}
		})
	}
	return nil
}

func (t *RedisTransport) SubscribeToMessageStream(streamName string) {
	streamArgs := &redis.XReadArgs{Streams: []string{streamName, "$"}, Count: 1, Block: 10000}
	for {
		select {
		case <-t.closed:
			return
		default:
			streams, err := t.client.XRead(streamArgs).Result()
			if err != nil {
				continue
			}

			entry := streams[0].Messages[0]
			message, ok := entry.Values["data"]
			if !ok {
				streamArgs.Streams[1] = entry.ID
				t.logger.Warn(fmt.Sprintf("Couldn't Decode Entry. Last Entry ID: %s", streamArgs.Streams[1]))
				continue
			}

			var update *Update
			if err := json.Unmarshal([]byte(fmt.Sprintf("%v", message)), &update); err != nil {
				streamArgs.Streams[1] = entry.ID
				t.logger.Warn(fmt.Sprintf("Couldn't JSON Load Entry ID: %s", entry.ID))
				continue
			}

			_, subscribers, _ := t.GetSubscribers()
			sent := 0

			for _, subscriber := range subscribers {
				if !subscriber.historySent {
					continue
				}

				update.ID = t.idEncrypt(entry.ID, streamName)
				if !subscriber.Dispatch(update, false) {
					t.closeSubscriberChannel(subscriber)
					continue
				}
				sent++
			}

			streamArgs.Streams[1] = entry.ID
			t.lastEventID = update.ID
		}
	}
}

func (t *RedisTransport) closeSubscriberChannel(subscriber *Subscriber) {
	t.Lock()
	defer t.Unlock()
	delete(t.subscribers, subscriber)
	runtime.GC()
}

var (
	_ Transport            = (*RedisTransport)(nil)
	_ TransportSubscribers = (*RedisTransport)(nil)
)
