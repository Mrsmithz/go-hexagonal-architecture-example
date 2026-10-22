//go:build integration

package kafka

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/metrics"
	"example.com/go-backend-example/internal/adapter/sqlite"
	"example.com/go-backend-example/internal/consumer"
	"example.com/go-backend-example/internal/message"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func testEventDelivery(t *testing.T, ctx context.Context, container *kafkamodule.KafkaContainer, brokers []string) {
	const topic = "integration-events"
	createTopic(t, ctx, container, topic, 1)
	cfg := integrationConfig(brokers, topic, "event-delivery")
	cfg.Kafka.MaxPollRecords = 3
	cfg.Topics[0].ProcessingConcurrency = 3
	cfg.Topics[0].BatchTimeout = 750 * time.Millisecond
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	invalid := []byte{255, '{', 0}
	records := []*kgo.Record{
		{Topic: topic, Value: []byte(`{"user_id":101,"event":"login","timestamp":1729526400}`)},
		{Topic: topic, Key: []byte{0, 255}, Value: invalid, Headers: []kgo.RecordHeader{{Key: "trace", Value: []byte{255}}, {Key: "trace", Value: nil}}},
		{Topic: topic, Value: []byte(`{"user_id":101,"event":"logout","timestamp":0}`)},
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	dsn := filepath.Join(t.TempDir(), "events.db")
	store := sqlite.New(dsn)
	if err := store.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	service := message.NewService(store, zap.NewNop())
	consumerMetrics := metrics.New(cfg.Topics)
	firstCtx, cancelFirst := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFirst()
	factory, err := NewClientFactory(firstCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var commits, firstPollSize, attempted atomic.Int32
	err = consumer.Run(firstCtx, cfg, consumerMetrics.Wrap(func(ctx context.Context, record consumer.Message) error {
		attempted.Add(1)
		return service.Handle(ctx, record)
	}), func(config consumer.Config, topic consumer.TopicConfig) (*deliveryProbe, error) {
		client, err := factory(config, topic)
		return &deliveryProbe{Client: client, commits: &commits, firstPollSize: &firstPollSize}, err
	})
	if err == nil || !strings.Contains(err.Error(), "dead-letter") {
		t.Fatalf("missing DLQ did not fail processing: %v", err)
	}
	if firstPollSize.Load() != 3 || attempted.Load() != 3 || commits.Load() != 0 {
		t.Fatalf("polled=%d attempted=%d commit attempts=%d; want 3,3,0", firstPollSize.Load(), attempted.Load(), commits.Load())
	}
	assertEventCount(t, ctx, db, 2)
	if offset := committedEventOffset(t, ctx, producer, cfg, topic); offset != -1 {
		t.Fatalf("failed DLQ advanced committed offset to %d", offset)
	}

	createTopic(t, ctx, container, cfg.Topics[0].DeadLetterTopic(), 1)
	cfg.Topics[0].BatchTimeout = 5 * time.Second
	retryCtx, cancelRetry := context.WithTimeout(ctx, 20*time.Second)
	defer cancelRetry()
	factory, err = NewClientFactory(retryCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	replayed := make(chan int64, 3)
	err = consumer.Run(retryCtx, cfg, consumerMetrics.Wrap(func(ctx context.Context, record consumer.Message) error {
		replayed <- record.Offset
		return service.Handle(ctx, record)
	}), func(config consumer.Config, topic consumer.TopicConfig) (*observedClient, error) {
		client, err := factory(config, topic)
		return &observedClient{Client: client, onCommit: func(records []consumer.Message) error {
			commits.Add(1)
			cancelRetry()
			return nil
		}}, err
	})
	if err != nil || commits.Load() != 1 || len(replayed) != 3 {
		t.Fatalf("replay error=%v commit calls=%d replayed=%d", err, commits.Load(), len(replayed))
	}
	seen := make(map[int64]bool)
	for range 3 {
		offset := <-replayed
		if offset < 0 || offset > 2 || seen[offset] {
			t.Fatalf("whole-batch replay produced duplicate or unexpected offset %d", offset)
		}
		seen[offset] = true
	}
	assertEventCount(t, ctx, db, 2)
	if offset := committedEventOffset(t, ctx, producer, cfg, topic); offset != 3 {
		t.Fatalf("resolved batch committed offset=%d, want 3", offset)
	}
	var zeroTimestamp int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_events WHERE timestamp = 0").Scan(&zeroTimestamp); err != nil || zeroTimestamp != 1 {
		t.Fatalf("zero timestamps=%d, error=%v", zeroTimestamp, err)
	}

	dlq := cfg.Topics[0].DeadLetterTopic()
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dlq: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	readCtx, cancelRead := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRead()
	fetches := reader.PollRecords(readCtx, 1)
	if errs := fetches.Errors(); len(errs) != 0 {
		t.Fatal(errs)
	}
	deadLetters := fetches.Records()
	if len(deadLetters) != 1 {
		t.Fatalf("DLQ records=%d, want 1", len(deadLetters))
	}
	var envelope deadLetterEnvelope
	if err := json.Unmarshal(deadLetters[0].Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SourceTopic != topic || envelope.SourceOffset != 1 || !bytes.Equal(envelope.Value, invalid) || !bytes.Equal(envelope.Key, records[1].Key) || len(envelope.Headers) != 2 || !strings.Contains(envelope.Error, "decode event") || string(deadLetters[0].Key) != topic+":0:1" {
		t.Fatalf("DLQ did not preserve failed event: %+v", envelope)
	}
	scrape := httptest.NewRecorder()
	consumerMetrics.Handler().ServeHTTP(scrape, httptest.NewRequest("GET", "/metrics", nil))
	for status, count := range map[string]int{"success": 4, "failed": 2} {
		for _, metric := range []string{"kafka_consumer_messages_processed_total", "kafka_consumer_message_processing_duration_seconds_count"} {
			want := fmt.Sprintf(`%s{status="%s",topic="%s"} %d`, metric, status, topic, count)
			if !strings.Contains(scrape.Body.String(), want) {
				t.Fatalf("missing metric %q in %s", want, scrape.Body.String())
			}
		}
	}
}

type deliveryProbe struct {
	*Client
	commits       *atomic.Int32
	firstPollSize *atomic.Int32
}

func (c *deliveryProbe) Poll(ctx context.Context, limit int) ([]consumer.Message, error) {
	records, err := c.Client.Poll(ctx, limit)
	if len(records) != 0 {
		c.firstPollSize.CompareAndSwap(0, int32(len(records)))
	}
	return records, err
}

func (c *deliveryProbe) Commit(ctx context.Context, records ...consumer.Message) error {
	c.commits.Add(1)
	return c.Client.Commit(ctx, records...)
}

func assertEventCount(t *testing.T, ctx context.Context, db *sql.DB, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_events").Scan(&count); err != nil || count != want {
		t.Fatalf("persisted events=%d, want %d; error=%v", count, want, err)
	}
}

func committedEventOffset(t *testing.T, ctx context.Context, client *kgo.Client, cfg consumer.Config, topic string) int64 {
	t.Helper()
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Groups = []kmsg.OffsetFetchRequestGroup{{Group: cfg.Kafka.GroupID + "." + topic, Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{0}}}}}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 || len(response.Groups[0].Topics) != 1 || len(response.Groups[0].Topics[0].Partitions) != 1 || response.Groups[0].Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("unexpected offset response: %+v", response)
	}
	return response.Groups[0].Topics[0].Partitions[0].Offset
}

func testDeadLetterInFlightCancellation(t *testing.T, ctx context.Context, container *kafkamodule.KafkaContainer, brokers []string) {
	const topic = "cancellation-dlq"
	createTopic(t, ctx, container, topic, 1)
	dropped := make(chan struct{})
	var dropOnce sync.Once
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation(),
		kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &withheldProduceReply{Conn: conn, dropped: func() { dropOnce.Do(func() { close(dropped) }) }}, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	adapter := &Client{client: client, dlqTopic: topic}
	publishCtx, cancelPublish := context.WithCancel(ctx)
	defer cancelPublish()
	done := make(chan error, 1)
	go func() {
		done <- adapter.PublishDeadLetter(publishCtx, consumer.Message{Topic: "source"}, errors.New("failed"))
	}()
	select {
	case <-dropped:
	case err := <-done:
		t.Fatalf("publish returned before a broker reply was withheld: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("no in-flight produce reply observed")
	}
	cancelPublish()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight publication = %v, want cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight DLQ publication did not cancel before client close")
	}
}

type withheldProduceReply struct {
	net.Conn
	produceIDs sync.Map
	pending    []byte
	dropped    func()
}

func (c *withheldProduceReply) Write(data []byte) (int, error) {
	if len(data) >= 12 && binary.BigEndian.Uint16(data[4:6]) == 0 {
		c.produceIDs.Store(binary.BigEndian.Uint32(data[8:12]), struct{}{})
	}
	return c.Conn.Write(data)
}

func (c *withheldProduceReply) Read(data []byte) (int, error) {
	for len(c.pending) == 0 {
		var prefix [4]byte
		if _, err := io.ReadFull(c.Conn, prefix[:]); err != nil {
			return 0, err
		}
		size := int(binary.BigEndian.Uint32(prefix[:]))
		if size < 4 || size > 100<<20 {
			return 0, fmt.Errorf("unexpected Kafka response size %d", size)
		}
		frame := make([]byte, 4+size)
		copy(frame, prefix[:])
		if _, err := io.ReadFull(c.Conn, frame[4:]); err != nil {
			return 0, err
		}
		if _, produce := c.produceIDs.LoadAndDelete(binary.BigEndian.Uint32(frame[4:8])); produce {
			c.dropped()
			continue
		}
		c.pending = frame
	}
	n := copy(data, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
