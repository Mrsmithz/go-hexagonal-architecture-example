//go:build integration

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"

	"example.com/go-backend-example/internal/consumer"
)

const kafkaTestImage = "docker.io/confluentinc/cp-kafka:8.0.0"

func TestKafkaConsumerIntegration(t *testing.T) {
	ctx := testContext(t)
	container, brokers := startKafka(t, ctx)

	t.Run("batch limit, partition distribution, shared concurrency and commits", func(t *testing.T) {
		topic := "integration-distribution"
		createTopic(t, ctx, container, topic, 2)
		const recordsCount = 24

		cfg := integrationConfig(brokers, topic, "integration-distribution-group")
		cfg.Topics[0].Consumers = 2
		cfg.Kafka.MaxPollRecords = 3
		cfg.Topics[0].ProcessingConcurrency = 2
		started, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		received := make(chan consumer.Message, recordsCount)
		commitSizes := make(chan int, recordsCount)
		var active atomic.Int32
		var peak atomic.Int32
		var finished sync.Map
		var clientsMu sync.Mutex
		var clients []*observedClient
		factory, err := NewClientFactory(started, cfg)
		if err != nil {
			t.Fatal(err)
		}
		newObservedClient := func(config consumer.Config, topicConfig consumer.TopicConfig) (*observedClient, error) {
			client, err := factory(config, topicConfig)
			if err != nil {
				return nil, err
			}
			observed := &observedClient{Client: client}
			observed.onCommit = func(messages []consumer.Message) error {
				for _, message := range messages {
					if _, ok := finished.Load(messageKey{partition: message.Partition, offset: message.Offset}); !ok {
						return fmt.Errorf("offset %d committed before its handler finished", message.Offset)
					}
				}
				clientsMu.Lock()
				for _, message := range messages {
					observed.partitions[message.Partition] = true
				}
				observed.committed += len(messages)
				clientsMu.Unlock()
				commitSizes <- len(messages)
				return nil
			}
			observed.partitions = make(map[int32]bool)
			clientsMu.Lock()
			clients = append(clients, observed)
			clientsMu.Unlock()
			return observed, nil
		}
		done := make(chan error, 1)
		go func() {
			done <- consumer.Run(started, cfg, func(ctx context.Context, message consumer.Message) error {
				current := active.Add(1)
				for {
					previous := peak.Load()
					if current <= previous || peak.CompareAndSwap(previous, current) {
						break
					}
				}
				select {
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					active.Add(-1)
					return ctx.Err()
				}
				active.Add(-1)
				finished.Store(messageKey{partition: message.Partition, offset: message.Offset}, struct{}{})
				received <- message
				return nil
			}, newObservedClient)
		}()

		joinCtx, cancelJoin := context.WithTimeout(ctx, 30*time.Second)
		defer cancelJoin()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			clientsMu.Lock()
			joined := len(clients) == 2
			generation := int32(-1)
			for _, client := range clients {
				member, current := client.client.GroupMetadata()
				joined = joined && member != "" && current >= 0 && (generation < 0 || generation == current)
				generation = current
			}
			clientsMu.Unlock()
			if joined {
				break
			}
			select {
			case <-ticker.C:
			case err := <-done:
				t.Fatalf("consumer stopped before group formation: %v", err)
			case <-joinCtx.Done():
				t.Fatal("consumer clients did not join the same group generation")
			}
		}
		produceRecords(t, ctx, brokers, topic, recordsCount, 2)
		waitForMessages(t, done, received, recordsCount)
		committed := waitForCommits(t, done, commitSizes, recordsCount, cfg.Kafka.MaxPollRecords)
		if committed != recordsCount {
			t.Fatalf("committed %d records, want %d", committed, recordsCount)
		}
		cancel()
		waitForRun(t, done)
		if got := peak.Load(); got < 2 || got > int32(cfg.Topics[0].ProcessingConcurrency) {
			t.Errorf("peak concurrent handlers = %d, want between 2 and %d", got, cfg.Topics[0].ProcessingConcurrency)
		}
		clientsMu.Lock()
		defer clientsMu.Unlock()
		if len(clients) != 2 {
			t.Fatalf("created %d consumer clients, want 2", len(clients))
		}
		seenPartitions := make(map[int32]bool)
		for _, client := range clients {
			if client.committed == 0 {
				t.Errorf("a consumer client committed no records")
			}
			for partition := range client.partitions {
				if seenPartitions[partition] {
					t.Errorf("partition %d was assigned to more than one client", partition)
				}
				seenPartitions[partition] = true
			}
		}
		if len(seenPartitions) != 2 || !seenPartitions[0] || !seenPartitions[1] {
			t.Errorf("consumer group partition assignments = %v, want partitions 0 and 1 split across clients", seenPartitions)
		}
	})

	t.Run("event persistence and DLQ-gated replay", func(t *testing.T) {
		testEventDelivery(t, ctx, container, brokers)
	})

	t.Run("in-flight DLQ cancellation", func(t *testing.T) {
		testDeadLetterInFlightCancellation(t, ctx, container, brokers)
	})

	t.Run("successful offsets survive restart", func(t *testing.T) {
		topic := "integration-restart"
		createTopic(t, ctx, container, topic, 1)
		produceRecords(t, ctx, brokers, topic, 5, 1)
		cfg := integrationConfig(brokers, topic, "integration-restart-group")
		for run := range 2 {
			if run == 1 {
				cfg.Kafka.AutoOffsetReset = "none"
				produceRecords(t, ctx, brokers, topic, 1, 1)
			}
			runCtx, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			received := make(chan consumer.Message, 6)
			commits := make(chan int, 6)
			factory, err := NewClientFactory(runCtx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- consumer.Run(runCtx, cfg, func(_ context.Context, message consumer.Message) error {
					received <- message
					return nil
				}, func(config consumer.Config, topic consumer.TopicConfig) (*observedClient, error) {
					client, err := factory(config, topic)
					if err != nil {
						return nil, err
					}
					return &observedClient{Client: client, onCommit: func(messages []consumer.Message) error {
						commits <- len(messages)
						return nil
					}}, nil
				})
			}()
			want := 5
			if run == 1 {
				want = 1
			}
			for range want {
				select {
				case message := <-received:
					if run == 1 && message.Offset != 5 {
						t.Fatalf("restart received offset %d, want only new offset 5", message.Offset)
					}
				case err := <-done:
					t.Fatalf("consumer stopped before receiving records: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			waitForCommits(t, done, commits, want, cfg.Kafka.MaxPollRecords)
			cancel()
			waitForRun(t, done)
		}
	})

	t.Run("cancellation leaves in-flight batch uncommitted", func(t *testing.T) {
		topic := "integration-cancel"
		createTopic(t, ctx, container, topic, 1)
		produceRecords(t, ctx, brokers, topic, 1, 1)
		cfg := integrationConfig(brokers, topic, "integration-cancel-group")
		runCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		handlerStarted := make(chan struct{})
		var commits atomic.Int32
		factory, err := NewClientFactory(runCtx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- consumer.Run(runCtx, cfg, func(ctx context.Context, _ consumer.Message) error {
				close(handlerStarted)
				<-ctx.Done()
				return ctx.Err()
			}, func(config consumer.Config, topicConfig consumer.TopicConfig) (*observedClient, error) {
				client, err := factory(config, topicConfig)
				if err != nil {
					return nil, err
				}
				return &observedClient{Client: client, onCommit: func([]consumer.Message) error {
					commits.Add(1)
					return nil
				}}, nil
			})
		}()
		select {
		case <-handlerStarted:
		case err := <-done:
			t.Fatalf("consumer stopped before handler started: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for handler")
		}
		cancel()
		waitForRun(t, done)
		if got := commits.Load(); got != 0 {
			t.Errorf("canceled batch committed %d times, want 0", got)
		}
	})

	t.Run("none offset reset requires prior committed offsets", func(t *testing.T) {
		const topic = "integration-no-reset"
		createTopic(t, ctx, container, topic, 1)
		produceRecords(t, ctx, brokers, topic, 3, 1)
		cfg := integrationConfig(brokers, topic, "integration-no-reset-group")
		cfg.Kafka.AutoOffsetReset = "none"
		runCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		factory, err := NewClientFactory(runCtx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		var processed atomic.Int32
		err = consumer.Run(runCtx, cfg, func(context.Context, consumer.Message) error {
			processed.Add(1)
			return nil
		}, factory)
		if err == nil || !strings.Contains(err.Error(), "partition has no prior committed offset") {
			t.Fatalf("Run() error = %v, want missing committed offset error", err)
		}
		if processed.Load() != 0 {
			t.Fatalf("processed %d records without committed offsets", processed.Load())
		}
	})

	t.Run("latest offset reset skips existing records", func(t *testing.T) {
		const topic = "integration-latest"
		createTopic(t, ctx, container, topic, 1)
		produceRecords(t, ctx, brokers, topic, 3, 1)
		cfg := integrationConfig(brokers, topic, "integration-latest-group")
		cfg.Kafka.AutoOffsetReset = "latest"
		factory, err := NewClientFactory(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		client, err := factory(cfg, cfg.Topics[0])
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseAllowingRebalance()
		initialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		messages, err := client.Poll(initialCtx, 1)
		cancel()
		client.AllowRebalance()
		member, generation := client.client.GroupMetadata()
		if len(messages) != 0 || !errors.Is(err, context.DeadlineExceeded) || member == "" || generation < 0 {
			t.Fatalf("initial latest poll = %v, %v; group member %q generation %d", messages, err, member, generation)
		}
		produceRecords(t, ctx, brokers, topic, 1, 1)
		messages, err = client.Poll(ctx, 1)
		if err != nil || len(messages) != 1 || messages[0].Offset != 3 {
			t.Fatalf("latest poll = %v, %v, want only new offset 3", messages, err)
		}
	})

	t.Run("default client produces LZ4 and decodes compressed records", func(t *testing.T) {
		const topic = "integration-lz4"
		createTopic(t, ctx, container, topic, 1)
		cfg := integrationConfig(brokers, topic, "integration-lz4-group")
		factory, err := NewClientFactory(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		client, err := factory(cfg, cfg.Topics[0])
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseAllowingRebalance()
		value := bytes.Repeat([]byte("LZ4 payload "), 1000)
		if err := client.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		fetches := client.client.PollRecords(ctx, 1)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs)
		}
		records := fetches.Records()
		if len(records) != 1 || !bytes.Equal(records[0].Value, value) || records[0].Attrs.CompressionType() != 3 {
			t.Fatalf("consumed records = %v, want original payload with LZ4 batch attributes", records)
		}
	})

	t.Run("shutdown with unavailable coordinator", func(t *testing.T) {
		const topic = "integration-close"
		createTopic(t, ctx, container, topic, 1)
		produceRecords(t, ctx, brokers, topic, 1, 1)
		cfg := integrationConfig(brokers, topic, "integration-close-group")
		cfg.Topics[0].CommitTimeout = 250 * time.Millisecond
		factory, err := NewClientFactory(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		client, err := factory(cfg, cfg.Topics[0])
		if err != nil {
			t.Fatal(err)
		}
		var closeOnce sync.Once
		closeClient := func() { closeOnce.Do(client.CloseAllowingRebalance) }
		t.Cleanup(closeClient)
		messages, err := client.Poll(ctx, 1)
		if err != nil || len(messages) != 1 {
			t.Fatalf("poll before coordinator outage = %v, %v", messages, err)
		}
		zero := time.Duration(0)
		if err := container.Stop(ctx, &zero); err != nil {
			t.Fatal(err)
		}
		closed := make(chan struct{})
		go func() {
			closeClient()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("client shutdown blocked on unavailable coordinator")
		}
	})
}

type observedClient struct {
	*Client
	onCommit   func([]consumer.Message) error
	partitions map[int32]bool
	committed  int
}

func (c *observedClient) Commit(ctx context.Context, messages ...consumer.Message) error {
	if err := c.Client.Commit(ctx, messages...); err != nil {
		return err
	}
	if c.onCommit != nil {
		return c.onCommit(messages)
	}
	return nil
}

type messageKey struct {
	partition int32
	offset    int64
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func startKafka(t *testing.T, ctx context.Context) (*kafkamodule.KafkaContainer, []string) {
	t.Helper()
	container, err := kafkamodule.Run(ctx, kafkaTestImage,
		kafkamodule.WithClusterID("go-backend-consumer-tests"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "1000",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":        "false",
		}),
	)
	if err != nil {
		t.Fatalf("start Kafka container %s: %v", kafkaTestImage, err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka test container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("get Kafka broker endpoint: %v", err)
	}
	return container, brokers
}

func createTopic(t *testing.T, ctx context.Context, container *kafkamodule.KafkaContainer, topic string, partitions int) {
	t.Helper()
	command := []string{
		"kafka-topics", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists",
		"--topic", topic, "--partitions", fmt.Sprint(partitions), "--replication-factor", "1",
	}
	exitCode, output, err := container.Exec(ctx, command)
	if err != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, err)
	}
	outputBytes, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read topic creation output: %v", readErr)
	}
	if exitCode != 0 {
		t.Fatalf("create Kafka topic %q exited %d: %s", topic, exitCode, outputBytes)
	}
}

func produceRecords(t *testing.T, ctx context.Context, brokers []string, topic string, count, partitions int) {
	t.Helper()
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("create Kafka producer: %v", err)
	}
	t.Cleanup(producer.Close)
	records := make([]*kgo.Record, count)
	for i := range count {
		records[i] = &kgo.Record{Topic: topic, Partition: int32(i % partitions), Key: []byte(fmt.Sprintf("key-%d", i)), Value: []byte(fmt.Sprintf("payload-%d", i))}
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce records to %q: %v", topic, err)
	}
}

func integrationConfig(brokers []string, topic, groupID string) consumer.Config {
	cfg := consumer.DefaultConfig()
	cfg.Kafka.Brokers = brokers
	cfg.Kafka.GroupID = groupID
	cfg.Kafka.TLS.Enabled = false
	cfg.Kafka.AllowInsecure = true
	cfg.Topics[0].Name = topic
	return cfg
}

func waitForMessages(t *testing.T, done <-chan error, messages <-chan consumer.Message, want int) {
	t.Helper()
	for range want {
		select {
		case <-messages:
		case err := <-done:
			t.Fatalf("consumer stopped before receiving all records: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for messages")
		}
	}
}

func waitForCommits(t *testing.T, done <-chan error, commits <-chan int, want, maxBatchSize int) int {
	t.Helper()
	committed := 0
	for committed < want {
		select {
		case count := <-commits:
			if count <= 0 || count > maxBatchSize {
				t.Fatalf("committed batch size = %d, want 1..%d", count, maxBatchSize)
			}
			committed += count
		case err := <-done:
			t.Fatalf("consumer stopped before committing all records: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for committed records")
		}
	}
	return committed
}

func waitForRun(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer Run() error = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
}
