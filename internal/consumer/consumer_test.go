package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunRejectsInvalidDependenciesAndConfig(t *testing.T) {
	validHandler := func(context.Context, Message) error { return nil }
	validFactory := func(Config, TopicConfig) (*testClient, error) { return &testClient{}, nil }
	for _, test := range []struct {
		name    string
		ctx     context.Context
		handler Handler
		factory func(Config, TopicConfig) (*testClient, error)
		config  Config
	}{
		{name: "nil context", ctx: nil, handler: validHandler, factory: validFactory, config: testConfig()},
		{name: "nil handler", ctx: context.Background(), handler: nil, factory: validFactory, config: testConfig()},
		{name: "nil client factory", ctx: context.Background(), handler: validHandler, factory: nil, config: testConfig()},
		{name: "invalid config", ctx: context.Background(), handler: validHandler, factory: validFactory, config: Config{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := Run[*testClient](test.ctx, test.config, test.handler, test.factory); err == nil {
				t.Fatal("Run() error = nil, want validation error")
			}
		})
	}
}

func TestRunCommitsWholeBatchAfterUnorderedHandlersSucceed(t *testing.T) {
	cfg := testConfig()
	cfg.Kafka.MaxPollRecords = 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batch := testMessages(2)
	var pollCount atomic.Int32
	var finished atomic.Int32
	offsetOneFinished := make(chan struct{})
	releaseOffsetZero := make(chan struct{})
	commitObserved := make(chan []Message, 1)
	consumerClient := &testClient{
		poll: func(ctx context.Context, maxRecords int) ([]Message, error) {
			if maxRecords != cfg.Kafka.MaxPollRecords {
				return nil, fmt.Errorf("poll max records = %d, want %d", maxRecords, cfg.Kafka.MaxPollRecords)
			}
			if pollCount.Add(1) == 1 {
				return batch, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		commit: func(_ context.Context, records ...Message) error {
			if finished.Load() != int32(len(batch)) {
				return fmt.Errorf("commit ran after only %d handlers finished", finished.Load())
			}
			commitObserved <- append([]Message(nil), records...)
			return nil
		},
	}
	finishedOffsets := make(chan int64, len(batch))
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, func(ctx context.Context, message Message) error {
			if message.Offset == 0 {
				select {
				case <-offsetOneFinished:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-releaseOffsetZero:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if message.Offset == 1 {
				close(offsetOneFinished)
			}
			finished.Add(1)
			finishedOffsets <- message.Offset
			return nil
		}, func(Config, TopicConfig) (*testClient, error) { return consumerClient, nil })
	}()

	select {
	case offset := <-finishedOffsets:
		if offset != 1 {
			t.Fatalf("first completed offset = %d, want 1 to prove unordered processing", offset)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not complete")
	}
	select {
	case <-commitObserved:
		t.Fatal("batch committed before all handlers finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseOffsetZero)
	if offset := <-finishedOffsets; offset != 0 {
		t.Fatalf("second completed offset = %d, want 0", offset)
	}
	select {
	case records := <-commitObserved:
		if len(records) != len(batch) {
			t.Fatalf("committed %d records, want %d", len(records), len(batch))
		}
		for i, record := range batch {
			if records[i].Offset != record.Offset {
				t.Fatalf("committed offset[%d] = %d, want %d", i, records[i].Offset, record.Offset)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("successful batch was not committed")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil on cancellation", err)
	}
	if got, want := consumerClient.allowCount.Load(), pollCount.Load(); got != want {
		t.Errorf("AllowRebalance() called %d times after %d polls", got, want)
	}
	if got := consumerClient.closeCount.Load(); got != 1 {
		t.Errorf("CloseAllowingRebalance() called %d times, want 1", got)
	}
}

func TestRunUsesOneConcurrencyLimitAcrossClients(t *testing.T) {
	cfg := testConfig()
	cfg.Topics[0].Consumers = 2
	cfg.Topics[0].ProcessingConcurrency = 2
	gate := make(chan struct{})
	started := make(chan struct{}, 4)
	var active atomic.Int32
	var peak atomic.Int32
	var factoryMu sync.Mutex
	var clients []*testClient
	factory := func(Config, TopicConfig) (*testClient, error) {
		client := &testClient{}
		var polled atomic.Bool
		client.poll = func(ctx context.Context, _ int) ([]Message, error) {
			if polled.CompareAndSwap(false, true) {
				return testMessages(2), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		client.commit = func(context.Context, ...Message) error { return nil }
		factoryMu.Lock()
		clients = append(clients, client)
		factoryMu.Unlock()
		return client, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, func(ctx context.Context, _ Message) error {
			current := active.Add(1)
			for {
				previous := peak.Load()
				if current <= previous || peak.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
				active.Add(-1)
				return ctx.Err()
			}
			active.Add(-1)
			return nil
		}, factory)
	}()

	waitSignals(t, started, 2)
	select {
	case <-started:
		t.Fatal("more handlers started than the shared concurrency limit")
	case <-time.After(30 * time.Millisecond):
	}
	close(gate)
	waitSignals(t, started, 2)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil on cancellation", err)
	}
	if got := peak.Load(); got != 2 {
		t.Errorf("peak concurrent handlers = %d, want 2", got)
	}
	if len(clients) != 2 {
		t.Fatalf("created %d clients, want 2", len(clients))
	}
	for _, client := range clients {
		if got := client.closeCount.Load(); got != 1 {
			t.Errorf("CloseAllowingRebalance() called %d times, want 1", got)
		}
	}
}

func TestRunCommitsBatchAfterHandlerFailuresAreDeadLettered(t *testing.T) {
	cfg := testConfig()
	cfg.Topics[0].ProcessingConcurrency = 1
	batch := testMessages(4)
	firstFailure := errors.New("first handler failed")
	secondFailure := errors.New("second handler failed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var finished, deadLetters, commits atomic.Int32
	consumerClient := &testClient{
		poll: func(context.Context, int) ([]Message, error) { return batch, nil },
		commit: func(_ context.Context, messages ...Message) error {
			if finished.Load() != int32(len(batch)) || deadLetters.Load() != 2 || len(messages) != len(batch) {
				return fmt.Errorf("commit before all handler and dead-letter attempts completed")
			}
			commits.Add(1)
			cancel()
			return nil
		},
		onDLQ: func(_ context.Context, record Message, handlerErr error) error {
			deadLetters.Add(1)
			if record.Offset == 0 && !errors.Is(handlerErr, firstFailure) || record.Offset == 2 && !errors.Is(handlerErr, secondFailure) {
				return fmt.Errorf("dead-letter received wrong failure for offset %d", record.Offset)
			}
			return nil
		},
	}
	err := Run(ctx, cfg, func(ctx context.Context, message Message) error {
		if ctx.Err() != nil {
			return fmt.Errorf("ordinary handler failure canceled a sibling: %w", ctx.Err())
		}
		finished.Add(1)
		switch message.Offset {
		case 0:
			return firstFailure
		case 2:
			return secondFailure
		default:
			return nil
		}
	}, func(Config, TopicConfig) (*testClient, error) { return consumerClient, nil })
	if err != nil {
		t.Fatalf("Run() error = %v, want nil after cancellation", err)
	}
	if commits.Load() != 1 || finished.Load() != int32(len(batch)) {
		t.Fatalf("commits=%d handlers=%d, want one commit after all handlers", commits.Load(), finished.Load())
	}
}

func TestRunDeadLetterPublicationHoldsTopicConcurrencySlot(t *testing.T) {
	cfg := testConfig()
	cfg.Topics[0].ProcessingConcurrency = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstPublishStarted := make(chan struct{})
	releasePublish := make(chan struct{})
	var handled, published atomic.Int32
	client := &testClient{
		poll: func(ctx context.Context, _ int) ([]Message, error) {
			if handled.Load() == 0 {
				return testMessages(2), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		commit: func(context.Context, ...Message) error { cancel(); return nil },
		onDLQ: func(ctx context.Context, _ Message, _ error) error {
			if published.Add(1) == 1 {
				close(firstPublishStarted)
				select {
				case <-releasePublish:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, func(context.Context, Message) error {
			handled.Add(1)
			return errors.New("handler failed")
		}, func(Config, TopicConfig) (*testClient, error) { return client, nil })
	}()
	select {
	case <-firstPublishStarted:
	case <-time.After(time.Second):
		t.Fatal("dead-letter publication did not start")
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("handlers started while dead-letter held concurrency slot: %d, want 1", got)
	}
	close(releasePublish)
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil after resolved batch cancellation", err)
	}
	if handled.Load() != 2 || published.Load() != 2 {
		t.Fatalf("handlers=%d dead-letter publications=%d, want 2 each", handled.Load(), published.Load())
	}
}

func TestRunPollFailureCancelsSiblingClients(t *testing.T) {
	cfg := testConfig()
	cfg.Topics[0].Consumers = 2
	pollFailure := errors.New("poll failed")
	var pollCount atomic.Int32
	pollBarrier := make(chan struct{})
	startPoll := func() {
		if pollCount.Add(1) == 2 {
			close(pollBarrier)
		}
		<-pollBarrier
	}
	failedPoll := &testClient{poll: func(context.Context, int) ([]Message, error) {
		startPoll()
		return nil, pollFailure
	}}
	siblingCanceled := make(chan struct{})
	waitingPoll := &testClient{poll: func(ctx context.Context, _ int) ([]Message, error) {
		startPoll()
		<-ctx.Done()
		close(siblingCanceled)
		return nil, ctx.Err()
	}}
	var factoryCalls atomic.Int32
	err := Run(context.Background(), cfg, func(context.Context, Message) error { return nil }, func(Config, TopicConfig) (*testClient, error) {
		if factoryCalls.Add(1) == 1 {
			return failedPoll, nil
		}
		return waitingPoll, nil
	})
	if !errors.Is(err, pollFailure) {
		t.Fatalf("Run() error = %v, want poll failure", err)
	}
	select {
	case <-siblingCanceled:
	default:
		t.Fatal("poll failure did not cancel sibling poll")
	}
	for _, client := range []*testClient{failedPoll, waitingPoll} {
		if got := client.closeCount.Load(); got != 1 {
			t.Errorf("CloseAllowingRebalance() called %d times, want 1", got)
		}
		if got := client.allowCount.Load(); got != 1 {
			t.Errorf("AllowRebalance() called %d times, want 1", got)
		}
	}
}

func TestRunCommitFailureReturnsError(t *testing.T) {
	cfg := testConfig()
	commitFailure := errors.New("commit failed")
	var handled atomic.Int32
	consumerClient := &testClient{
		poll:   func(context.Context, int) ([]Message, error) { return testMessages(1), nil },
		commit: func(context.Context, ...Message) error { return commitFailure },
	}
	err := Run(context.Background(), cfg, func(context.Context, Message) error {
		handled.Add(1)
		return nil
	}, func(Config, TopicConfig) (*testClient, error) { return consumerClient, nil })
	if !errors.Is(err, commitFailure) {
		t.Fatalf("Run() error = %v, want commit failure", err)
	}
	if got := handled.Load(); got != 1 {
		t.Errorf("handler called %d times, want 1", got)
	}
	if got := consumerClient.allowCount.Load(); got != 1 {
		t.Errorf("AllowRebalance() called %d times, want 1", got)
	}
}

func TestRunDoesNotCommitWhenDeadLetterPublicationFails(t *testing.T) {
	handlerFailure := errors.New("handler failed")
	publishFailure := errors.New("dead-letter publish failed")
	var polls, handlers, commits atomic.Int32
	client := &testClient{
		poll: func(context.Context, int) ([]Message, error) {
			polls.Add(1)
			return testMessages(3), nil
		},
		commit: func(context.Context, ...Message) error { commits.Add(1); return nil },
		onDLQ: func(_ context.Context, message Message, _ error) error {
			if message.Offset == 0 {
				return publishFailure
			}
			return nil
		},
	}
	err := Run(t.Context(), testConfig(), func(context.Context, Message) error {
		handlers.Add(1)
		return handlerFailure
	}, func(Config, TopicConfig) (*testClient, error) { return client, nil })
	if !errors.Is(err, handlerFailure) || !errors.Is(err, publishFailure) {
		t.Fatalf("Run() error = %v, want handler and publish failures", err)
	}
	if handlers.Load() != 3 || polls.Load() != 1 || commits.Load() != 0 {
		t.Fatalf("handlers=%d polls=%d commits=%d, want 3, 1, 0", handlers.Load(), polls.Load(), commits.Load())
	}
}

func TestRunDeadlinesLeaveUnfinishedBatchesUncommitted(t *testing.T) {
	for _, deadline := range []string{"batch", "poll interval"} {
		t.Run(deadline, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := testConfig()
				cfg.Topics[0].ProcessingConcurrency = 1
				if deadline == "batch" {
					cfg.Topics[0].BatchTimeout = 20 * time.Millisecond
				} else {
					cfg.Kafka.MaxPollIntervalMS = 100
				}
				var started, commits atomic.Int32
				client := &testClient{
					poll:   func(context.Context, int) ([]Message, error) { return testMessages(3), nil },
					commit: func(context.Context, ...Message) error { commits.Add(1); return nil },
				}
				err := Run(t.Context(), cfg, func(ctx context.Context, _ Message) error {
					started.Add(1)
					<-ctx.Done()
					return ctx.Err()
				}, func(Config, TopicConfig) (*testClient, error) { return client, nil })
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Run() error = %v, want deadline exceeded", err)
				}
				if started.Load() != 1 || commits.Load() != 0 {
					t.Fatalf("started %d handlers, committed %d incomplete batches", started.Load(), commits.Load())
				}
			})
		})
	}
}

func TestRunPollIntervalAlsoBoundsCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.Kafka.MaxPollIntervalMS = 100
		cfg.Topics[0].CommitTimeout = time.Second
		start := time.Now()
		client := &testClient{
			poll:   func(context.Context, int) ([]Message, error) { return testMessages(1), nil },
			commit: func(ctx context.Context, _ ...Message) error { <-ctx.Done(); return ctx.Err() },
		}
		err := Run(t.Context(), cfg, func(context.Context, Message) error {
			time.Sleep(40 * time.Millisecond)
			return nil
		}, func(Config, TopicConfig) (*testClient, error) { return client, nil })
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 100*time.Millisecond {
			t.Fatalf("Run() = %v after %s, want total processing-plus-commit deadline of 100ms", err, time.Since(start))
		}
	})
}

func TestRunCancellationLeavesBatchUncommitted(t *testing.T) {
	cfg := testConfig()
	cfg.Topics[0].ProcessingConcurrency = 1
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var commits atomic.Int32
	consumerClient := &testClient{
		poll: func(context.Context, int) ([]Message, error) { return testMessages(3), nil },
		commit: func(context.Context, ...Message) error {
			commits.Add(1)
			return nil
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, func(ctx context.Context, _ Message) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}, func(Config, TopicConfig) (*testClient, error) { return consumerClient, nil })
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v, want nil on cancellation", err)
	}
	if got := commits.Load(); got != 0 {
		t.Errorf("committed %d canceled batches, want none", got)
	}
	if got := consumerClient.allowCount.Load(); got != 1 {
		t.Errorf("AllowRebalance() called %d times, want 1", got)
	}
}

func TestRunClosesClientsAfterFactoryFailure(t *testing.T) {
	cfg := testConfig()
	cfg.Topics[0].Consumers = 2
	creationFailure := errors.New("cannot create client")
	first := &testClient{}
	var calls atomic.Int32
	err := Run(context.Background(), cfg, func(context.Context, Message) error { return nil }, func(Config, TopicConfig) (*testClient, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}
		return nil, creationFailure
	})
	if !errors.Is(err, creationFailure) {
		t.Fatalf("Run() error = %v, want factory failure", err)
	}
	if got := first.closeCount.Load(); got != 1 {
		t.Errorf("CloseAllowingRebalance() called %d times, want 1", got)
	}
}

func testConfig() Config {
	return DefaultConfig()
}

func testMessages(count int) []Message {
	messages := make([]Message, count)
	for i := range count {
		messages[i] = Message{Topic: "greetings", Partition: 0, Offset: int64(i)}
	}
	return messages
}

func waitSignals(t *testing.T, signals <-chan struct{}, count int) {
	t.Helper()
	for range count {
		select {
		case <-signals:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for handlers")
		}
	}
}

type testClient struct {
	poll       func(context.Context, int) ([]Message, error)
	commit     func(context.Context, ...Message) error
	onDLQ      func(context.Context, Message, error) error
	allowCount atomic.Int32
	closeCount atomic.Int32
}

func (c *testClient) Poll(ctx context.Context, maxRecords int) ([]Message, error) {
	if c.poll == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.poll(ctx, maxRecords)
}

func (c *testClient) Commit(ctx context.Context, records ...Message) error {
	if c.commit == nil {
		return nil
	}
	return c.commit(ctx, records...)
}

func (c *testClient) PublishDeadLetter(ctx context.Context, message Message, handlerErr error) error {
	if c.onDLQ == nil {
		return nil
	}
	return c.onDLQ(ctx, message, handlerErr)
}

func (c *testClient) AllowRebalance() {
	c.allowCount.Add(1)
}

func (c *testClient) CloseAllowingRebalance() {
	c.closeCount.Add(1)
}
