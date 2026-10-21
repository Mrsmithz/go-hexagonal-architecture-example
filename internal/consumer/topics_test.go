package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunSharesConcurrencyWithinTopicButNotAcrossTopics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		first := cfg.Topics[0]
		first.Name, first.Consumers, first.ProcessingConcurrency = "first", 2, 1
		second := first
		second.Name, second.ProcessingConcurrency = "second", 2
		cfg.Topics = []TopicConfig{first, second}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var active [2]atomic.Int32
		var commits atomic.Int32
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- Run(ctx, cfg, func(ctx context.Context, message Message) error {
				index := 0
				if message.Topic == "second" {
					index = 1
				}
				active[index].Add(1)
				defer active[index].Add(-1)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, func(_ Config, topic TopicConfig) (*testClient, error) {
				polled := false
				return &testClient{
					poll: func(ctx context.Context, _ int) ([]Message, error) {
						if polled {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						polled = true
						records := testMessages(2)
						for i := range records {
							records[i].Topic = topic.Name
						}
						return records, nil
					},
					commit: func(context.Context, ...Message) error { commits.Add(1); return nil },
				}, nil
			})
		}()
		synctest.Wait()
		if gotFirst, gotSecond := active[0].Load(), active[1].Load(); gotFirst != 1 || gotSecond != 2 {
			t.Fatalf("active handlers = first:%d second:%d, want independent topic caps 1 and 2", gotFirst, gotSecond)
		}
		close(release)
		synctest.Wait()
		if got := commits.Load(); got != 4 {
			t.Fatalf("committed batches = %d, want all four clients", got)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunUsesEachTopicsBatchAndCommitDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		first := cfg.Topics[0]
		first.Name, first.BatchTimeout, first.CommitTimeout = "first", 200*time.Millisecond, 30*time.Millisecond
		second := first
		second.Name, second.BatchTimeout, second.CommitTimeout = "second", 400*time.Millisecond, 70*time.Millisecond
		cfg.Topics = []TopicConfig{first, second}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		type observed struct {
			topic  string
			budget time.Duration
		}
		batches := make(chan observed, 2)
		commits := make(chan observed, 2)
		done := make(chan error, 1)
		go func() {
			done <- Run(ctx, cfg, func(ctx context.Context, message Message) error {
				deadline, _ := ctx.Deadline()
				batches <- observed{message.Topic, time.Until(deadline)}
				return nil
			}, func(_ Config, topic TopicConfig) (*testClient, error) {
				polled := false
				return &testClient{
					poll: func(ctx context.Context, _ int) ([]Message, error) {
						if polled {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						polled = true
						return []Message{{Topic: topic.Name}}, nil
					},
					commit: func(ctx context.Context, _ ...Message) error {
						deadline, _ := ctx.Deadline()
						commits <- observed{topic.Name, time.Until(deadline)}
						return nil
					},
				}, nil
			})
		}()
		synctest.Wait()
		for range 2 {
			batch := <-batches
			want := first.BatchTimeout
			if batch.topic == second.Name {
				want = second.BatchTimeout
			}
			if batch.budget != want {
				t.Fatalf("topic %s batch budget = %s, want %s", batch.topic, batch.budget, want)
			}
			commit := <-commits
			want = first.CommitTimeout
			if commit.topic == second.Name {
				want = second.CommitTimeout
			}
			if commit.budget != want {
				t.Fatalf("topic %s commit budget = %s, want %s", commit.topic, commit.budget, want)
			}
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}
