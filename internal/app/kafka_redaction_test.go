package app

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.com/go-backend-example/internal/consumer"
	"example.com/go-backend-example/internal/message"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestFatalDLQFailureDoesNotLogPayloadDerivedErrors(t *testing.T) {
	t.Parallel()
	privateField := "private-" + strings.Repeat("secret-field-", 500)
	privateNumber := strings.Repeat("987654321", 500)
	for name, value := range map[string]string{
		"unknown field":               `{"user_id":101,"event":"login","timestamp":0,"` + privateField + `":true}`,
		"overflowing number":          `{"user_id":` + privateNumber + `,"event":"login","timestamp":0}`,
		"trailing overflowing number": `{"user_id":101,"event":"login","timestamp":0} ` + privateNumber,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			core, logs := observer.New(zapcore.ErrorLevel)
			cfg := consumer.DefaultConfig()
			cfg.Kafka.Enabled = true
			cfg.Topics[0].Name = "events"
			cfg.Topics[0].Consumers = 1
			client := &unavailableDLQClient{record: consumer.Message{Topic: "events", Partition: 2, Offset: 7, Value: []byte(value)}}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := Run(ctx, Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: filepath.Join(t.TempDir(), "events.db"), Consumer: cfg}, zap.New(core),
				fx.Decorate(func(_ kafkaRun, service *message.Service) kafkaRun {
					return func(ctx context.Context) error {
						return consumer.Run(ctx, cfg, service.Handle, func(consumer.Config, consumer.TopicConfig) (*unavailableDLQClient, error) { return client, nil })
					}
				}))
			if err == nil || client.commits.Load() != 0 {
				t.Fatalf("Run error=%v, commits=%d; want fatal unresolved error without commits", err, client.commits.Load())
			}
			assertSafe := func(text string) {
				t.Helper()
				if strings.Contains(text, "secret-field-") || strings.Contains(text, "987654321987654321") || len(text) > 1024 {
					t.Fatalf("payload-derived or unbounded error escaped (%d bytes)", len(text))
				}
				if !strings.Contains(text, "events partition 2 offset 7") || !strings.Contains(text, "decode") || !strings.Contains(text, "DLQ unavailable") {
					t.Fatalf("safe diagnostics lost source or classifications: %q", text)
				}
			}
			assertSafe(err.Error())
			entries := logs.FilterMessage("kafka consumer failed").All()
			if len(entries) != 1 {
				t.Fatalf("fatal Kafka logs=%d, want 1", len(entries))
			}
			text, ok := entries[0].ContextMap()["error"].(string)
			if !ok {
				t.Fatal("fatal Kafka log has no error text")
			}
			assertSafe(text)
		})
	}
}

type unavailableDLQClient struct {
	record  consumer.Message
	commits atomic.Int32
}

func (c *unavailableDLQClient) Poll(context.Context, int) ([]consumer.Message, error) {
	return []consumer.Message{c.record}, nil
}
func (c *unavailableDLQClient) Commit(context.Context, ...consumer.Message) error {
	c.commits.Add(1)
	return nil
}
func (*unavailableDLQClient) PublishDeadLetter(context.Context, consumer.Message, error) error {
	return errors.New("DLQ unavailable")
}
func (*unavailableDLQClient) AllowRebalance()         {}
func (*unavailableDLQClient) CloseAllowingRebalance() {}
