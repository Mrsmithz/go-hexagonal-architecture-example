package message

import (
	"context"
	"errors"
	"sync"
	"testing"

	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type storeFunc func(context.Context, Source, Event) error

func (f storeFunc) SaveEvent(ctx context.Context, source Source, event Event) error {
	return f(ctx, source, event)
}

func TestHandlePersistsValidatedEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
		want  Event
	}{
		{
			name:  "sample",
			value: `{"user_id":101,"event":"login","timestamp":1729526400}`,
			want:  Event{UserID: 101, Event: "login", Timestamp: 1729526400},
		},
		{
			name:  "zero timestamp and unicode whitespace",
			value: "{\"user_id\":101,\"event\":\"\\t\\n\u2003login\u00a0\",\"timestamp\":0} \r\n\t",
			want:  Event{UserID: 101, Event: "login", Timestamp: 0},
		},
		{
			name:  "signed timestamp and maximum user id",
			value: `{"user_id":9223372036854775807,"event":"custom event","timestamp":-1}`,
			want:  Event{UserID: 9223372036854775807, Event: "custom event", Timestamp: -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			wantSource := Source{Topic: "events", Partition: 3, Offset: 42}
			calls := 0
			store := storeFunc(func(gotCtx context.Context, source Source, event Event) error {
				calls++
				if gotCtx != ctx {
					t.Error("SaveEvent did not receive the handler context")
				}
				if source != wantSource || event != tt.want {
					t.Errorf("SaveEvent = %+v, %+v; want %+v, %+v", source, event, wantSource, tt.want)
				}
				return nil
			})
			service := NewService(store, zap.NewNop())
			err := service.Handle(ctx, consumer.Message{
				Topic:     wantSource.Topic,
				Partition: wantSource.Partition,
				Offset:    wantSource.Offset,
				Value:     []byte(tt.value),
			})
			if err != nil || calls != 1 {
				t.Fatalf("Handle = %v, SaveEvent calls = %d; want nil, 1", err, calls)
			}
		})
	}
}

func TestHandleRejectsInvalidPayloadWithoutSavingOrLogging(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "malformed", value: `{"user_id":`},
		{name: "null object", value: `null`},
		{name: "array", value: `[]`},
		{name: "string", value: `"event"`},
		{name: "number", value: `101`},
		{name: "boolean", value: `true`},
		{name: "empty object", value: `{}`},
		{name: "missing user id", value: `{"event":"login","timestamp":0}`},
		{name: "null user id", value: `{"user_id":null,"event":"login","timestamp":0}`},
		{name: "zero user id", value: `{"user_id":0,"event":"login","timestamp":0}`},
		{name: "negative user id", value: `{"user_id":-1,"event":"login","timestamp":0}`},
		{name: "string user id", value: `{"user_id":"101","event":"login","timestamp":0}`},
		{name: "fractional user id", value: `{"user_id":101.5,"event":"login","timestamp":0}`},
		{name: "overflow user id", value: `{"user_id":9223372036854775808,"event":"login","timestamp":0}`},
		{name: "boolean user id", value: `{"user_id":true,"event":"login","timestamp":0}`},
		{name: "object user id", value: `{"user_id":{},"event":"login","timestamp":0}`},
		{name: "missing event", value: `{"user_id":101,"timestamp":0}`},
		{name: "null event", value: `{"user_id":101,"event":null,"timestamp":0}`},
		{name: "empty event", value: `{"user_id":101,"event":"","timestamp":0}`},
		{name: "whitespace event", value: `{"user_id":101,"event":" \t\n\u2003\u00a0","timestamp":0}`},
		{name: "number event", value: `{"user_id":101,"event":101,"timestamp":0}`},
		{name: "boolean event", value: `{"user_id":101,"event":true,"timestamp":0}`},
		{name: "object event", value: `{"user_id":101,"event":{},"timestamp":0}`},
		{name: "missing timestamp", value: `{"user_id":101,"event":"login"}`},
		{name: "null timestamp", value: `{"user_id":101,"event":"login","timestamp":null}`},
		{name: "string timestamp", value: `{"user_id":101,"event":"login","timestamp":"0"}`},
		{name: "fractional timestamp", value: `{"user_id":101,"event":"login","timestamp":0.5}`},
		{name: "overflow timestamp", value: `{"user_id":101,"event":"login","timestamp":9223372036854775808}`},
		{name: "boolean timestamp", value: `{"user_id":101,"event":"login","timestamp":true}`},
		{name: "object timestamp", value: `{"user_id":101,"event":"login","timestamp":{}}`},
		{name: "unknown field", value: `{"user_id":101,"event":"login","timestamp":0,"extra":true}`},
		{name: "multiple objects", value: `{"user_id":101,"event":"login","timestamp":0}{}`},
		{name: "trailing null", value: `{"user_id":101,"event":"login","timestamp":0} null`},
		{name: "trailing number", value: `{"user_id":101,"event":"login","timestamp":0} 1`},
		{name: "trailing garbage", value: `{"user_id":101,"event":"login","timestamp":0} invalid`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			core, logs := observer.New(zapcore.InfoLevel)
			store := storeFunc(func(context.Context, Source, Event) error {
				t.Error("SaveEvent called for invalid payload")
				return nil
			})
			service := NewService(store, zap.New(core))
			if err := service.Handle(t.Context(), consumer.Message{Value: []byte(tt.value)}); err == nil {
				t.Fatal("Handle accepted invalid payload")
			}
			if logs.Len() != 0 {
				t.Errorf("logged %d entries for invalid payload", logs.Len())
			}
		})
	}
}

func TestHandleCancellationAndStoreError(t *testing.T) {
	t.Parallel()
	t.Run("canceled before handling", func(t *testing.T) {
		t.Parallel()
		core, logs := observer.New(zapcore.InfoLevel)
		store := storeFunc(func(context.Context, Source, Event) error {
			t.Error("SaveEvent called with canceled context")
			return nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := NewService(store, zap.New(core)).Handle(ctx, consumer.Message{})
		if !errors.Is(err, context.Canceled) || logs.Len() != 0 {
			t.Fatalf("Handle = %v, logs = %d; want cancellation without logging", err, logs.Len())
		}
	})
	for _, storeErr := range []error{errors.New("database unavailable"), context.Canceled, context.DeadlineExceeded} {
		t.Run(storeErr.Error(), func(t *testing.T) {
			t.Parallel()
			core, logs := observer.New(zapcore.InfoLevel)
			store := storeFunc(func(context.Context, Source, Event) error { return storeErr })
			err := NewService(store, zap.New(core)).Handle(t.Context(), consumer.Message{
				Value: []byte(`{"user_id":101,"event":"login","timestamp":1729526400}`),
			})
			if !errors.Is(err, storeErr) || logs.Len() != 0 {
				t.Fatalf("Handle = %v, logs = %d; want wrapped store error without logging", err, logs.Len())
			}
		})
	}
}

func TestHandleLogsOnlyMessageMetadataAfterPersistence(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zapcore.InfoLevel)
	store := storeFunc(func(context.Context, Source, Event) error {
		if logs.Len() != 0 {
			t.Error("success logged before persistence")
		}
		return nil
	})
	service := NewService(store, zap.New(core))
	record := consumer.Message{
		Topic: "events",
		Key:   []byte("private-key"),
		Value: []byte(`{"user_id":101,"event":"private-event","timestamp":1729526400}`),
		Headers: []consumer.Header{{
			Key:   "private-header",
			Value: []byte("private-header-value"),
		}},
		Partition: 3,
		Offset:    42,
	}

	if err := service.Handle(t.Context(), record); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("got %d log entries, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Level != zapcore.InfoLevel {
		t.Errorf("log level = %v, want %v", entry.Level, zapcore.InfoLevel)
	}
	if entry.Message != "kafka message processed" {
		t.Errorf("log message = %q, want %q", entry.Message, "kafka message processed")
	}
	fields := entry.ContextMap()
	if len(fields) != 3 {
		t.Fatalf("got %d fields, want 3: %v", len(fields), fields)
	}
	if fields["topic"] != "events" {
		t.Errorf("topic = %v, want events", fields["topic"])
	}
	if fields["partition"] != int32(3) {
		t.Errorf("partition = %v, want int32(3)", fields["partition"])
	}
	if fields["offset"] != int64(42) {
		t.Errorf("offset = %v, want int64(42)", fields["offset"])
	}
}

func TestHandleConcurrentCallsLogOnceEach(t *testing.T) {
	t.Parallel()
	const count = 100
	core, logs := observer.New(zapcore.InfoLevel)
	var mu sync.Mutex
	persisted := make(map[Source]Event)
	store := storeFunc(func(_ context.Context, source Source, event Event) error {
		mu.Lock()
		defer mu.Unlock()
		persisted[source] = event
		return nil
	})
	service := NewService(store, zap.New(core))
	var group sync.WaitGroup
	for i := range count {
		group.Go(func() {
			err := service.Handle(t.Context(), consumer.Message{
				Topic:     "events",
				Partition: 2,
				Offset:    int64(i),
				Value:     []byte(`{"user_id":101,"event":"login","timestamp":1729526400}`),
			})
			if err != nil {
				t.Errorf("Handle() error = %v, want nil", err)
			}
		})
	}
	group.Wait()

	entries := logs.All()
	if len(entries) != count || len(persisted) != count {
		t.Fatalf("got %d logs and %d persisted events, want %d each", len(entries), len(persisted), count)
	}
	seen := make(map[int64]bool, count)
	for _, entry := range entries {
		fields := entry.ContextMap()
		if entry.Message != "kafka message processed" || entry.Level != zapcore.InfoLevel {
			t.Errorf("unexpected log entry: %s", entry.Message)
		}
		if fields["topic"] != "events" || fields["partition"] != int32(2) {
			t.Errorf("unexpected metadata: %v", fields)
		}
		offset, ok := fields["offset"].(int64)
		if !ok {
			t.Errorf("offset has type %T, want int64", fields["offset"])
			continue
		}
		if seen[offset] {
			t.Errorf("duplicate offset %d", offset)
		}
		seen[offset] = true
	}
	for i := range count {
		if !seen[int64(i)] {
			t.Errorf("missing offset %d", i)
		}
		if persisted[Source{Topic: "events", Partition: 2, Offset: int64(i)}] !=
			(Event{UserID: 101, Event: "login", Timestamp: 1729526400}) {
			t.Errorf("missing or incorrect persisted event for offset %d", i)
		}
	}
}
