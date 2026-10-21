package message

import (
	"sync"
	"testing"

	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestHandleLogsOnlyMessageMetadata(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	service := NewService(zap.New(core))
	record := consumer.Message{
		Topic: "events",
		Key:   []byte("private-key"),
		Value: []byte("private-payload"),
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
	const count = 100
	core, logs := observer.New(zapcore.InfoLevel)
	service := NewService(zap.New(core))
	var group sync.WaitGroup
	for i := range count {
		group.Go(func() {
			err := service.Handle(t.Context(), consumer.Message{
				Topic:     "events",
				Partition: 2,
				Offset:    int64(i),
			})
			if err != nil {
				t.Errorf("Handle() error = %v, want nil", err)
			}
		})
	}
	group.Wait()

	entries := logs.All()
	if len(entries) != count {
		t.Fatalf("got %d log entries, want %d", len(entries), count)
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
	}
}
