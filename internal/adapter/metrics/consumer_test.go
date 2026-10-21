package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"example.com/go-backend-example/internal/consumer"
)

func TestConsumerWrap(t *testing.T) {
	failure := errors.New("handler failed")
	configured := []consumer.TopicConfig{{Name: "alpha"}, {Name: "beta"}}
	m := New(configured)
	ctx := context.WithValue(context.Background(), struct{}{}, "preserved")
	message := consumer.Message{Topic: "alpha", Value: []byte("payload")}
	gotMessage := false
	gotContext := false
	err := m.Wrap(func(gotCtx context.Context, got consumer.Message) error {
		gotContext = gotCtx == ctx
		gotMessage = got.Topic == message.Topic && string(got.Value) == "payload"
		return failure
	})(ctx, message)
	if err != failure || !gotContext || !gotMessage {
		t.Fatalf("Wrap() err/context/message = %v/%v/%v", err, gotContext, gotMessage)
	}
	m.Wrap(func(context.Context, consumer.Message) error { return nil })(ctx, consumer.Message{Topic: "beta"})

	exposition := scrape(t, m)
	for _, want := range []string{
		`kafka_consumer_messages_processed_total{status="failed",topic="alpha"} 1`,
		`kafka_consumer_messages_processed_total{status="success",topic="alpha"} 0`,
		`kafka_consumer_messages_processed_total{status="success",topic="beta"} 1`,
		`kafka_consumer_messages_processed_total{status="failed",topic="beta"} 0`,
		`kafka_consumer_message_processing_duration_seconds_count{status="failed",topic="alpha"} 1`,
		`kafka_consumer_message_processing_duration_seconds_count{status="success",topic="alpha"} 0`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("scrape missing %q", want)
		}
	}
}

func TestConsumerWrapDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New([]consumer.TopicConfig{{Name: "topic"}})
		m.Wrap(func(context.Context, consumer.Message) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})(context.Background(), consumer.Message{Topic: "topic"})
		output := scrape(t, m)
		if !strings.Contains(output, `kafka_consumer_message_processing_duration_seconds_count{status="success",topic="topic"} 1`) ||
			!strings.Contains(output, `kafka_consumer_message_processing_duration_seconds_sum{status="success",topic="topic"} 0.1`) {
			t.Fatalf("duration exposition does not show virtual 100ms observation:\n%s", output)
		}
	})
}

func TestConsumerWrapConcurrent(t *testing.T) {
	const calls = 500
	m := New([]consumer.TopicConfig{{Name: "topic"}})
	handler := m.Wrap(func(context.Context, consumer.Message) error { return nil })
	var group sync.WaitGroup
	for range calls {
		group.Go(func() { handler(context.Background(), consumer.Message{Topic: "topic"}) })
	}
	group.Wait()
	output := scrape(t, m)
	for _, want := range []string{
		fmt.Sprintf(`kafka_consumer_messages_processed_total{status="success",topic="topic"} %d`, calls),
		fmt.Sprintf(`kafka_consumer_message_processing_duration_seconds_count{status="success",topic="topic"} %d`, calls),
	} {
		if !strings.Contains(output, want) {
			t.Errorf("scrape missing %q", want)
		}
	}
}

func TestConsumerIsolationAndHandler(t *testing.T) {
	first := New([]consumer.TopicConfig{{Name: "first"}})
	second := New([]consumer.TopicConfig{{Name: "second"}})
	first.Wrap(func(context.Context, consumer.Message) error { return nil })(context.Background(), consumer.Message{Topic: "first"})
	firstOutput := scrape(t, first)
	secondOutput := scrape(t, second)
	if !strings.Contains(firstOutput, `topic="first"`) || strings.Contains(firstOutput, `topic="second"`) {
		t.Fatalf("first registry is not isolated:\n%s", firstOutput)
	}
	if !strings.Contains(secondOutput, `topic="second"`) || strings.Contains(secondOutput, `topic="first"`) {
		t.Fatalf("second registry is not isolated:\n%s", secondOutput)
	}
	recorder := httptest.NewRecorder()
	first.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), "kafka_consumer_messages_processed_total") ||
		!strings.Contains(recorder.Body.String(), "kafka_consumer_message_processing_duration_seconds") {
		t.Fatalf("Handler() response = %d, body:\n%s", recorder.Code, recorder.Body.String())
	}
}

func scrape(t *testing.T, m *Consumer) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Code != 200 {
		t.Fatalf("metrics scrape status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}
