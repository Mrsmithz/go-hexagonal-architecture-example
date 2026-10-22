package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"example.com/go-backend-example/internal/consumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeadLetterPreservesSourceBytesAndMetadata(t *testing.T) {
	message := consumer.Message{
		Topic: "events", Partition: 2, Offset: 7, LeaderEpoch: 3,
		Time: time.UnixMilli(1729526400123),
		Key:  []byte{0, 255}, Value: []byte{255, '{', 0},
		Headers: []consumer.Header{{Key: "trace", Value: []byte{255}}, {Key: "trace", Value: nil}},
	}
	cause := errors.New("invalid JSON")
	record, err := newDeadLetterRecord("events.dlq", message, cause)
	if err != nil {
		t.Fatal(err)
	}
	if record.Topic != "events.dlq" || string(record.Key) != "events:2:7" {
		t.Fatalf("DLQ destination or deterministic key = %q %q", record.Topic, record.Key)
	}
	var envelope deadLetterEnvelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != 1 || envelope.SourceTopic != message.Topic || envelope.SourcePartition != message.Partition || envelope.SourceOffset != message.Offset || envelope.SourceLeaderEpoch != message.LeaderEpoch || envelope.SourceTimestampMS != message.Time.UnixMilli() || envelope.Error != cause.Error() {
		t.Fatalf("DLQ metadata = %+v", envelope)
	}
	if !bytes.Equal(envelope.Key, message.Key) || !bytes.Equal(envelope.Value, message.Value) || len(envelope.Headers) != 2 || envelope.Headers[0].Key != "trace" || !bytes.Equal(envelope.Headers[0].Value, []byte{255}) || envelope.Headers[1].Key != "trace" || envelope.Headers[1].Value != nil {
		t.Fatalf("DLQ did not preserve arbitrary bytes or repeated headers: %+v", envelope)
	}
	repeated, err := newDeadLetterRecord("events.dlq", message, cause)
	if err != nil || !bytes.Equal(record.Key, repeated.Key) {
		t.Fatalf("replay changed DLQ key: %q, %v", repeated.Key, err)
	}
}

func TestDeadLetterErrorTextIsBounded(t *testing.T) {
	record, err := newDeadLetterRecord("dlq", consumer.Message{}, errors.New(strings.Repeat("x", 10_000)))
	if err != nil {
		t.Fatal(err)
	}
	var envelope deadLetterEnvelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Error) != maxDeadLetterErrorBytes || envelope.Key != nil || envelope.Value != nil {
		t.Fatalf("DLQ error bytes = %d; nil payload = %t", len(envelope.Error), envelope.Value == nil)
	}
}

func TestDeadLetterProducerAcknowledgesAllReplicasAndAllowsCancellation(t *testing.T) {
	cfg := consumer.DefaultConfig()
	cfg.Topics[0].DLQTopic = "rejected-events"
	client, err := newClient(cfg, cfg.Topics[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.client.Close)
	if client.dlqTopic != "rejected-events" {
		t.Fatalf("DLQ topic = %q", client.dlqTopic)
	}
	if got := client.client.OptValue(kgo.RequiredAcks); !reflect.DeepEqual(got, kgo.AllISRAcks()) {
		t.Fatalf("RequiredAcks = %#v", got)
	}
	if got := client.client.OptValue(kgo.AllowIdempotentProduceCancellation); got != true {
		t.Fatalf("AllowIdempotentProduceCancellation = %v", got)
	}
	client.client.Close()
	if err := client.PublishDeadLetter(t.Context(), consumer.Message{}, errors.New("failed")); !errors.Is(err, kgo.ErrClientClosed) {
		t.Fatalf("PublishDeadLetter() = %v, want closed client", err)
	}
}

func TestDeadLetterCanceledBeforePublish(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (&Client{}).PublishDeadLetter(ctx, consumer.Message{}, errors.New("failed")); !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishDeadLetter() = %v, want canceled", err)
	}
}
