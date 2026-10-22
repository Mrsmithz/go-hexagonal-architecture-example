package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"example.com/go-backend-example/internal/consumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

const maxDeadLetterErrorBytes = 4096

type deadLetterHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type deadLetterEnvelope struct {
	Version           int                `json:"version"`
	SourceTopic       string             `json:"source_topic"`
	SourcePartition   int32              `json:"source_partition"`
	SourceOffset      int64              `json:"source_offset"`
	SourceLeaderEpoch int32              `json:"source_leader_epoch"`
	SourceTimestampMS int64              `json:"source_timestamp_ms"`
	Key               []byte             `json:"key"`
	Value             []byte             `json:"value"`
	Headers           []deadLetterHeader `json:"headers"`
	Error             string             `json:"error"`
}

func (c *Client) PublishDeadLetter(ctx context.Context, record consumer.Message, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadLetter, err := newDeadLetterRecord(c.dlqTopic, record, cause)
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	if err := c.client.ProduceSync(ctx, deadLetter).FirstErr(); err != nil {
		return fmt.Errorf("publish dead letter to %q: %w", c.dlqTopic, err)
	}
	return nil
}

func newDeadLetterRecord(topic string, record consumer.Message, cause error) (*kgo.Record, error) {
	headers := make([]deadLetterHeader, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = deadLetterHeader{Key: header.Key, Value: header.Value}
	}
	reason := fmt.Sprint(cause)
	if len(reason) > maxDeadLetterErrorBytes {
		reason = reason[:maxDeadLetterErrorBytes]
	}
	value, err := json.Marshal(deadLetterEnvelope{
		Version:           1,
		SourceTopic:       record.Topic,
		SourcePartition:   record.Partition,
		SourceOffset:      record.Offset,
		SourceLeaderEpoch: record.LeaderEpoch,
		SourceTimestampMS: record.Time.UnixMilli(),
		Key:               record.Key,
		Value:             record.Value,
		Headers:           headers,
		Error:             reason,
	})
	if err != nil {
		return nil, err
	}
	return &kgo.Record{
		Topic: topic,
		Key:   fmt.Appendf(nil, "%s:%d:%d", record.Topic, record.Partition, record.Offset),
		Value: value,
	}, nil
}
