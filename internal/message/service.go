package message

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"example.com/go-backend-example/internal/consumer"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

type Source struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Event struct {
	UserID    int64
	Event     string
	Timestamp int64
}

type Store interface {
	SaveEvent(context.Context, Source, Event) error
}

type Service struct {
	store     Store
	logger    *zap.Logger
	validator *validator.Validate
}

type payload struct {
	UserID    int64  `json:"user_id" validate:"required,gt=0"`
	Event     string `json:"event" validate:"required"`
	Timestamp *int64 `json:"timestamp" validate:"required"`
}

func NewService(store Store, logger *zap.Logger) *Service {
	return &Service{
		store:     store,
		logger:    logger,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (s *Service) Handle(ctx context.Context, record consumer.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
	decoder.DisallowUnknownFields()
	var input payload
	if err := decoder.Decode(&input); err != nil {
		return errors.New("decode event: invalid JSON or fields")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return errors.New("decode trailing event data: invalid JSON")
		}
		return errors.New("event must contain exactly one JSON object")
	}
	input.Event = strings.TrimSpace(input.Event)
	if err := s.validator.Struct(input); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	source := Source{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset}
	event := Event{UserID: input.UserID, Event: input.Event, Timestamp: *input.Timestamp}
	if err := s.store.SaveEvent(ctx, source, event); err != nil {
		return fmt.Errorf("save event: %w", err)
	}
	s.logger.Info("kafka message processed",
		zap.String("topic", record.Topic),
		zap.Int32("partition", record.Partition),
		zap.Int64("offset", record.Offset),
	)
	return nil
}
