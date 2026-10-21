package message

import (
	"context"

	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/zap"
)

type Service struct {
	logger *zap.Logger
}

func NewService(logger *zap.Logger) *Service {
	return &Service{logger: logger}
}

func (s *Service) Handle(_ context.Context, record consumer.Message) error {
	s.logger.Info("kafka message processed",
		zap.String("topic", record.Topic),
		zap.Int32("partition", record.Partition),
		zap.Int64("offset", record.Offset),
	)
	return nil
}
