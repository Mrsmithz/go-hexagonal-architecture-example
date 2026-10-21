package app

import (
	"context"

	"example.com/go-backend-example/internal/adapter/kafka"
	"example.com/go-backend-example/internal/adapter/metrics"
	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/zap"
)

type kafkaRun func(context.Context) error

func newKafkaRun(config Config, logger *zap.Logger, consumerMetrics *metrics.Consumer) (kafkaRun, error) {
	cfg := config.Consumer
	if !cfg.Kafka.Enabled {
		return nil, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		factory, err := kafka.NewClientFactory(ctx, cfg)
		if err != nil {
			return err
		}
		handler := func(_ context.Context, message consumer.Message) error {
			logger.Info("kafka message processed",
				zap.String("topic", message.Topic),
				zap.Int32("partition", message.Partition),
				zap.Int64("offset", message.Offset),
			)
			return nil
		}
		return consumer.Run(ctx, cfg, consumerMetrics.Wrap(handler), factory)
	}, nil
}
