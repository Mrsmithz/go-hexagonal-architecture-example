package app

import (
	"context"

	"example.com/go-backend-example/internal/adapter/kafka"
	"example.com/go-backend-example/internal/adapter/metrics"
	"example.com/go-backend-example/internal/consumer"
	"example.com/go-backend-example/internal/message"
)

type kafkaRun func(context.Context) error

func newKafkaRun(config Config, service *message.Service, consumerMetrics *metrics.Consumer) (kafkaRun, error) {
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
		return consumer.Run(ctx, cfg, consumerMetrics.Wrap(service.Handle), factory)
	}, nil
}
