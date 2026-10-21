package metrics

import (
	"context"
	"net/http"
	"time"

	"example.com/go-backend-example/internal/consumer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Consumer struct {
	registry *prometheus.Registry
	messages *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func New(topics []consumer.TopicConfig) *Consumer {
	registry := prometheus.NewRegistry()
	messages := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_processed_total",
		Help: "Total number of Kafka messages processed.",
	}, []string{"topic", "status"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_consumer_message_processing_duration_seconds",
		Help:    "Time spent processing a Kafka message in seconds.",
		Buckets: []float64{.0001, .001, .01, .1, .5, 1, 2.5, 5, 10, 30, 60, 300},
	}, []string{"topic", "status"})
	registry.MustRegister(messages, duration)
	for _, topic := range topics {
		for _, status := range []string{"success", "failed"} {
			messages.WithLabelValues(topic.Name, status)
			duration.WithLabelValues(topic.Name, status)
		}
	}
	return &Consumer{registry: registry, messages: messages, duration: duration}
}

func (m *Consumer) Wrap(next consumer.Handler) consumer.Handler {
	return func(ctx context.Context, message consumer.Message) error {
		started := time.Now()
		err := next(ctx, message)
		elapsed := time.Since(started).Seconds()
		status := "success"
		if err != nil {
			status = "failed"
		}
		m.messages.WithLabelValues(message.Topic, status).Inc()
		m.duration.WithLabelValues(message.Topic, status).Observe(elapsed)
		return err
	}
}

func (m *Consumer) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
