package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/go-backend-example/internal/adapter/metrics"
	"example.com/go-backend-example/internal/consumer"
	"github.com/labstack/echo/v5"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func TestMetricsEndpointUsesApplicationRegistryWithKafkaDisabled(t *testing.T) {
	cfg := consumer.DefaultConfig()
	failure := errors.New("processing failed")
	for instance := range 2 {
		var router *echo.Echo
		var consumerMetrics *metrics.Consumer
		application := New(Config{Consumer: cfg}, zap.NewNop(), fx.Populate(&router, &consumerMetrics))
		if err := application.Err(); err != nil {
			t.Fatal(err)
		}
		if instance == 0 {
			handler := consumerMetrics.Wrap(func(context.Context, consumer.Message) error { return failure })
			if err := handler(t.Context(), consumer.Message{Topic: "greetings"}); err != failure {
				t.Fatalf("handler error = %v, want original error", err)
			}
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET /metrics status = %d", response.Code)
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(strings.NewReader(response.Body.String()))
		if err != nil {
			t.Fatal(err)
		}
		if len(families) != 2 {
			t.Fatalf("metric families = %d, want exactly two", len(families))
		}
		for name, family := range families {
			if name != "kafka_consumer_messages_processed_total" && name != "kafka_consumer_message_processing_duration_seconds" {
				t.Fatalf("unexpected metric family %s", name)
			}
			if len(family.Metric) != 2 {
				t.Fatalf("%s series = %d, want both statuses", name, len(family.Metric))
			}
			for _, metric := range family.Metric {
				labels := make(map[string]string)
				for _, label := range metric.Label {
					labels[label.GetName()] = label.GetValue()
				}
				if len(labels) != 2 || labels["topic"] != "greetings" || (labels["status"] != "success" && labels["status"] != "failed") {
					t.Fatalf("unexpected labels %v", labels)
				}
				want := float64(0)
				if instance == 0 && labels["status"] == "failed" {
					want = 1
				}
				if name == "kafka_consumer_messages_processed_total" && metric.GetCounter().GetValue() != want {
					t.Fatalf("counter %v = %v, want %v", labels, metric.GetCounter().GetValue(), want)
				}
				if name == "kafka_consumer_message_processing_duration_seconds" && metric.GetHistogram().GetSampleCount() != uint64(want) {
					t.Fatalf("histogram %v count = %v, want %v", labels, metric.GetHistogram().GetSampleCount(), want)
				}
			}
		}
	}
}
