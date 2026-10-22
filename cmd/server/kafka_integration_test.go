//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestServerRunsKafkaAlongsideHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	container, err := kafkamodule.Run(ctx, "docker.io/confluentinc/cp-kafka:8.0.0",
		kafkamodule.WithClusterID("go-backend-fx-tests"),
		testcontainers.WithEnv(map[string]string{"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "1000"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Error(err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const topic = "fx-messages"
	code, output, err := container.Exec(ctx, []string{
		"kafka-topics", "--bootstrap-server", "localhost:9092", "--create",
		"--topic", topic, "--partitions", "2", "--replication-factor", "1",
	}, exec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	result, err := io.ReadAll(output)
	if err != nil || code != 0 {
		t.Fatalf("create topic = %d, %v, %s", code, err, result)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for index := range 6 {
		record := &kgo.Record{Topic: topic, Partition: int32(index % 2), Value: fmt.Appendf(nil, `{"user_id":%d,"event":"login","timestamp":1729526400}`, 101+index)}
		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := fmt.Sprintf("kafka:\n  enabled: true\n  brokers: [%q]\n  group_id: fx-server\n  max.poll.records: 2\n  allow_insecure: true\n  tls:\n    enabled: false\ntopics:\n  - name: %s\n    consumers: 2\n    processing_concurrency: 2\n", brokers[0], topic)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	core, logs := observer.New(zapcore.InfoLevel)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = run(runCtx, func(key string) string {
			switch key {
			case "CONFIG_FILE":
				return path
			case "HTTP_ADDR":
				return address
			case "SQLITE_DSN":
				return filepath.Join(filepath.Dir(path), "server.db")
			default:
				return ""
			}
		}, zap.New(core))
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
			if runErr != nil {
				t.Errorf("server shutdown = %v", runErr)
			}
		case <-time.After(30 * time.Second):
			t.Error("server did not join Kafka during shutdown")
		}
	})
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var groupOutput string
	for {
		select {
		case <-done:
			t.Fatalf("server exited before committing Kafka records: %v", runErr)
		case <-deadline:
			t.Fatalf("server did not commit Kafka records; logs = %+v; group = %q", logs.All(), groupOutput)
		case <-ticker.C:
		}
		if logs.FilterMessage("kafka message processed").Len() != 6 {
			continue
		}
		code, output, err := container.Exec(ctx, []string{
			"kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
			"--group", "fx-server." + topic, "--describe",
		}, exec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		result, err := io.ReadAll(output)
		if err != nil {
			t.Fatal(err)
		}
		groupOutput = string(result)
		committed := 0
		for line := range strings.SplitSeq(groupOutput, "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 6 && fields[1] == topic && fields[3] == "3" && fields[5] == "0" {
				committed++
			}
		}
		if code == 0 && committed == 2 {
			break
		}
	}
	db, err := sql.Open("sqlite3", filepath.Join(filepath.Dir(path), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var persisted int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_events WHERE event = 'login' AND timestamp = 1729526400").Scan(&persisted); err != nil || persisted != 6 {
		t.Fatalf("persisted events = %d, error = %v", persisted, err)
	}
	response, err := client.Get("http://" + address + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HTTP alongside Kafka = %d", response.StatusCode)
	}
	metricsResponse, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricsResponse.Body.Close()
	if metricsResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", metricsResponse.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(metricsResponse.Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 2 {
		t.Fatalf("metric families = %d, want two", len(families))
	}
	for _, name := range []string{"kafka_consumer_messages_processed_total", "kafka_consumer_message_processing_duration_seconds"} {
		family := families[name]
		if len(family.GetMetric()) != 2 {
			t.Fatalf("%s series = %d, want success and failed", name, len(family.GetMetric()))
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if len(labels) != 2 || labels["topic"] != topic || (labels["status"] != "success" && labels["status"] != "failed") {
				t.Fatalf("metric labels = %v", labels)
			}
			want := float64(0)
			if labels["status"] == "success" {
				want = 6
			}
			if name == "kafka_consumer_messages_processed_total" && metric.GetCounter().GetValue() != want {
				t.Fatalf("counter %v = %v, want %v", labels, metric.GetCounter().GetValue(), want)
			}
			if name == "kafka_consumer_message_processing_duration_seconds" && metric.GetHistogram().GetSampleCount() != uint64(want) {
				t.Fatalf("histogram count %v = %v, want %v", labels, metric.GetHistogram().GetSampleCount(), want)
			}
		}
	}
	for _, entry := range logs.FilterMessage("kafka message processed").All() {
		fields := entry.ContextMap()
		if fields["topic"] != topic || fields["partition"] == nil || fields["offset"] == nil || fields["value"] != nil {
			t.Fatalf("processed metadata = %v", fields)
		}
	}
}
