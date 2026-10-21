package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"example.com/go-backend-example/internal/adapter/metrics"
	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewKafkaRunDisabledSkipsValidationAndSetup(t *testing.T) {
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "missing-aws-config"))
	t.Setenv("AWS_PROFILE", "missing-profile")
	for _, cfg := range []consumer.Config{
		{},
		{Kafka: consumer.KafkaConfig{Auth: consumer.AuthAWSIAM, Username: "invalid", Password: "invalid"}},
	} {
		run, err := newKafkaRun(Config{Consumer: cfg}, zap.NewNop(), metrics.New(cfg.Topics))
		if err != nil || run != nil {
			t.Fatalf("disabled Kafka runner is nil = %t, error = %v", run == nil, err)
		}
	}
}

func TestNewKafkaRunRejectsEnabledInvalidConfig(t *testing.T) {
	cfg := consumer.DefaultConfig()
	cfg.Kafka.Enabled = true
	cfg.Kafka.GroupID = ""
	if run, err := newKafkaRun(Config{Consumer: cfg}, zap.NewNop(), metrics.New(cfg.Topics)); err == nil || run != nil {
		t.Fatalf("invalid Kafka runner is nil = %t, error = %v", run == nil, err)
	}
	application := New(Config{Consumer: cfg}, zap.NewNop())
	if application.Err() == nil {
		t.Fatal("invalid enabled Kafka config was accepted by Fx")
	}
}

func TestNewKafkaRunDefersAWSSetupUntilExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aws-config")
	if err := os.WriteFile(path, []byte("[profile configured]\nregion = us-east-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", path)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing-credentials"))
	t.Setenv("AWS_PROFILE", "missing-profile")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	cfg := consumer.DefaultConfig()
	cfg.Kafka.Enabled = true
	cfg.Kafka.Auth = consumer.AuthAWSIAM
	run, err := newKafkaRun(Config{Consumer: cfg}, zap.NewNop(), metrics.New(cfg.Topics))
	if err != nil || run == nil {
		t.Fatalf("Kafka constructor performed AWS setup: %v", err)
	}
	dsn := filepath.Join(t.TempDir(), "not-created.db")
	application := New(Config{SQLiteDSN: dsn, Consumer: cfg}, zap.NewNop())
	if err := application.Err(); err != nil {
		t.Fatalf("Fx construction performed AWS setup: %v", err)
	}
	if _, err := os.Stat(dsn); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database exists before startup: %v", err)
	}
	if err := run(t.Context()); err == nil {
		t.Fatal("runner did not report invalid AWS setup at runtime")
	}
}

type syncTracker struct{ count atomic.Int32 }

func (*syncTracker) Write(p []byte) (int, error) { return len(p), nil }
func (s *syncTracker) Sync() error {
	s.count.Add(1)
	return nil
}

func (s *syncTracker) logger() *zap.Logger {
	return zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), s, zapcore.InfoLevel))
}

func TestRunFlushesLoggerOnGraphErrorAndCanceledStart(t *testing.T) {
	graphErr := errors.New("graph rejected")
	for _, graphFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled start", true: "graph error"}[graphFailure], func(t *testing.T) {
			sink := &syncTracker{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var options []fx.Option
			want := error(context.Canceled)
			if graphFailure {
				options = append(options, fx.Error(graphErr))
				want = graphErr
			} else {
				cancel()
			}
			dsn := filepath.Join(t.TempDir(), "not-created.db")
			if err := Run(ctx, Config{SQLiteDSN: dsn}, sink.logger(), options...); !errors.Is(err, want) {
				t.Fatalf("Run = %v, want %v", err, want)
			}
			if got := sink.count.Load(); got != 1 {
				t.Fatalf("logger synced %d times", got)
			}
			if _, err := os.Stat(dsn); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("database exists after failed startup: %v", err)
			}
		})
	}
}
