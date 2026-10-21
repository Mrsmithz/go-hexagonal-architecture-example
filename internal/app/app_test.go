package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/sqlite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestConfigFrom(t *testing.T) {
	config := ConfigFrom(func(key string) string {
		if key == "HTTP_ADDR" {
			return " 127.0.0.1:8181 "
		}
		if key == "SQLITE_DSN" {
			return " ./data.db "
		}
		return ""
	})
	if config.HTTPAddress != "127.0.0.1:8181" || config.SQLiteDSN != "./data.db" || config.StopTimeout != 10*time.Second {
		t.Fatalf("config = %+v", config)
	}
	defaults := ConfigFrom(func(string) string { return " " })
	if defaults.HTTPAddress != ":8080" || defaults.SQLiteDSN != "greetings.db" {
		t.Fatalf("defaults = %+v", defaults)
	}
}

func TestNewBuildsGraphWithoutOpeningDatabase(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "not-created.db")
	app := New(Config{HTTPAddress: "invalid address", SQLiteDSN: dsn}, nil)
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New: %v", err)
	}
	if _, err := os.Stat(dsn); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database exists after fx.New: %v", err)
	}
}

func TestRunReportsDatabaseStartupFailure(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "missing", "db.sqlite")
	if err := Run(t.Context(), Config{HTTPAddress: "invalid address", SQLiteDSN: dsn}, zap.NewNop()); err == nil {
		t.Fatal("invalid database path succeeded")
	}
}

func TestGraphErrorIsReported(t *testing.T) {
	app := New(Config{}, zap.NewNop(),
		fx.Provide(func() int { return 1 }),
		fx.Provide(func() int { return 2 }),
		fx.Invoke(func(int) {}),
	)
	if app.Err() == nil {
		t.Fatal("duplicate provider graph was accepted")
	}
}

func TestRunCleansStoreAfterHTTPBindFailure(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "rollback.db")
	var store *sqlite.Store
	app := New(Config{HTTPAddress: "invalid address", SQLiteDSN: dsn, StopTimeout: time.Second}, zap.NewNop(), fx.Populate(&store))
	if err := app.Start(t.Context()); err == nil {
		t.Fatal("invalid listener address succeeded")
	}
	if store == nil {
		t.Fatal("Fx did not populate the store")
	}
	if _, err := store.Insert(t.Context(), "after rollback"); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("store after failed startup = %v, want closed pool", err)
	}
}

func TestFilterSyncErrorKeepsRealFailures(t *testing.T) {
	failure := errors.New("disk flush failed")
	joined := errors.Join(&os.PathError{Op: "sync", Path: "stdout", Err: syscall.EINVAL}, failure)
	if got := filterSyncError(joined); !errors.Is(got, failure) || strings.Contains(got.Error(), "invalid argument") {
		t.Fatalf("filtered sync error = %v", got)
	}
	if got := filterSyncError(&os.PathError{Op: "sync", Path: "stdout", Err: syscall.ENOTTY}); got != nil {
		t.Fatalf("unsupported stdout sync error = %v", got)
	}
}

type syncFailure struct{ err error }

func (*syncFailure) Write(p []byte) (int, error) { return len(p), nil }
func (w *syncFailure) Sync() error               { return w.err }

func TestSyncLoggerReturnsSupportedSinkFailure(t *testing.T) {
	failure := errors.New("sink flush failed")
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), &syncFailure{err: failure}, zapcore.InfoLevel)
	if err := syncLogger(zap.New(core)); !errors.Is(err, failure) {
		t.Fatalf("syncLogger = %v", err)
	}
}

func TestProductionLoggerCanFlushConsoleOutput(t *testing.T) {
	logger, err := ProductionLogger()
	if err != nil {
		t.Fatal(err)
	}
	if err := syncLogger(logger); err != nil {
		t.Fatalf("console sync = %v", err)
	}
}

func TestHTTPMonitorReportsFailureThroughFx(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean stop", true: "serve failure"}[failed], func(t *testing.T) {
			var shutdowner fx.Shutdowner
			application := fx.New(fx.NopLogger, fx.Populate(&shutdowner))
			if err := application.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := application.Stop(ctx); err != nil {
					t.Error(err)
				}
			})
			signals := application.Wait()
			core, logs := observer.New(zapcore.ErrorLevel)
			failure := errors.New("accept failed")
			done := watchHTTP(func() error {
				if failed {
					return failure
				}
				return nil
			}, zap.New(core), shutdowner)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("HTTP monitor did not finish")
			}
			if !failed {
				select {
				case signal := <-signals:
					t.Fatalf("clean stop requested shutdown: %+v", signal)
				default:
				}
				if logs.Len() != 0 {
					t.Fatalf("clean stop emitted errors: %+v", logs.All())
				}
				return
			}
			select {
			case signal := <-signals:
				if signal.ExitCode != 1 {
					t.Fatalf("serve failure exit code = %d", signal.ExitCode)
				}
			case <-time.After(time.Second):
				t.Fatal("serve failure did not request Fx shutdown")
			}
			if entries := logs.FilterMessage("http server failed").All(); len(entries) != 1 || entries[0].ContextMap()["error"] != failure.Error() {
				t.Fatalf("serve failure logs = %+v", logs.All())
			}
		})
	}
}
