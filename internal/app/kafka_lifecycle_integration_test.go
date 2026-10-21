//go:build integration

package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/sqlite"
	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func awaitKafkaEvent(t *testing.T, event <-chan struct{}) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(5 * time.Second):
		t.Fatal("Kafka lifecycle event did not arrive")
	}
}

func assertStoreClosed(t *testing.T, store *sqlite.Store) {
	t.Helper()
	if _, err := store.All(t.Context()); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("store after cleanup = %v, want closed pool", err)
	}
}

func TestKafkaContextOutlivesStartupAndStopsCleanly(t *testing.T) {
	started := make(chan context.Context, 1)
	run := kafkaRun(func(ctx context.Context) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	})
	sink := &syncTracker{}
	core, logs := observer.New(zapcore.ErrorLevel)
	var store *sqlite.Store
	var shutdown *shutdownState
	application := New(Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/lifetime.db"},
		zap.New(zapcore.NewTee(sink.logger().Core(), core)), fx.Replace(run), fx.Populate(&store, &shutdown))
	startCtx, cancelStart := context.WithCancel(t.Context())
	defer cancelStart()
	if err := application.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown.Stop(context.Background()) })
	var runnerCtx context.Context
	select {
	case runnerCtx = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Kafka did not start")
	}
	cancelStart()
	if err := runnerCtx.Err(); err != nil {
		t.Fatalf("Kafka inherited startup context: %v", err)
	}
	if err := application.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := shutdown.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(runnerCtx.Err(), context.Canceled) || logs.Len() != 0 {
		t.Fatalf("clean cancellation = %v, logs = %+v", runnerCtx.Err(), logs.All())
	}
	if got := sink.count.Load(); got != 1 {
		t.Fatalf("logger synced %d times", got)
	}
	assertStoreClosed(t, store)
}

func TestKafkaFailureRequestsFxShutdownAndJoinsError(t *testing.T) {
	for _, failure := range []error{errors.New("Kafka processing failed"), context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			core, logs := observer.New(zapcore.ErrorLevel)
			var store *sqlite.Store
			err := Run(t.Context(), Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/failure.db"}, zap.New(core),
				fx.Replace(kafkaRun(func(context.Context) error { return failure })), fx.Populate(&store))
			if !errors.Is(err, failure) || !strings.Contains(err.Error(), "exit code 1") {
				t.Fatalf("Run = %v, want Kafka failure and exit code 1", err)
			}
			entries := logs.FilterMessage("kafka consumer failed").All()
			if len(entries) != 1 || entries[0].ContextMap()["error"] != failure.Error() {
				t.Fatalf("Kafka failure logs = %+v", entries)
			}
			assertStoreClosed(t, store)
		})
	}
}

func TestKafkaSetupFailureIsRuntimeFxFailure(t *testing.T) {
	cfg := consumer.DefaultConfig()
	cfg.Kafka.Enabled = true
	cfg.Kafka.TLS.CAFile = filepath.Join(t.TempDir(), "missing-ca.pem")
	core, logs := observer.New(zapcore.ErrorLevel)
	var store *sqlite.Store
	err := Run(t.Context(), Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/setup.db", Consumer: cfg}, zap.New(core), fx.Populate(&store))
	if err == nil || !strings.Contains(err.Error(), "exit code 1") || !strings.Contains(err.Error(), "missing-ca.pem") {
		t.Fatalf("Run = %v, want runtime TLS setup failure", err)
	}
	if logs.FilterMessage("kafka consumer failed").Len() != 1 {
		t.Fatalf("setup failure logs = %+v", logs.All())
	}
	assertStoreClosed(t, store)
}

func TestKafkaDoesNotLaunchAfterHTTPStartupFailure(t *testing.T) {
	launched := make(chan struct{}, 1)
	sink := &syncTracker{}
	var store *sqlite.Store
	application := New(Config{HTTPAddress: "invalid address", SQLiteDSN: t.TempDir() + "/rollback.db"}, sink.logger(),
		fx.Replace(kafkaRun(func(context.Context) error { launched <- struct{}{}; return nil })), fx.Populate(&store))
	if err := application.Start(t.Context()); err == nil {
		t.Fatal("invalid HTTP address succeeded")
	}
	select {
	case <-launched:
		t.Fatal("Kafka launched after HTTP startup failed")
	default:
	}
	assertStoreClosed(t, store)
	if got := sink.count.Load(); got != 1 {
		t.Fatalf("rollback synced logger %d times", got)
	}
}

func TestStopBeforeStartPreventsLateAcquisition(t *testing.T) {
	launched := make(chan struct{}, 1)
	sink := &syncTracker{}
	var shutdown *shutdownState
	dsn := t.TempDir() + "/not-created.db"
	application := New(Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: dsn}, sink.logger(), fx.Populate(&shutdown),
		fx.Replace(kafkaRun(func(context.Context) error { launched <- struct{}{}; return nil })))
	if err := shutdown.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := application.Start(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start after cleanup = %v", err)
	}
	select {
	case <-launched:
		t.Fatal("Kafka launched after cleanup")
	default:
	}
	if _, err := os.Stat(dsn); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database acquired after cleanup: %v", err)
	}
	if got := sink.count.Load(); got != 1 {
		t.Fatalf("logger synced %d times", got)
	}
}

func TestRunJoinsKafkaAfterFxStopDeadline(t *testing.T) {
	for _, skipHook := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup in progress", true: "cleanup hook skipped"}[skipHook], func(t *testing.T) {
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			deadlinePassed := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			t.Cleanup(finish)
			sink := &syncTracker{}
			var store *sqlite.Store
			workDone := make(chan error, 1)
			run := kafkaRun(func(ctx context.Context) error {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				_, err := store.Insert(context.Background(), "Kafka completing after deadline")
				workDone <- err
				return err
			})
			done := make(chan error, 1)
			go func() {
				done <- Run(t.Context(), Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/join.db", StopTimeout: 20 * time.Millisecond}, sink.logger(),
					fx.Replace(run), fx.Populate(&store),
					fx.Invoke(func(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner) {
						lifecycle.Append(fx.Hook{
							OnStart: func(context.Context) error { <-entered; return shutdowner.Shutdown() },
							OnStop: func(ctx context.Context) error {
								if skipHook {
									<-ctx.Done()
									close(deadlinePassed)
									return ctx.Err()
								}
								go func() { <-ctx.Done(); close(deadlinePassed) }()
								return nil
							},
						})
					}),
				)
			}()
			awaitKafkaEvent(t, canceled)
			awaitKafkaEvent(t, deadlinePassed)
			if _, err := store.Insert(t.Context(), "before Kafka completes"); err != nil {
				t.Fatalf("database closed before Kafka completed: %v", err)
			}
			if got := sink.count.Load(); got != 0 {
				t.Fatalf("logger synced %d times before Kafka completed", got)
			}
			select {
			case err := <-done:
				t.Fatalf("Run returned before Kafka completed: %v", err)
			default:
			}
			finish()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Run = %v, want Fx deadline", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not join Kafka")
			}
			if err := <-workDone; err != nil {
				t.Fatalf("Kafka lost database access during completion: %v", err)
			}
			assertStoreClosed(t, store)
			if got := sink.count.Load(); got != 1 {
				t.Fatalf("logger synced %d times", got)
			}
		})
	}
}

func TestRunRollsBackKafkaAfterLaterStartupFailure(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "hook failed", true: "startup deadline"}[timeout], func(t *testing.T) {
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			t.Cleanup(finish)
			sink := &syncTracker{}
			var store *sqlite.Store
			failure := errors.New("later startup failed")
			done := make(chan error, 1)
			options := []fx.Option{
				fx.Populate(&store),
				fx.Replace(kafkaRun(func(ctx context.Context) error {
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return ctx.Err()
				})),
				fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(ctx context.Context) error {
						<-entered
						if timeout {
							<-ctx.Done()
							return ctx.Err()
						}
						return failure
					}})
				}),
			}
			if timeout {
				options = append(options, fx.StartTimeout(20*time.Millisecond))
			}
			go func() {
				done <- Run(t.Context(), Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/startup.db", StopTimeout: 20 * time.Millisecond}, sink.logger(), options...)
			}()
			awaitKafkaEvent(t, canceled)
			if _, err := store.Insert(t.Context(), "startup rollback pending Kafka"); err != nil {
				t.Fatal(err)
			}
			if got := sink.count.Load(); got != 0 {
				t.Fatalf("logger synced %d times before rollback joined Kafka", got)
			}
			select {
			case err := <-done:
				t.Fatalf("Run returned before rollback joined Kafka: %v", err)
			default:
			}
			finish()
			select {
			case err := <-done:
				want := failure
				if timeout {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Fatalf("Run = %v, want %v", err, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not finish startup rollback")
			}
			assertStoreClosed(t, store)
			if got := sink.count.Load(); got != 1 {
				t.Fatalf("logger synced %d times", got)
			}
		})
	}
}
