//go:build integration

package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/sqlite"
	"example.com/go-backend-example/internal/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLifecycleServesThenClosesStoreAndLogsEvents(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)
	var store *sqlite.Store
	var httpServer *server.HTTP
	app := New(Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/lifecycle.db", StopTimeout: time.Second}, logger, fx.Populate(&store, &httpServer))
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get("http://" + httpServer.Addr().String() + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Insert(t.Context(), "after stop"); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("store after stop = %v", err)
	}
	if logs.FilterMessage("http server starting").Len() != 1 || logs.FilterMessage("http server stopped").Len() != 1 {
		t.Fatalf("lifecycle logs = %+v", logs.All())
	}
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	core, logs := observer.New(zapcore.DebugLevel)
	dsn := t.TempDir() + "/runner.db"
	go func() {
		done <- Run(ctx, Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: dsn, StopTimeout: time.Second}, zap.New(core))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for logs.FilterMessage("started").Len() == 0 {
		select {
		case err := <-done:
			t.Fatalf("Run returned before cancellation: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Fx did not finish starting")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRunHonorsFxShutdownExitCode(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var store *sqlite.Store
			err := Run(t.Context(), Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/shutdown.db"}, zap.NewNop(),
				fx.Populate(&store),
				fx.Invoke(func(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
						return shutdowner.Shutdown(fx.ExitCode(code))
					}})
				}),
			)
			if code == 0 && err != nil {
				t.Fatalf("clean Fx shutdown = %v", err)
			}
			if code != 0 && (err == nil || !strings.Contains(err.Error(), "exit code 7")) {
				t.Fatalf("failing Fx shutdown = %v", err)
			}
			if _, err := store.Insert(t.Context(), "after shutdown"); err == nil || !strings.Contains(err.Error(), "database is closed") {
				t.Fatalf("pool after Fx shutdown = %v", err)
			}
		})
	}
}

type blockingFlush struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*blockingFlush) Write(p []byte) (int, error) { return len(p), nil }
func (s *blockingFlush) Sync() error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil
}

func TestRunWaitsForCleanupAfterFxDeadline(t *testing.T) {
	for _, skipHook := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup in progress", true: "resource hook skipped"}[skipHook], func(t *testing.T) {
			sink := &blockingFlush{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(sink.release) }) }
			t.Cleanup(release)
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), sink, zapcore.InfoLevel)
			var store *sqlite.Store
			done := make(chan error, 1)
			dsn := t.TempDir() + "/deadline.db"
			go func() {
				done <- Run(t.Context(), Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: dsn, StopTimeout: 10 * time.Millisecond}, zap.New(core),
					fx.Populate(&store),
					fx.Invoke(func(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner) {
						hook := fx.Hook{OnStart: func(context.Context) error {
							return shutdowner.Shutdown()
						}}
						if skipHook {
							hook.OnStop = func(ctx context.Context) error {
								<-ctx.Done()
								return ctx.Err()
							}
						}
						lifecycle.Append(hook)
					}),
				)
			}()
			select {
			case <-sink.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not reach logger flush")
			}
			select {
			case err := <-done:
				t.Fatalf("Run returned before logger flush completed: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			release()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Run = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not join cleanup")
			}
			if _, err := store.All(t.Context()); err == nil || !strings.Contains(err.Error(), "database is closed") {
				t.Fatalf("pool remained open when Run returned: %v", err)
			}
		})
	}
}

func TestServerStopDeadlineForcesCloseAndCancelsHandler(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	httpServer := server.New("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(finished)
	}), zap.NewNop())
	if err := httpServer.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	go func() {
		response, err := http.Get("http://" + httpServer.Addr().String() + "/hold")
		if err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := httpServer.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("forced close did not cancel request")
	}
}
