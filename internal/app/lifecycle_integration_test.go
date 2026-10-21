//go:build integration

package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/sqlite"
	"example.com/go-backend-example/internal/server"
	"github.com/labstack/echo/v5"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func TestLifecycleClosesDatabaseAfterHTTPShutdown(t *testing.T) {
	for _, forced := range []bool{false, true} {
		t.Run(map[bool]string{false: "graceful drain", true: "deadline"}[forced], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			var store *sqlite.Store
			var httpServer *server.HTTP
			application := New(Config{HTTPAddress: "127.0.0.1:0", SQLiteDSN: t.TempDir() + "/drain.db"}, zap.NewNop(),
				fx.Populate(&store, &httpServer),
				fx.Decorate(func(handler *echo.Echo, database *sqlite.Store) *echo.Echo {
					handler.GET("/drain", func(c *echo.Context) error {
						close(entered)
						select {
						case <-c.Request().Context().Done():
							return nil
						case <-release:
						}
						if _, err := database.Insert(c.Request().Context(), "during shutdown"); err != nil {
							return err
						}
						return c.NoContent(http.StatusNoContent)
					})
					return handler
				}),
			)
			if err := application.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				finish()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = application.Stop(ctx)
			})
			client := &http.Client{Timeout: 5 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			responseDone := make(chan error, 1)
			go func() {
				response, err := client.Get("http://" + httpServer.Addr().String() + "/drain")
				if err == nil {
					response.Body.Close()
					if response.StatusCode != http.StatusNoContent {
						err = errors.New("database was unavailable to the draining request")
					}
				}
				responseDone <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("request did not enter handler")
			}
			budget := 5 * time.Second
			if forced {
				budget = 50 * time.Millisecond
			}
			stopCtx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			stopped := make(chan error, 1)
			go func() { stopped <- application.Stop(stopCtx) }()
			deadline := time.Now().Add(5 * time.Second)
			for {
				connection, err := net.DialTimeout("tcp", httpServer.Addr().String(), 20*time.Millisecond)
				if err != nil {
					break
				}
				connection.Close()
				if time.Now().After(deadline) {
					t.Fatal("shutdown did not close the listener")
				}
				time.Sleep(time.Millisecond)
			}
			if !forced {
				finish()
			}
			select {
			case err := <-stopped:
				if forced && !errors.Is(err, context.DeadlineExceeded) || !forced && err != nil {
					t.Fatalf("Stop = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("application did not stop")
			}
			select {
			case err := <-responseDone:
				if !forced && err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("request did not finish")
			}
			deadline = time.Now().Add(time.Second)
			for {
				_, err := store.All(t.Context())
				if err != nil && strings.Contains(err.Error(), "database is closed") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("SQLite pool remained open after HTTP shutdown: %v", err)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
