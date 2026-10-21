//go:build integration

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/sqlite"
	"go.uber.org/zap"
)

func TestRunStartupFailuresAndDatabaseCleanup(t *testing.T) {
	logger := zap.NewNop()
	badDSN := filepath.Join(t.TempDir(), "missing", "db.sqlite")
	if err := run(t.Context(), func(key string) string {
		switch key {
		case "SQLITE_DSN":
			return badDSN
		case "HTTP_ADDR":
			return "127.0.0.1:0"
		default:
			return ""
		}
	}, logger); err == nil {
		t.Fatal("invalid database path succeeded")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dsn := filepath.Join(t.TempDir(), "listener-error.db")
	if err := run(t.Context(), func(key string) string {
		switch key {
		case "SQLITE_DSN":
			return dsn
		case "HTTP_ADDR":
			return listener.Addr().String()
		}
		return ""
	}, logger); err == nil {
		t.Fatal("occupied listener succeeded")
	}
	store := sqlite.New(dsn)
	if err := store.Start(t.Context()); err != nil {
		t.Fatalf("database could not reopen after listener failure: %v", err)
	}
	store.Close()
	t.Chdir(t.TempDir())
	if err := run(t.Context(), func(key string) string {
		if key == "HTTP_ADDR" {
			return listener.Addr().String()
		}
		return ""
	}, logger); err == nil {
		t.Fatal("default database with occupied listener succeeded")
	}
}

func TestRunServesAndClosesDatabaseWithKafkaDisabled(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	contents := "kafka:\n  enabled: false\n  brokers: []\n  auth: aws-iam\n  tls:\n    enabled: false\n    ca_file: /not-readable\n"
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	probe.Close()
	dsn := filepath.Join(t.TempDir(), "served.db")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	logger := zap.NewNop()
	go func() {
		done <- run(ctx, func(key string) string {
			switch key {
			case "SQLITE_DSN":
				return dsn
			case "HTTP_ADDR":
				return address
			case "CONFIG_FILE":
				return configPath
			}
			return ""
		}, logger)
	}()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/hello")
		if requestErr == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("GET /hello = %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("server did not start: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop")
	}
	store := sqlite.New(dsn)
	if err := store.Start(t.Context()); err != nil {
		t.Fatalf("database could not reopen after shutdown: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunKafkaFailureStopsHTTP(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "config.yaml")
	contents := "kafka:\n  enabled: true\n  brokers: [127.0.0.1:1]\n  tls:\n    enabled: true\n    ca_file: /missing-kafka-ca.pem\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = run(ctx, func(key string) string {
		switch key {
		case "CONFIG_FILE":
			return path
		case "HTTP_ADDR":
			return address
		case "SQLITE_DSN":
			return filepath.Join(directory, "server.db")
		default:
			return ""
		}
	}, zap.NewNop())
	if err == nil || !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("Kafka runtime failure = %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("Kafka failure did not stop the server before the process deadline")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("HTTP listener leaked after Kafka failure: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunProcessHandlesSIGTERMAndStartupFailure(t *testing.T) {
	t.Chdir(t.TempDir())
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HTTP_ADDR", address)
	t.Setenv("SQLITE_DSN", "process.db")
	done := make(chan int, 1)
	go func() { done <- runProcess() }()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/hello")
		if requestErr == nil {
			response.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("process did not start: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("clean SIGTERM exit = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process did not stop after SIGTERM")
	}
	t.Setenv("HTTP_ADDR", "invalid address")
	t.Setenv("SQLITE_DSN", "failed-process.db")
	if code := runProcess(); code != 1 {
		t.Fatalf("startup failure exit = %d", code)
	}
}
