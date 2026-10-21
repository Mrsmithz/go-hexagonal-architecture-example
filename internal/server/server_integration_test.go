//go:build integration

package server

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestStopDrainsActiveRequest(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	h := New("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		if _, err := io.WriteString(w, "completed"); err != nil {
			t.Error(err)
		}
	}), zap.NewNop())
	if err := h.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	type result struct {
		body   string
		status int
		err    error
	}
	responseDone := make(chan result, 1)
	go func() {
		response, err := client.Get("http://" + h.Addr().String())
		if err != nil {
			responseDone <- result{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		responseDone <- result{string(body), response.StatusCode, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached handler")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- h.Stop(t.Context()) }()
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before active request completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	finish()
	select {
	case got := <-responseDone:
		if got.err != nil || got.status != 200 || got.body != "completed" {
			t.Fatalf("response = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request did not complete")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not complete shutdown")
	}
}
