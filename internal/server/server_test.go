package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
)

type failingListener struct {
	err    error
	closed chan struct{}
}

func (l *failingListener) Accept() (net.Conn, error) {
	if l.err != nil {
		return nil, l.err
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *failingListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (l *failingListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
}

func TestNewIsInertAndStartReportsServeFailure(t *testing.T) {
	failure := errors.New("listener failed")
	listener := &failingListener{err: failure, closed: make(chan struct{})}
	h := New("unused", http.NotFoundHandler(), zap.NewNop())
	h.listen = func(context.Context, string, string) (net.Listener, error) { return listener, nil }
	if h.Done() != nil {
		t.Fatal("new server acquired resources")
	}
	if err := h.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Done():
	case <-time.After(time.Second):
		t.Fatal("Serve did not report failure")
	}
	if !errors.Is(h.Err(), failure) {
		t.Fatalf("server error = %v", h.Err())
	}
	if err := h.Wait(); !errors.Is(err, failure) {
		t.Fatalf("Wait = %v", err)
	}
	if err := h.Stop(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("Stop = %v", err)
	}
}

func TestNewWithoutLoggerAndAddressBeforeStart(t *testing.T) {
	h := New("unused", http.NotFoundHandler(), nil)
	if h.Addr() != nil {
		t.Fatal("address exists before Start")
	}
	if err := h.Wait(); err == nil {
		t.Fatal("Wait before Start succeeded")
	}
}

func TestStopClosesListenerAndJoinsServe(t *testing.T) {
	listener := &failingListener{closed: make(chan struct{})}
	h := New("unused", http.NotFoundHandler(), zap.NewNop())
	h.listen = func(context.Context, string, string) (net.Listener, error) { return listener, nil }
	if err := h.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := h.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Done():
	default:
		t.Fatal("Stop returned before Serve exited")
	}
	if err := h.Err(); err != nil {
		t.Fatalf("serve error = %v", err)
	}
}
