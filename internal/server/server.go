package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 15 * time.Second
	WriteTimeout      = 30 * time.Second
	IdleTimeout       = 60 * time.Second
	ShutdownTimeout   = 10 * time.Second
)

type HTTP struct {
	address   string
	handler   http.Handler
	logger    *zap.Logger
	server    *http.Server
	listener  net.Listener
	serveDone chan struct{}
	serveErr  error
	mu        sync.Mutex
	listen    func(context.Context, string, string) (net.Listener, error)
}

func New(address string, handler http.Handler, logger *zap.Logger) *HTTP {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HTTP{address: address, handler: handler, logger: logger}
}

func (h *HTTP) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.server != nil {
		return errors.New("http server already started")
	}
	listen := h.listen
	if listen == nil {
		listen = (&net.ListenConfig{}).Listen
	}
	listener, err := listen(ctx, "tcp", h.address)
	if err != nil {
		return err
	}
	s := &http.Server{
		Handler:           h.handler,
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		ErrorLog:          zap.NewStdLog(h.logger),
	}
	h.server, h.listener, h.serveDone = s, listener, make(chan struct{})
	h.logger.Info("http server starting", zap.String("address", listener.Addr().String()))
	go func() {
		err := s.Serve(listener)
		h.mu.Lock()
		h.serveErr = err
		close(h.serveDone)
		h.mu.Unlock()
	}()
	return nil
}

func (h *HTTP) Wait() error {
	h.mu.Lock()
	done := h.serveDone
	h.mu.Unlock()
	if done == nil {
		return errors.New("http server is not started")
	}
	<-done
	h.mu.Lock()
	err := h.serveErr
	h.mu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (h *HTTP) Done() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.serveDone
}

func (h *HTTP) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if errors.Is(h.serveErr, http.ErrServerClosed) {
		return nil
	}
	return h.serveErr
}

func (h *HTTP) Addr() net.Addr {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.listener == nil {
		return nil
	}
	return h.listener.Addr()
}

func (h *HTTP) Stop(ctx context.Context) error {
	h.mu.Lock()
	s, done := h.server, h.serveDone
	h.mu.Unlock()
	if s == nil {
		return nil
	}
	err := s.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, s.Close())
	}
	<-done
	h.mu.Lock()
	serveErr := h.serveErr
	h.mu.Unlock()
	if !errors.Is(serveErr, http.ErrServerClosed) && serveErr != nil {
		err = errors.Join(err, serveErr)
	}
	h.logger.Info("http server stopped")
	return err
}
