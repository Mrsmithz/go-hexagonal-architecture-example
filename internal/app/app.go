package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"example.com/go-backend-example/internal/adapter/httpapi"
	"example.com/go-backend-example/internal/adapter/metrics"
	"example.com/go-backend-example/internal/adapter/sqlite"
	"example.com/go-backend-example/internal/consumer"
	"example.com/go-backend-example/internal/greeting"
	"example.com/go-backend-example/internal/server"
	"github.com/labstack/echo/v5"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
)

type Config struct {
	HTTPAddress string
	SQLiteDSN   string
	StopTimeout time.Duration
	Consumer    consumer.Config
}

func ConfigFrom(getenv func(string) string) Config {
	address := strings.TrimSpace(getenv("HTTP_ADDR"))
	if address == "" {
		address = ":8080"
	}
	dsn := strings.TrimSpace(getenv("SQLITE_DSN"))
	if dsn == "" {
		dsn = "greetings.db"
	}
	return Config{HTTPAddress: address, SQLiteDSN: dsn, StopTimeout: server.ShutdownTimeout}
}

func ProductionLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	return cfg.Build()
}

func New(config Config, logger *zap.Logger, extra ...fx.Option) *fx.App {
	config = normalizeConfig(config)
	if logger == nil {
		logger = zap.NewNop()
	}
	options := []fx.Option{
		fx.StopTimeout(config.StopTimeout),
		fx.Supply(config, logger),
		fx.Provide(func() *shutdownState { return &shutdownState{} }),
		fx.Provide(func(config Config) *sqlite.Store { return sqlite.New(config.SQLiteDSN) }),
		fx.Provide(func(store *sqlite.Store) greeting.Store { return store }),
		fx.Provide(greeting.NewService),
		fx.Provide(func(config Config) *metrics.Consumer { return metrics.New(config.Consumer.Topics) }),
		fx.Provide(newKafkaRun),
		fx.Provide(func(service *greeting.Service) httpapi.Service { return service }),
		fx.Provide(func(service httpapi.Service, logger *zap.Logger, consumerMetrics *metrics.Consumer) *echo.Echo {
			handler := httpapi.New(service, logger)
			handler.GET("/metrics", echo.WrapHandler(consumerMetrics.Handler()))
			return handler
		}),
		fx.Provide(func(config Config, handler *echo.Echo, logger *zap.Logger) *server.HTTP {
			return server.New(config.HTTPAddress, handler, logger)
		}),
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			eventLogger := &fxevent.ZapLogger{Logger: logger}
			eventLogger.UseLogLevel(zap.DebugLevel)
			return eventLogger
		}),
		fx.Invoke(registerLifecycle),
	}
	options = append(options, extra...)
	return fx.New(options...)
}

type shutdownState struct {
	mu       sync.Mutex
	stopping bool
	once     sync.Once
	cleanup  func(context.Context) error
	err      error
}

func (s *shutdownState) Stop(ctx context.Context) error {
	s.once.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stopping = true
		if s.cleanup != nil {
			s.err = s.cleanup(ctx)
		}
	})
	return s.err
}

func registerLifecycle(lifecycle fx.Lifecycle, logger *zap.Logger, store *sqlite.Store, httpServer *server.HTTP, runKafka kafkaRun, shutdowner fx.Shutdowner, shutdown *shutdownState) {
	var monitorDone <-chan struct{}
	var cancelKafka context.CancelFunc
	var kafkaDone chan struct{}
	var kafkaErr error
	shutdown.cleanup = func(ctx context.Context) error {
		if cancelKafka != nil {
			cancelKafka()
		}
		httpErr := httpServer.Stop(ctx)
		if monitorDone != nil {
			<-monitorDone
		}
		if kafkaDone != nil {
			<-kafkaDone
		}
		dbErr := store.Close()
		loggerErr := syncLogger(logger)
		return errors.Join(httpErr, kafkaErr, dbErr, loggerErr)
	}
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			err := func() error {
				shutdown.mu.Lock()
				defer shutdown.mu.Unlock()
				if shutdown.stopping {
					return context.Canceled
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := store.Start(ctx); err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := httpServer.Start(ctx); err != nil {
					return err
				}
				monitorDone = watchHTTP(httpServer.Wait, logger, shutdowner)
				if err := ctx.Err(); err != nil {
					return err
				}
				if runKafka != nil {
					var kafkaCtx context.Context
					kafkaCtx, cancelKafka = context.WithCancel(context.Background())
					kafkaDone = make(chan struct{})
					go func() {
						defer close(kafkaDone)
						if err := runKafka(kafkaCtx); err != nil && !(kafkaCtx.Err() != nil && errors.Is(err, context.Canceled)) {
							kafkaErr = err
							logger.Error("kafka consumer failed", zap.Error(err))
							_ = shutdowner.Shutdown(fx.ExitCode(1))
						}
					}()
				}
				return nil
			}()
			if err != nil {
				return errors.Join(err, shutdown.Stop(ctx))
			}
			return nil
		},
		OnStop: shutdown.Stop,
	})
}

func watchHTTP(wait func() error, logger *zap.Logger, shutdowner fx.Shutdowner) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := wait(); err != nil {
			logger.Error("http server failed", zap.Error(err))
			_ = shutdowner.Shutdown(fx.ExitCode(1))
		}
	}()
	return done
}

func syncLogger(logger *zap.Logger) error {
	return filterSyncError(logger.Sync())
}

func filterSyncError(err error) error {
	if err == nil || err == os.ErrInvalid || err == syscall.EINVAL || err == syscall.ENOTTY {
		return nil
	}
	if multiple, ok := err.(interface{ Unwrap() []error }); ok {
		var remaining []error
		for _, item := range multiple.Unwrap() {
			if filtered := filterSyncError(item); filtered != nil {
				remaining = append(remaining, filtered)
			}
		}
		return errors.Join(remaining...)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return filterSyncError(wrapped.Unwrap())
	}
	return err
}

func Run(ctx context.Context, config Config, logger *zap.Logger, extra ...fx.Option) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	var shutdown *shutdownState
	options := append([]fx.Option{fx.Populate(&shutdown)}, extra...)
	app := New(config, logger, options...)
	cleanup := func() error {
		if shutdown == nil || shutdown.cleanup == nil {
			return syncLogger(logger)
		}
		cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
		cancelCleanup()
		return shutdown.Stop(cleanupCtx)
	}
	startCtx, cancelStart := context.WithTimeout(ctx, app.StartTimeout())
	startErr := app.Start(startCtx)
	cancelStart()
	if startErr != nil {
		return errors.Join(startErr, stopApp(app), cleanup())
	}
	var runtimeErr error
	select {
	case <-ctx.Done():
	case signal := <-app.Wait():
		if signal.ExitCode != 0 {
			runtimeErr = fmt.Errorf("application requested exit code %d", signal.ExitCode)
		}
	}
	stopErr := stopApp(app)
	return errors.Join(runtimeErr, stopErr, cleanup())
}

func normalizeConfig(config Config) Config {
	if config.HTTPAddress == "" {
		config.HTTPAddress = ":8080"
	}
	if config.SQLiteDSN == "" {
		config.SQLiteDSN = "greetings.db"
	}
	if config.StopTimeout <= 0 {
		config.StopTimeout = server.ShutdownTimeout
	}
	return config
}

func stopApp(app *fx.App) error {
	ctx, cancel := context.WithTimeout(context.Background(), app.StopTimeout())
	defer cancel()
	return app.Stop(ctx)
}
