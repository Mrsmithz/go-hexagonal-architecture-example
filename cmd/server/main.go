package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"example.com/go-backend-example/internal/app"
	"example.com/go-backend-example/internal/consumer"
	"go.uber.org/zap"
)

func main() {
	os.Exit(runProcess())
}

func runProcess() int {
	logger, err := app.ProductionLogger()
	if err != nil {
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = run(ctx, os.Getenv, logger)
	stop()
	if err != nil {
		logger.Error("server failed", zap.Error(err))
		_ = logger.Sync()
		return 1
	}
	_ = logger.Sync()
	return 0
}

func run(ctx context.Context, getenv func(string) string, logger *zap.Logger) error {
	config := app.ConfigFrom(getenv)
	kafkaConfig, err := consumer.LoadConfig(strings.TrimSpace(getenv("CONFIG_FILE")))
	if err != nil {
		return err
	}
	config.Consumer = kafkaConfig
	return app.Run(ctx, config, logger)
}
