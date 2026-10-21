package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Topic       string
	Key         []byte
	Value       []byte
	Headers     []Header
	Partition   int32
	LeaderEpoch int32
	Offset      int64
	Time        time.Time
}

type Handler func(context.Context, Message) error

type client interface {
	Poll(context.Context, int) ([]Message, error)
	Commit(context.Context, ...Message) error
	AllowRebalance()
	CloseAllowingRebalance()
}

func Run[C client](ctx context.Context, cfg Config, handler Handler, newClient func(Config, TopicConfig) (C, error)) error {
	if ctx == nil {
		return errors.New("consumer context must not be nil")
	}
	if handler == nil {
		return errors.New("consumer handler must not be nil")
	}
	if newClient == nil {
		return errors.New("consumer client factory must not be nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid consumer config: %w", err)
	}

	clients := make([]client, 0)
	for _, topic := range cfg.Topics {
		for range topic.Consumers {
			if err := ctx.Err(); err != nil {
				closeClients(clients)
				return err
			}
			created, err := newClient(cfg, topic)
			if err != nil {
				closeClients(clients)
				return fmt.Errorf("create consumer for topic %q: %w", topic.Name, err)
			}
			clients = append(clients, created)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	semaphore := make(chan struct{}, cfg.ProcessingConcurrency)
	var clientGroup sync.WaitGroup
	var errorMu sync.Mutex
	var firstError error
	for _, consumerClient := range clients {
		clientGroup.Go(func() {
			err := runClient(runCtx, cfg, handler, consumerClient, semaphore)
			if err == nil || ctx.Err() != nil {
				return
			}
			errorMu.Lock()
			if firstError == nil {
				firstError = err
				cancel()
			}
			errorMu.Unlock()
		})
	}
	clientGroup.Wait()
	closeClients(clients)

	return firstError
}

func runClient(ctx context.Context, cfg Config, handler Handler, client client, semaphore chan struct{}) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		records, err := client.Poll(ctx, cfg.BatchSize)
		if err != nil {
			client.AllowRebalance()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("poll records: %w", err)
		}
		if len(records) == 0 {
			client.AllowRebalance()
			continue
		}

		batchCtx, cancelBatch := context.WithTimeout(ctx, cfg.BatchTimeout)
		processErr := processBatch(batchCtx, records, handler, semaphore)
		cancelBatch()
		if processErr != nil {
			client.AllowRebalance()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("process batch: %w", processErr)
		}
		if ctx.Err() != nil {
			client.AllowRebalance()
			return nil
		}

		commitCtx, cancelCommit := context.WithTimeout(ctx, cfg.CommitTimeout)
		commitErr := client.Commit(commitCtx, records...)
		cancelCommit()
		client.AllowRebalance()
		if commitErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit batch: %w", commitErr)
		}
	}
}

func processBatch(ctx context.Context, records []Message, handler Handler, semaphore chan struct{}) error {
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var handlers sync.WaitGroup
	var errorMu sync.Mutex
	var firstError error
	for _, record := range records {
		handlers.Go(func() {
			select {
			case semaphore <- struct{}{}:
			case <-processCtx.Done():
				return
			}
			defer func() { <-semaphore }()
			if err := handler(processCtx, record); err != nil {
				errorMu.Lock()
				if firstError == nil {
					firstError = err
				}
				errorMu.Unlock()
				cancel()
			}
		})
	}
	handlers.Wait()
	if firstError != nil {
		return firstError
	}
	return processCtx.Err()
}

func closeClients(clients []client) {
	var closing sync.WaitGroup
	for _, client := range clients {
		closing.Go(client.CloseAllowingRebalance)
	}
	closing.Wait()
}
