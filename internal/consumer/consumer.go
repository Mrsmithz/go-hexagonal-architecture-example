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

type topicClient struct {
	client    client
	topic     TopicConfig
	semaphore chan struct{}
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

	clients := make([]topicClient, 0)
	for _, topic := range cfg.Topics {
		semaphore := make(chan struct{}, topic.ProcessingConcurrency)
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
			clients = append(clients, topicClient{client: created, topic: topic, semaphore: semaphore})
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var clientGroup sync.WaitGroup
	var errorMu sync.Mutex
	var firstError error
	for _, consumerClient := range clients {
		clientGroup.Go(func() {
			err := runClient(runCtx, cfg, consumerClient.topic, handler, consumerClient.client, consumerClient.semaphore)
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

func runClient(ctx context.Context, cfg Config, topic TopicConfig, handler Handler, client client, semaphore chan struct{}) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		records, err := client.Poll(ctx, cfg.Kafka.MaxPollRecords)
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

		if err := processAndCommitBatch(ctx, cfg, topic, records, handler, client, semaphore); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func processAndCommitBatch(ctx context.Context, cfg Config, topic TopicConfig, records []Message, handler Handler, client client, semaphore chan struct{}) error {
	defer client.AllowRebalance()
	pollCtx, cancelPoll := context.WithTimeout(ctx, time.Duration(cfg.Kafka.MaxPollIntervalMS)*time.Millisecond)
	defer cancelPoll()
	batchCtx, cancelBatch := context.WithTimeout(pollCtx, topic.BatchTimeout)
	processErr := processBatch(batchCtx, records, handler, semaphore)
	batchErr := batchCtx.Err()
	cancelBatch()
	if batchErr != nil {
		return fmt.Errorf("incomplete batch: %w", errors.Join(batchErr, processErr))
	}
	if err := pollCtx.Err(); err != nil {
		return fmt.Errorf("incomplete batch: %w", errors.Join(err, processErr))
	}

	commitCtx, cancelCommit := context.WithTimeout(pollCtx, topic.CommitTimeout)
	commitErr := client.Commit(commitCtx, records...)
	cancelCommit()
	if commitErr != nil {
		return errors.Join(fmt.Errorf("commit batch: %w", commitErr), processErr)
	}
	if processErr != nil {
		return fmt.Errorf("process batch (offsets committed): %w", processErr)
	}
	return nil
}

func processBatch(ctx context.Context, records []Message, handler Handler, semaphore chan struct{}) error {
	var handlers sync.WaitGroup
	var errorMu sync.Mutex
	var handlerErrors []error
	for _, record := range records {
		handlers.Go(func() {
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-semaphore }()
			if ctx.Err() != nil {
				return
			}
			if err := handler(ctx, record); err != nil {
				errorMu.Lock()
				handlerErrors = append(handlerErrors, fmt.Errorf("handle %s partition %d offset %d: %w", record.Topic, record.Partition, record.Offset, err))
				errorMu.Unlock()
			}
		})
	}
	handlers.Wait()
	return errors.Join(handlerErrors...)
}

func closeClients(clients []topicClient) {
	var closing sync.WaitGroup
	for _, client := range clients {
		closing.Go(client.client.CloseAllowingRebalance)
	}
	closing.Wait()
}
