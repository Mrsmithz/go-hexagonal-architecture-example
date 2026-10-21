package greeting

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const MaxMessageRunes = 1000

var (
	ErrInvalidMessage = errors.New("greeting: invalid message")
	ErrInvalidID      = errors.New("greeting: invalid id")
	ErrNotFound       = errors.New("greeting: not found")
)

type ID uint64

type Greeting struct {
	ID      ID
	Message string
}

// Store completes each operation atomically and honors context cancellation before mutation.
// All returns a detached snapshot sorted by ID. Find, Replace, and Remove return
// ErrNotFound when an ID is absent.
type Store interface {
	Insert(context.Context, string) (Greeting, error)
	Find(context.Context, ID) (Greeting, error)
	All(context.Context) ([]Greeting, error)
	Replace(context.Context, ID, string) (Greeting, error)
	Remove(context.Context, ID) error
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) Create(ctx context.Context, message string) (Greeting, error) {
	if err := ctx.Err(); err != nil {
		return Greeting{}, err
	}
	message, err := validateMessage(message)
	if err != nil {
		return Greeting{}, err
	}
	if err := ctx.Err(); err != nil {
		return Greeting{}, err
	}
	result, err := s.store.Insert(ctx, message)
	if err != nil {
		return Greeting{}, fmt.Errorf("create greeting: %w", err)
	}
	return result, nil
}

func (s *Service) Get(ctx context.Context, id ID) (Greeting, error) {
	if err := ctx.Err(); err != nil {
		return Greeting{}, err
	}
	if id == 0 {
		return Greeting{}, ErrInvalidID
	}
	result, err := s.store.Find(ctx, id)
	if err != nil {
		return Greeting{}, fmt.Errorf("get greeting: %w", err)
	}
	return result, nil
}

func (s *Service) List(ctx context.Context) ([]Greeting, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := s.store.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list greetings: %w", err)
	}
	return result, nil
}

func (s *Service) Update(ctx context.Context, id ID, message string) (Greeting, error) {
	if err := ctx.Err(); err != nil {
		return Greeting{}, err
	}
	if id == 0 {
		return Greeting{}, ErrInvalidID
	}
	message, err := validateMessage(message)
	if err != nil {
		return Greeting{}, err
	}
	if err := ctx.Err(); err != nil {
		return Greeting{}, err
	}
	result, err := s.store.Replace(ctx, id, message)
	if err != nil {
		return Greeting{}, fmt.Errorf("update greeting: %w", err)
	}
	return result, nil
}

func (s *Service) Delete(ctx context.Context, id ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == 0 {
		return ErrInvalidID
	}
	if err := s.store.Remove(ctx, id); err != nil {
		return fmt.Errorf("delete greeting: %w", err)
	}
	return nil
}

func validateMessage(message string) (string, error) {
	if !utf8.ValidString(message) {
		return "", ErrInvalidMessage
	}
	message = strings.TrimSpace(message)
	if message == "" || utf8.RuneCountInString(message) > MaxMessageRunes {
		return "", ErrInvalidMessage
	}
	return message, nil
}
