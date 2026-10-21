package greeting

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type storeStub struct {
	insert  func(context.Context, string) (Greeting, error)
	find    func(context.Context, ID) (Greeting, error)
	all     func(context.Context) ([]Greeting, error)
	replace func(context.Context, ID, string) (Greeting, error)
	remove  func(context.Context, ID) error
	calls   int
}

func (s *storeStub) Insert(ctx context.Context, message string) (Greeting, error) {
	s.calls++
	if s.insert != nil {
		return s.insert(ctx, message)
	}
	return Greeting{ID: 1, Message: message}, nil
}
func (s *storeStub) Find(ctx context.Context, id ID) (Greeting, error) {
	s.calls++
	if s.find != nil {
		return s.find(ctx, id)
	}
	return Greeting{ID: id, Message: "hello"}, nil
}
func (s *storeStub) All(ctx context.Context) ([]Greeting, error) {
	s.calls++
	if s.all != nil {
		return s.all(ctx)
	}
	return []Greeting{}, nil
}
func (s *storeStub) Replace(ctx context.Context, id ID, message string) (Greeting, error) {
	s.calls++
	if s.replace != nil {
		return s.replace(ctx, id, message)
	}
	return Greeting{ID: id, Message: message}, nil
}
func (s *storeStub) Remove(ctx context.Context, id ID) error {
	s.calls++
	if s.remove != nil {
		return s.remove(ctx, id)
	}
	return nil
}

func TestServiceCreateValidation(t *testing.T) {
	store := &storeStub{}
	service := NewService(store)
	for _, test := range []struct {
		name    string
		message string
		want    string
		wantErr bool
	}{
		{name: "trims surrounding whitespace", message: "  hello\n", want: "hello"},
		{name: "unicode code points", message: strings.Repeat("界", MaxMessageRunes), want: strings.Repeat("界", MaxMessageRunes)},
		{name: "rejects empty", message: " \t\n", wantErr: true},
		{name: "rejects over limit", message: strings.Repeat("a", MaxMessageRunes+1), wantErr: true},
		{name: "rejects invalid utf8", message: string([]byte{0xff}), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := service.Create(t.Context(), test.message)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("error = %v, want ErrInvalidMessage", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Message != test.want {
				t.Fatalf("message = %q, want %q", got.Message, test.want)
			}
		})
	}
}

func TestServiceRejectsZeroIDBeforeStore(t *testing.T) {
	store := &storeStub{}
	service := NewService(store)
	if _, err := service.Get(t.Context(), 0); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Get error = %v", err)
	}
	if _, err := service.Update(t.Context(), 0, "hello"); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Update error = %v", err)
	}
	if err := service.Delete(t.Context(), 0); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Delete error = %v", err)
	}
	if store.calls != 0 {
		t.Fatalf("store called %d times", store.calls)
	}
}

func TestServiceCancellationBeforeStore(t *testing.T) {
	store := &storeStub{}
	service := NewService(store)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.Create(ctx, "hello"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create error = %v", err)
	}
	if _, err := service.Get(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get error = %v", err)
	}
	if _, err := service.List(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("List error = %v", err)
	}
	if _, err := service.Update(ctx, 1, "hello"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update error = %v", err)
	}
	if err := service.Delete(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete error = %v", err)
	}
	if store.calls != 0 {
		t.Fatalf("store called %d times", store.calls)
	}
}

func TestServiceWrapsStoreErrors(t *testing.T) {
	failure := errors.New("storage failed")
	store := &storeStub{
		insert:  func(context.Context, string) (Greeting, error) { return Greeting{}, failure },
		find:    func(context.Context, ID) (Greeting, error) { return Greeting{}, failure },
		all:     func(context.Context) ([]Greeting, error) { return nil, failure },
		replace: func(context.Context, ID, string) (Greeting, error) { return Greeting{}, failure },
		remove:  func(context.Context, ID) error { return failure },
	}
	service := NewService(store)
	if _, err := service.Create(t.Context(), "hello"); !errors.Is(err, failure) {
		t.Fatalf("Create error = %v", err)
	}
	if _, err := service.Get(t.Context(), 1); !errors.Is(err, failure) {
		t.Fatalf("Get error = %v", err)
	}
	if _, err := service.List(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("List error = %v", err)
	}
	if _, err := service.Update(t.Context(), 1, "hello"); !errors.Is(err, failure) {
		t.Fatalf("Update error = %v", err)
	}
	if err := service.Delete(t.Context(), 1); !errors.Is(err, failure) {
		t.Fatalf("Delete error = %v", err)
	}
}

func TestServiceSuccessfulOperations(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	expected := Greeting{ID: 42, Message: "hello"}
	store := &storeStub{
		find: func(gotCtx context.Context, id ID) (Greeting, error) {
			if gotCtx != ctx || id != 42 {
				t.Errorf("Find input = %v, %d", gotCtx, id)
			}
			return expected, nil
		},
		all: func(gotCtx context.Context) ([]Greeting, error) {
			if gotCtx != ctx {
				t.Error("List did not propagate context")
			}
			return []Greeting{expected}, nil
		},
		replace: func(gotCtx context.Context, id ID, message string) (Greeting, error) {
			if gotCtx != ctx || id != 42 || message != "updated" {
				t.Errorf("Replace input = %v, %d, %q", gotCtx, id, message)
			}
			return Greeting{ID: id, Message: message}, nil
		},
		remove: func(gotCtx context.Context, id ID) error {
			if gotCtx != ctx || id != 42 {
				t.Errorf("Remove input = %v, %d", gotCtx, id)
			}
			return nil
		},
	}
	service := NewService(store)
	if got, err := service.Get(ctx, 42); err != nil || got != expected {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if got, err := service.List(ctx); err != nil || len(got) != 1 || got[0] != expected {
		t.Fatalf("List = %+v, %v", got, err)
	}
	if got, err := service.Update(ctx, 42, " updated "); err != nil || got != (Greeting{ID: 42, Message: "updated"}) {
		t.Fatalf("Update = %+v, %v", got, err)
	}
	if err := service.Delete(ctx, 42); err != nil {
		t.Fatal(err)
	}
}

func TestServiceInvalidMessagesNeverReachStore(t *testing.T) {
	t.Parallel()
	for _, input := range []struct{ name, message string }{
		{"empty", ""}, {"unicode whitespace", "\u2003\t\n"},
		{"too many unicode characters", strings.Repeat("界", MaxMessageRunes+1)},
		{"invalid utf8", string([]byte{0xff})},
	} {
		for _, operation := range []string{"create", "update"} {
			t.Run(operation+"/"+input.name, func(t *testing.T) {
				t.Parallel()
				store := &storeStub{}
				service := NewService(store)
				var err error
				if operation == "create" {
					_, err = service.Create(t.Context(), input.message)
				} else {
					_, err = service.Update(t.Context(), 1, input.message)
				}
				if !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("error = %v", err)
				}
				if store.calls != 0 {
					t.Fatalf("invalid input reached store %d times", store.calls)
				}
			})
		}
	}
}

func TestServicePreservesNotFound(t *testing.T) {
	t.Parallel()
	store := &storeStub{
		find:    func(context.Context, ID) (Greeting, error) { return Greeting{}, ErrNotFound },
		replace: func(context.Context, ID, string) (Greeting, error) { return Greeting{}, ErrNotFound },
		remove:  func(context.Context, ID) error { return ErrNotFound },
	}
	service := NewService(store)
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{"get", func() error { _, err := service.Get(t.Context(), 1); return err }},
		{"update", func() error { _, err := service.Update(t.Context(), 1, "valid"); return err }},
		{"delete", func() error { return service.Delete(t.Context(), 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, ErrNotFound) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
