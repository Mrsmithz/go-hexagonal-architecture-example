package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"

	"example.com/go-backend-example/internal/greeting"
)

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "greetings.db")
	store := New(dsn)
	if err := store.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, dsn
}

func TestStoreCRUDAndMonotonicIDsAcrossReopen(t *testing.T) {
	store, dsn := openTestStore(t)
	first, err := store.Insert(t.Context(), "first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Insert(t.Context(), "second")
	if err != nil {
		t.Fatal(err)
	}
	found, err := store.Find(t.Context(), first.ID)
	if err != nil || found != first {
		t.Fatalf("Find = %+v, %v", found, err)
	}
	if _, err := store.Replace(t.Context(), second.ID, "second"); err != nil {
		t.Fatal(err)
	}
	updated, err := store.Replace(t.Context(), second.ID, "updated")
	if err != nil || updated != (greeting.Greeting{ID: second.ID, Message: "updated"}) {
		t.Fatalf("Replace = %+v, %v", updated, err)
	}
	if err := store.Remove(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := New(dsn)
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	third, err := reopened.Insert(t.Context(), "third")
	if err != nil || third.ID <= second.ID {
		t.Fatalf("reopened insert = %+v, %v", third, err)
	}
	items, err := reopened.All(t.Context())
	if err != nil || len(items) != 2 || items[0] != updated || items[1] != third {
		t.Fatalf("All = %+v, %v", items, err)
	}
	items[0].Message = "detached"
	again, err := reopened.All(t.Context())
	if err != nil || again[0] != updated {
		t.Fatalf("snapshot mutated: %+v, %v", again, err)
	}
}

func TestStoreMissingBoundariesCancellationAndClosed(t *testing.T) {
	store, _ := openTestStore(t)
	empty, err := store.All(t.Context())
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty All = %#v, %v", empty, err)
	}
	for _, id := range []greeting.ID{100, greeting.ID(math.MaxInt64) + 1, greeting.ID(math.MaxUint64)} {
		if _, err := store.Find(t.Context(), id); !errors.Is(err, greeting.ErrNotFound) {
			t.Errorf("Find(%d) = %v", id, err)
		}
		if _, err := store.Replace(t.Context(), id, "x"); !errors.Is(err, greeting.ErrNotFound) {
			t.Errorf("Replace(%d) = %v", id, err)
		}
		if err := store.Remove(t.Context(), id); !errors.Is(err, greeting.ErrNotFound) {
			t.Errorf("Remove(%d) = %v", id, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.Find(ctx, greeting.ID(math.MaxUint64)); !errors.Is(err, context.Canceled) {
		t.Errorf("Find canceled out-of-range ID = %v", err)
	}
	if _, err := store.Replace(ctx, greeting.ID(math.MaxUint64), "x"); !errors.Is(err, context.Canceled) {
		t.Errorf("Replace canceled out-of-range ID = %v", err)
	}
	if err := store.Remove(ctx, greeting.ID(math.MaxUint64)); !errors.Is(err, context.Canceled) {
		t.Errorf("Remove canceled out-of-range ID = %v", err)
	}
	if _, err := store.Insert(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Errorf("Insert canceled = %v", err)
	}
	if _, err := store.All(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("All canceled = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Insert(t.Context(), "closed"); err == nil {
		t.Fatal("Insert on closed store succeeded")
	}
	if _, err := store.Find(t.Context(), 1); err == nil {
		t.Fatal("Find on closed store succeeded")
	}
	if _, err := store.All(t.Context()); err == nil {
		t.Fatal("All on closed store succeeded")
	}
	if _, err := store.Replace(t.Context(), 1, "x"); err == nil {
		t.Fatal("Replace on closed store succeeded")
	}
	if err := store.Remove(t.Context(), 1); err == nil {
		t.Fatal("Remove on closed store succeeded")
	}
}

func TestOpenRejectsBadDatabaseAndCanceledContext(t *testing.T) {
	if err := New("  ").Start(t.Context()); err == nil {
		t.Fatal("Open with empty DSN succeeded")
	}
	if err := New(filepath.Join(t.TempDir(), "missing", "greetings.db")).Start(t.Context()); err == nil {
		t.Fatal("Open with invalid path succeeded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := New(filepath.Join(t.TempDir(), "canceled.db")).Start(ctx); err == nil {
		t.Fatal("Open with canceled context succeeded")
	}
}

func TestStoreConcurrentInserts(t *testing.T) {
	store, _ := openTestStore(t)
	const count = 48
	ids := make(chan greeting.ID, count)
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() {
			item, err := store.Insert(t.Context(), "parallel")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- item.ID
		})
	}
	wg.Wait()
	close(ids)
	seen := make(map[greeting.ID]bool)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID %d", id)
		}
		seen[id] = true
	}
	items, err := store.All(t.Context())
	if err != nil || len(items) != count || len(seen) != count {
		t.Fatalf("stored %d items, %v", len(items), err)
	}
}

func TestOpenFailsOnConflictingSchemaAndCanRecover(t *testing.T) {
	t.Parallel()
	dsn := filepath.Join(t.TempDir(), "conflicting.db")
	prepare := func(statement string) {
		t.Helper()
		db, err := sql.Open("sqlite3", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	prepare("CREATE VIEW greetings AS SELECT 1 AS id, 'existing' AS message")
	store := New(dsn)
	if err := store.Start(t.Context()); err == nil {
		store.Close()
		t.Fatal("Open accepted a view in place of the greetings table")
	}
	prepare("DROP VIEW greetings")
	recovered := New(dsn)
	if err := recovered.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Error(err)
		}
	})
	created, err := recovered.Insert(t.Context(), "recovered")
	if err != nil || created.ID != 1 || created.Message != "recovered" {
		t.Fatalf("insert after schema repair = %+v, %v", created, err)
	}
}
