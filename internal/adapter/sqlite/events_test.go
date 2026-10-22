package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"example.com/go-backend-example/internal/message"
)

func readEvents(t *testing.T, store *Store) map[message.Source]message.Event {
	t.Helper()
	rows, err := store.sqlDB.QueryContext(t.Context(),
		"SELECT source_topic, source_partition, source_offset, user_id, event, timestamp FROM user_events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	result := make(map[message.Source]message.Event)
	for rows.Next() {
		var source message.Source
		var event message.Event
		if err := rows.Scan(&source.Topic, &source.Partition, &source.Offset,
			&event.UserID, &event.Event, &event.Timestamp); err != nil {
			t.Fatal(err)
		}
		if _, exists := result[source]; exists {
			t.Errorf("duplicate source identity %+v", source)
		}
		result[source] = event
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSaveEventPersistsAndDeduplicatesAcrossRestart(t *testing.T) {
	t.Parallel()
	store, dsn := openTestStore(t)
	source := message.Source{Topic: "events", Partition: 0, Offset: 0}
	event := message.Event{UserID: 101, Event: "login", Timestamp: 1729526400}
	if err := store.SaveEvent(t.Context(), source, event); err != nil {
		t.Fatal(err)
	}
	changed := message.Event{UserID: 202, Event: "logout", Timestamp: 0}
	if err := store.SaveEvent(t.Context(), source, changed); err != nil {
		t.Fatalf("replay = %v", err)
	}
	want := map[message.Source]message.Event{source: event}
	if got := readEvents(t, store); !reflect.DeepEqual(got, want) {
		t.Fatalf("events after replay = %+v, want %+v", got, want)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := New(dsn)
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := reopened.SaveEvent(t.Context(), source, changed); err != nil {
		t.Fatalf("replay after restart = %v", err)
	}
	if got := readEvents(t, reopened); !reflect.DeepEqual(got, want) {
		t.Fatalf("events after restart = %+v, want %+v", got, want)
	}
}

func TestSaveEventDistinguishesEverySourceCoordinate(t *testing.T) {
	t.Parallel()
	store, _ := openTestStore(t)
	sources := []message.Source{
		{Topic: "events", Partition: 0, Offset: 0},
		{Topic: "events", Partition: 1, Offset: 0},
		{Topic: "events", Partition: 0, Offset: 1},
		{Topic: "other-events", Partition: 0, Offset: 0},
	}
	event := message.Event{UserID: 101, Event: "login", Timestamp: 0}
	want := make(map[message.Source]message.Event)
	for _, source := range sources {
		if err := store.SaveEvent(t.Context(), source, event); err != nil {
			t.Fatal(err)
		}
		want[source] = event
	}
	if got := readEvents(t, store); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %+v, want %+v", got, want)
	}
}

func TestSaveEventConcurrentReplayAndDistinctRecords(t *testing.T) {
	t.Parallel()
	store, _ := openTestStore(t)
	const count = 48
	event := message.Event{UserID: 101, Event: "login", Timestamp: 1729526400}
	var group sync.WaitGroup
	for i := range count {
		for range 2 {
			group.Go(func() {
				source := message.Source{Topic: "events", Partition: int32(i % 2), Offset: int64(i)}
				if err := store.SaveEvent(t.Context(), source, event); err != nil {
					t.Error(err)
				}
			})
		}
	}
	group.Wait()
	want := make(map[message.Source]message.Event, count)
	for i := range count {
		want[message.Source{Topic: "events", Partition: int32(i % 2), Offset: int64(i)}] = event
	}
	if got := readEvents(t, store); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %+v, want %+v", got, want)
	}
}

func TestSaveEventCancellationAndClosedDatabase(t *testing.T) {
	t.Parallel()
	store, _ := openTestStore(t)
	source := message.Source{Topic: "events", Partition: 0, Offset: 0}
	event := message.Event{UserID: 101, Event: "login", Timestamp: 0}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.SaveEvent(ctx, source, event); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled SaveEvent = %v, want context.Canceled", err)
	}
	if got := readEvents(t, store); len(got) != 0 {
		t.Fatalf("canceled SaveEvent persisted events: %+v", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEvent(t.Context(), source, event); err == nil {
		t.Fatal("SaveEvent succeeded on closed database")
	}
}

func TestSaveEventDoesNotIgnoreOtherConstraints(t *testing.T) {
	t.Parallel()
	store, _ := openTestStore(t)
	if _, err := store.sqlDB.ExecContext(t.Context(),
		"CREATE UNIQUE INDEX one_event_per_user ON user_events (user_id)"); err != nil {
		t.Fatal(err)
	}
	event := message.Event{UserID: 101, Event: "login", Timestamp: 0}
	first := message.Source{Topic: "events", Partition: 0, Offset: 0}
	second := message.Source{Topic: "events", Partition: 0, Offset: 1}
	if err := store.SaveEvent(t.Context(), first, event); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEvent(t.Context(), second, event); err == nil {
		t.Fatal("SaveEvent ignored an unrelated unique constraint")
	}
	want := map[message.Source]message.Event{first: event}
	if got := readEvents(t, store); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %+v, want %+v", got, want)
	}
}

func TestEventSchemaRequiresPayloadFields(t *testing.T) {
	t.Parallel()
	store, _ := openTestStore(t)
	tests := []struct {
		name      string
		userID    any
		event     any
		timestamp any
	}{
		{name: "null user id", userID: nil, event: "login", timestamp: 0},
		{name: "null event", userID: 101, event: nil, timestamp: 0},
		{name: "null timestamp", userID: 101, event: "login", timestamp: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.sqlDB.ExecContext(t.Context(),
				"INSERT INTO user_events (source_topic, source_partition, source_offset, user_id, event, timestamp) "+
					"VALUES (?, ?, ?, ?, ?, ?)",
				"events",
				0,
				0,
				tt.userID,
				tt.event,
				tt.timestamp,
			)
			if err == nil {
				t.Fatal("schema accepted a null required field")
			}
		})
	}
}

func TestEventSchemaMigrationPreservesGreetingsAndCanRecover(t *testing.T) {
	t.Parallel()
	dsn := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, statement := range []string{
		"CREATE TABLE greetings (id integer PRIMARY KEY AUTOINCREMENT, message text NOT NULL)",
		"INSERT INTO greetings (message) VALUES ('existing')",
		"CREATE VIEW user_events AS SELECT 'events' AS source_topic",
	} {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	store := New(dsn)
	if err := store.Start(t.Context()); err == nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal("Start accepted a view in place of user_events")
	}
	if _, err := db.ExecContext(t.Context(), "DROP VIEW user_events"); err != nil {
		t.Fatal(err)
	}
	if err := store.Start(t.Context()); err != nil {
		t.Fatalf("Start after schema repair = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	greeting, err := store.Find(t.Context(), 1)
	if err != nil || greeting.Message != "existing" {
		t.Fatalf("legacy greeting = %+v, %v", greeting, err)
	}
	source := message.Source{Topic: "events", Partition: 0, Offset: 0}
	event := message.Event{UserID: 101, Event: "login", Timestamp: 0}
	if err := store.SaveEvent(t.Context(), source, event); err != nil {
		t.Fatalf("SaveEvent after migration = %v", err)
	}
	want := map[message.Source]message.Event{source: event}
	if got := readEvents(t, store); !reflect.DeepEqual(got, want) {
		t.Fatalf("migrated events = %+v, want %+v", got, want)
	}
}
