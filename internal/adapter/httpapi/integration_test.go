//go:build integration

package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"example.com/go-backend-example/internal/adapter/httpapi"
	"example.com/go-backend-example/internal/adapter/sqlite"
	"example.com/go-backend-example/internal/greeting"
)

type resource struct {
	ID      uint64 `json:"id"`
	Message string `json:"message"`
}

func newStack(t *testing.T) (*http.Client, string) {
	t.Helper()
	store := sqlite.New(filepath.Join(t.TempDir(), "greetings.db"))
	if err := store.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	srv := httptest.NewServer(httpapi.New(greeting.NewService(store), nil))
	t.Cleanup(srv.Close)
	client := srv.Client()
	client.Timeout = 5 * time.Second
	return client, srv.URL
}

func exchange(client *http.Client, method, url, body string) (int, http.Header, []byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		return 0, nil, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, response.Header, data, err
}

func expectJSON(t *testing.T, client *http.Client, method, url, body string, status int, expected any) http.Header {
	t.Helper()
	gotStatus, headers, data, err := exchange(client, method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if gotStatus != status {
		t.Fatalf("%s %s = %d %s, want %d", method, url, gotStatus, data, status)
	}
	if expected == nil {
		if len(data) != 0 {
			t.Fatalf("expected empty response, got %q", data)
		}
		return headers
	}
	var got, want any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	wantData, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wantData, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s %s = %s, want %s", method, url, data, wantData)
	}
	return headers
}

func TestRealStackCRUDOverHTTP(t *testing.T) {
	t.Parallel()
	client, base := newStack(t)
	collection := base + "/api/v1/greetings"
	expectJSON(t, client, "GET", base+"/hello", "", 200, map[string]string{"message": "Hello, World!"})
	expectJSON(t, client, "GET", collection, "", 200, []resource{})
	first := resource{1, "Hello, 世界!"}
	headers := expectJSON(t, client, "POST", collection, `{"message":"  Hello, 世界!  "}`, 201, first)
	if headers.Get("Location") != "/api/v1/greetings/1" {
		t.Fatalf("location = %q", headers.Get("Location"))
	}
	path := base + headers.Get("Location")
	expectJSON(t, client, "GET", path, "", 200, first)
	second := resource{2, "second"}
	expectJSON(t, client, "POST", collection, `{"message":"second"}`, 201, second)
	expectJSON(t, client, "GET", collection, "", 200, []resource{first, second})
	updated := resource{1, "updated"}
	expectJSON(t, client, "PUT", path, `{"message":" updated "}`, 200, updated)
	expectJSON(t, client, "GET", path, "", 200, updated)
	expectJSON(t, client, "GET", collection, "", 200, []resource{updated, second})
	expectJSON(t, client, "DELETE", path, "", 204, nil)
	missing := map[string]any{"error": map[string]string{"code": "not_found", "message": "greeting not found"}}
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		body := ""
		if method == "PUT" {
			body = `{"message":"gone"}`
		}
		expectJSON(t, client, method, path, body, 404, missing)
	}
	expectJSON(t, client, "DELETE", collection+"/2", "", 204, nil)
	expectJSON(t, client, "GET", collection, "", 200, []resource{})
	expectJSON(t, client, "POST", collection, `{"message":"after delete"}`, 201, resource{3, "after delete"})
}

func TestRealStackConcurrentCRUD(t *testing.T) {
	t.Parallel()
	client, base := newStack(t)
	collection := base + "/api/v1/greetings"
	const workers = 24
	ids := make(chan uint64, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			status, headers, data, err := exchange(client, "POST", collection, `{"message":"parallel"}`)
			if err != nil {
				t.Error(err)
				return
			}
			if status != 201 {
				t.Errorf("create = %d %s", status, data)
				return
			}
			var created resource
			if err := json.Unmarshal(data, &created); err != nil {
				t.Error(err)
				return
			}
			if created.ID == 0 || created.Message != "parallel" {
				t.Errorf("created = %+v", created)
				return
			}
			ids <- created.ID
			if headers.Get("Location") != fmt.Sprintf("/api/v1/greetings/%d", created.ID) {
				t.Errorf("location = %q", headers.Get("Location"))
				return
			}
			path := base + headers.Get("Location")
			for _, step := range []struct {
				method, body string
				expected     resource
			}{
				{"GET", "", created},
				{"PUT", `{"message":"changed"}`, resource{created.ID, "changed"}},
				{"GET", "", resource{created.ID, "changed"}},
			} {
				status, _, data, err := exchange(client, step.method, path, step.body)
				if err != nil {
					t.Error(err)
					return
				}
				var got resource
				if err := json.Unmarshal(data, &got); err != nil {
					t.Error(err)
					return
				}
				if status != 200 || got != step.expected {
					t.Errorf("%s = %d %+v, want %+v", step.method, status, got, step.expected)
					return
				}
			}
			status, _, data, err = exchange(client, "DELETE", path, "")
			if err != nil || status != 204 || len(data) != 0 {
				t.Errorf("delete = %d %s %v", status, data, err)
				return
			}
			status, _, _, err = exchange(client, "GET", path, "")
			if err != nil || status != 404 {
				t.Errorf("get deleted = %d %v", status, err)
			}
		})
	}
	wait.Wait()
	close(ids)
	seen := make(map[uint64]bool)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != workers {
		t.Errorf("created %d distinct IDs, want %d", len(seen), workers)
	}
	expectJSON(t, client, "GET", collection, "", 200, []resource{})
}

func TestRealStackRejectsInvalidMessagesWithoutMutation(t *testing.T) {
	t.Parallel()
	client, base := newStack(t)
	collection := base + "/api/v1/greetings"
	for _, body := range []string{`{}`, `{"message":null}`, `{"message":" "}`, `{"message":"valid","id":8}`, `{"message":"ok"}{}`} {
		status, _, data, err := exchange(client, "POST", collection, body)
		if err != nil {
			t.Fatal(err)
		}
		if status != 400 {
			t.Fatalf("body %s = %d %s", body, status, data)
		}
	}
	expectJSON(t, client, "GET", collection, "", 200, []resource{})
}

func TestRealStackMissingIDsAcrossSQLiteIntegerBoundary(t *testing.T) {
	t.Parallel()
	client, base := newStack(t)
	missing := map[string]any{"error": map[string]string{"code": "not_found", "message": "greeting not found"}}
	for _, id := range []string{"9223372036854775807", "9223372036854775808", "18446744073709551615"} {
		for _, method := range []string{"GET", "PUT", "DELETE"} {
			t.Run(method+"/"+id, func(t *testing.T) {
				body := ""
				if method == "PUT" {
					body = `{"message":"absent"}`
				}
				expectJSON(t, client, method, base+"/api/v1/greetings/"+id, body, 404, missing)
			})
		}
	}
}
