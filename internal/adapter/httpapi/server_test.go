package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"example.com/go-backend-example/internal/greeting"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type serviceStub struct {
	create func(string) (greeting.Greeting, error)
	get    func(greeting.ID) (greeting.Greeting, error)
	list   func() ([]greeting.Greeting, error)
	update func(greeting.ID, string) (greeting.Greeting, error)
	delete func(greeting.ID) error
}

func (s *serviceStub) Create(_ context.Context, message string) (greeting.Greeting, error) {
	if s.create != nil {
		return s.create(message)
	}
	return greeting.Greeting{ID: 1, Message: message}, nil
}
func (s *serviceStub) Get(_ context.Context, id greeting.ID) (greeting.Greeting, error) {
	if s.get != nil {
		return s.get(id)
	}
	return greeting.Greeting{ID: id, Message: "hello"}, nil
}
func (s *serviceStub) List(context.Context) ([]greeting.Greeting, error) {
	if s.list != nil {
		return s.list()
	}
	return []greeting.Greeting{}, nil
}
func (s *serviceStub) Update(_ context.Context, id greeting.ID, message string) (greeting.Greeting, error) {
	if s.update != nil {
		return s.update(id, message)
	}
	return greeting.Greeting{ID: id, Message: message}, nil
}
func (s *serviceStub) Delete(_ context.Context, id greeting.ID) error {
	if s.delete != nil {
		return s.delete(id)
	}
	return nil
}

func serve(t *testing.T, service Service, method, path, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	logger := zap.NewNop()
	handler := New(service, logger)
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestHello(t *testing.T) {
	response := serve(t, &serviceStub{}, http.MethodGet, "/hello", "", "")
	if response.Code != http.StatusOK || response.Body.String() != "{\"message\":\"Hello, World!\"}\n" {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestInternalFailuresAndPanicsUseInjectedZap(t *testing.T) {
	for _, test := range []struct {
		name   string
		create func(string) (greeting.Greeting, error)
	}{
		{name: "service error", create: func(string) (greeting.Greeting, error) { return greeting.Greeting{}, errors.New("storage failed") }},
		{name: "panic", create: func(string) (greeting.Greeting, error) { panic("broken service") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.ErrorLevel)
			logger := zap.New(core)
			handler := New(&serviceStub{create: test.create}, logger)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/greetings", strings.NewReader(`{"message":"hello"}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d", response.Code)
			}
			if logs.FilterMessage("request failed").Len() != 1 {
				t.Fatalf("logs = %+v", logs.All())
			}
		})
	}
}

type failedResponseWriter struct{ header http.Header }

func (w *failedResponseWriter) Header() http.Header     { return w.header }
func (*failedResponseWriter) WriteHeader(int)           {}
func (*failedResponseWriter) Write([]byte) (int, error) { return 0, errors.New("client write failed") }

func TestFailedErrorResponseWriteUsesInjectedZap(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	handler := New(&serviceStub{create: func(string) (greeting.Greeting, error) { return greeting.Greeting{}, errors.New("storage failed") }}, logger)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/greetings", strings.NewReader(`{"message":"hello"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(&failedResponseWriter{header: make(http.Header)}, request)
	if logs.FilterMessage("request failed").Len() != 1 || logs.FilterMessage("write error response failed").Len() != 1 {
		t.Fatalf("logs = %+v", logs.All())
	}
}

func assertJSONBody(t *testing.T, response *httptest.ResponseRecorder, expected string) {
	t.Helper()
	var got, want any
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(expected), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response = %s, want %s", response.Body.String(), expected)
	}
}

func TestGreetingCRUDResponses(t *testing.T) {
	service := &serviceStub{
		create: func(message string) (greeting.Greeting, error) {
			return greeting.Greeting{ID: 7, Message: message}, nil
		},
		list: func() ([]greeting.Greeting, error) { return []greeting.Greeting{{ID: 7, Message: "hello"}}, nil },
	}
	create := serve(t, service, http.MethodPost, "/api/v1/greetings", "application/json; charset=utf-8", `{"message":"hello"}`)
	if create.Code != http.StatusCreated || create.Header().Get("Location") != "/api/v1/greetings/7" || !strings.Contains(create.Body.String(), `"id":7`) {
		t.Fatalf("create = %d %v %s", create.Code, create.Header(), create.Body.String())
	}
	assertJSONBody(t, create, `{"id":7,"message":"hello"}`)
	get := serve(t, service, http.MethodGet, "/api/v1/greetings/7", "", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"message":"hello"`) {
		t.Fatalf("get = %d %s", get.Code, get.Body.String())
	}
	assertJSONBody(t, get, `{"id":7,"message":"hello"}`)
	list := serve(t, service, http.MethodGet, "/api/v1/greetings", "", "")
	if list.Code != http.StatusOK || !strings.HasPrefix(list.Body.String(), "[") {
		t.Fatalf("list = %d %s", list.Code, list.Body.String())
	}
	assertJSONBody(t, list, `[{"id":7,"message":"hello"}]`)
	update := serve(t, service, http.MethodPut, "/api/v1/greetings/7", "application/json", `{"message":"updated"}`)
	if update.Code != http.StatusOK || !strings.Contains(update.Body.String(), `"message":"updated"`) {
		t.Fatalf("update = %d %s", update.Code, update.Body.String())
	}
	assertJSONBody(t, update, `{"id":7,"message":"updated"}`)
	delete := serve(t, service, http.MethodDelete, "/api/v1/greetings/7", "", "")
	if delete.Code != http.StatusNoContent || delete.Body.Len() != 0 {
		t.Fatalf("delete = %d %q", delete.Code, delete.Body.String())
	}
}

func TestRequestBodyValidation(t *testing.T) {
	cases := []struct {
		name, body, contentType string
		status                  int
	}{
		{name: "wrong content type", body: `{"message":"x"}`, contentType: "text/plain", status: http.StatusUnsupportedMediaType},
		{name: "malformed json", body: `{"message":`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "unknown field", body: `{"message":"x","other":1}`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "trailing object", body: `{"message":"x"}{}`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "trailing scalar", body: `{"message":"x"} true`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "null", body: `null`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "array", body: `[]`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "missing message", body: `{}`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "null message", body: `{"message":null}`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "empty body", contentType: "application/json", status: http.StatusBadRequest},
		{name: "wrong message type", body: `{"message":1}`, contentType: "application/json", status: http.StatusBadRequest},
		{name: "invalid utf8", body: string([]byte{'{', '"', 'm', 'e', 's', 's', 'a', 'g', 'e', '"', ':', '"', 0xff, '"', '}'}), contentType: "application/json", status: http.StatusBadRequest},
		{name: "oversized including whitespace", body: `{"message":"x"}` + strings.Repeat(" ", MaxRequestBytes), contentType: "application/json", status: http.StatusRequestEntityTooLarge},
		{name: "missing content type", body: `{"message":"x"}`, status: http.StatusUnsupportedMediaType},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			response := serve(t, &serviceStub{}, http.MethodPost, "/api/v1/greetings", test.contentType, test.body)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, test.status, response.Body.String())
			}
			mediaType, _, err := mime.ParseMediaType(response.Header().Get("Content-Type"))
			if err != nil || mediaType != "application/json" {
				t.Fatalf("content type = %q", response.Header().Get("Content-Type"))
			}
		})
	}
}

func TestIDValidationAndNotFound(t *testing.T) {
	for _, path := range []string{"/api/v1/greetings/0", "/api/v1/greetings/-1", "/api/v1/greetings/+1", "/api/v1/greetings/1x", "/api/v1/greetings/18446744073709551616"} {
		t.Run(path, func(t *testing.T) {
			response := serve(t, &serviceStub{}, http.MethodGet, path, "", "")
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}
	service := &serviceStub{get: func(greeting.ID) (greeting.Greeting, error) {
		return greeting.Greeting{}, fmt.Errorf("lookup: %w", greeting.ErrNotFound)
	}}
	response := serve(t, service, http.MethodGet, "/api/v1/greetings/4", "", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestErrorsDoNotExposeInternalDetails(t *testing.T) {
	service := &serviceStub{
		get: func(greeting.ID) (greeting.Greeting, error) {
			return greeting.Greeting{}, errors.New("database password is secret")
		},
		create: func(string) (greeting.Greeting, error) {
			return greeting.Greeting{}, errors.New("database password is secret")
		},
		list: func() ([]greeting.Greeting, error) { return nil, errors.New("database password is secret") },
		update: func(greeting.ID, string) (greeting.Greeting, error) {
			return greeting.Greeting{}, errors.New("database password is secret")
		},
		delete: func(greeting.ID) error { return errors.New("database password is secret") },
	}
	for _, test := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/greetings", `{"message":"hello"}`},
		{http.MethodGet, "/api/v1/greetings", ""},
		{http.MethodGet, "/api/v1/greetings/1", ""},
		{http.MethodPut, "/api/v1/greetings/1", `{"message":"hello"}`},
		{http.MethodDelete, "/api/v1/greetings/1", ""},
	} {
		contentType := ""
		if test.body != "" {
			contentType = "application/json"
		}
		response := serve(t, service, test.method, test.path, contentType, test.body)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("%s %s status = %d", test.method, test.path, response.Code)
		}
		if strings.Contains(response.Body.String(), "secret") {
			t.Fatalf("internal error leaked: %s", response.Body.String())
		}
		assertJSONBody(t, response, `{"error":{"code":"internal_error","message":"internal server error"}}`)
	}
}

func TestRouterErrorsUseJSONEnvelope(t *testing.T) {
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/missing", http.StatusNotFound},
		{http.MethodPatch, "/hello", http.StatusMethodNotAllowed},
	} {
		response := serve(t, &serviceStub{}, test.method, test.path, "", "")
		if response.Code != test.status {
			t.Fatalf("%s %s status = %d, want %d", test.method, test.path, response.Code, test.status)
		}
		if !strings.Contains(response.Body.String(), `"error"`) {
			t.Fatalf("response lacks error envelope: %s", response.Body.String())
		}
	}
}

func TestDecodeBodyEnforcesLimitBeforeTrailingWhitespace(t *testing.T) {
	body := append(bytes.Repeat([]byte{' '}, MaxRequestBytes), []byte(`{"message":"x"}`)...)
	response := serve(t, &serviceStub{}, http.MethodPost, "/api/v1/greetings", "application/json", string(body))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestMessageFieldMatching(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ name, body, expected string }{
		{"case insensitive field", `{"MESSAGE":"hello"}`, "hello"},
		{"last duplicate wins", `{"message":"first","message":"last"}`, "last"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			service := &serviceStub{create: func(message string) (greeting.Greeting, error) {
				calls++
				if message != test.expected {
					t.Errorf("message = %q, want %q", message, test.expected)
				}
				return greeting.Greeting{ID: 1, Message: message}, nil
			}}
			response := serve(t, service, http.MethodPost, "/api/v1/greetings", "application/json", test.body)
			if response.Code != 201 || calls != 1 {
				t.Fatalf("status = %d, creates = %d", response.Code, calls)
			}
			assertJSONBody(t, response, fmt.Sprintf(`{"id":1,"message":%q}`, test.expected))
		})
	}
}
