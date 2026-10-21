package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"go.uber.org/zap"

	"example.com/go-backend-example/internal/greeting"
)

const MaxRequestBytes = 16 << 10

type Service interface {
	Create(context.Context, string) (greeting.Greeting, error)
	Get(context.Context, greeting.ID) (greeting.Greeting, error)
	List(context.Context) ([]greeting.Greeting, error)
	Update(context.Context, greeting.ID, string) (greeting.Greeting, error)
	Delete(context.Context, greeting.ID) error
}

type greetingDTO struct {
	ID      uint64 `json:"id"`
	Message string `json:"message"`
}

type messageRequest struct {
	Message *string `json:"message"`
}

type errorEnvelope struct {
	Error errorDTO `json:"error"`
}

type errorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type requestError struct {
	status  int
	code    string
	message string
}

func (e *requestError) Error() string { return e.message }

func New(service Service, logger *zap.Logger) *echo.Echo {
	if logger == nil {
		logger = zap.NewNop()
	}
	e := echo.New()
	e.Use(middleware.Recover())
	e.HTTPErrorHandler = func(c *echo.Context, err error) {
		response, _ := echo.ResolveResponseStatus(c.Response(), err)
		if response != nil && response.Committed {
			logger.Error("write error response failed", zap.Error(err))
			return
		}
		if reqErr, ok := errors.AsType[*requestError](err); ok {
			writeError(logger, c, reqErr.status, reqErr.code, reqErr.message)
			return
		}
		if status := echo.StatusCode(err); status >= 400 && status < 500 {
			switch status {
			case http.StatusNotFound:
				writeError(logger, c, http.StatusNotFound, "not_found", "resource not found")
			case http.StatusMethodNotAllowed:
				writeError(logger, c, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			default:
				writeError(logger, c, status, "request_error", http.StatusText(status))
			}
			return
		}
		logger.Error("request failed", zap.Error(err))
		writeError(logger, c, http.StatusInternalServerError, "internal_error", "internal server error")
	}

	e.GET("/hello", func(c *echo.Context) error {
		return c.JSON(http.StatusOK, struct {
			Message string `json:"message"`
		}{Message: "Hello, World!"})
	})
	e.POST("/api/v1/greetings", func(c *echo.Context) error {
		request, err := decodeMessage(c)
		if err != nil {
			return err
		}
		result, err := service.Create(c.Request().Context(), *request.Message)
		if err != nil {
			return translateError(err)
		}
		c.Response().Header().Set(echo.HeaderLocation, fmt.Sprintf("/api/v1/greetings/%d", result.ID))
		return c.JSON(http.StatusCreated, toDTO(result))
	})
	e.GET("/api/v1/greetings", func(c *echo.Context) error {
		result, err := service.List(c.Request().Context())
		if err != nil {
			return translateError(err)
		}
		items := make([]greetingDTO, 0, len(result))
		for _, item := range result {
			items = append(items, toDTO(item))
		}
		return c.JSON(http.StatusOK, items)
	})
	e.GET("/api/v1/greetings/:id", func(c *echo.Context) error {
		id, err := parseID(c.Param("id"))
		if err != nil {
			return err
		}
		result, err := service.Get(c.Request().Context(), id)
		if err != nil {
			return translateError(err)
		}
		return c.JSON(http.StatusOK, toDTO(result))
	})
	e.PUT("/api/v1/greetings/:id", func(c *echo.Context) error {
		id, err := parseID(c.Param("id"))
		if err != nil {
			return err
		}
		request, err := decodeMessage(c)
		if err != nil {
			return err
		}
		result, err := service.Update(c.Request().Context(), id, *request.Message)
		if err != nil {
			return translateError(err)
		}
		return c.JSON(http.StatusOK, toDTO(result))
	})
	e.DELETE("/api/v1/greetings/:id", func(c *echo.Context) error {
		id, err := parseID(c.Param("id"))
		if err != nil {
			return err
		}
		if err := service.Delete(c.Request().Context(), id); err != nil {
			return translateError(err)
		}
		return c.NoContent(http.StatusNoContent)
	})
	return e
}

func decodeMessage(c *echo.Context) (messageRequest, error) {
	mediaType, _, err := mime.ParseMediaType(c.Request().Header.Get(echo.HeaderContentType))
	if err != nil || !strings.EqualFold(mediaType, echo.MIMEApplicationJSON) {
		return messageRequest{}, &requestError{status: http.StatusUnsupportedMediaType, code: "unsupported_media_type", message: "content type must be application/json"}
	}
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, MaxRequestBytes+1))
	if err != nil {
		return messageRequest{}, &requestError{status: http.StatusBadRequest, code: "invalid_request", message: "could not read request body"}
	}
	if len(body) > MaxRequestBytes {
		return messageRequest{}, &requestError{status: http.StatusRequestEntityTooLarge, code: "body_too_large", message: "request body exceeds 16 KiB"}
	}
	if !utf8.Valid(body) {
		return messageRequest{}, invalidJSON()
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return messageRequest{}, invalidJSON()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var request messageRequest
	if err := decoder.Decode(&request); err != nil {
		return messageRequest{}, invalidJSON()
	}
	if request.Message == nil {
		return messageRequest{}, invalidJSON()
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return messageRequest{}, invalidJSON()
	}
	return request, nil
}

func invalidJSON() error {
	return &requestError{status: http.StatusBadRequest, code: "invalid_request", message: "request body must be one JSON object with a message string"}
}

func parseID(value string) (greeting.ID, error) {
	if value == "" {
		return 0, invalidID()
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, invalidID()
		}
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		return 0, invalidID()
	}
	return greeting.ID(id), nil
}

func invalidID() error {
	return &requestError{status: http.StatusBadRequest, code: "invalid_id", message: "id must be a positive decimal integer"}
}

func translateError(err error) error {
	switch {
	case errors.Is(err, greeting.ErrNotFound):
		return &requestError{status: http.StatusNotFound, code: "not_found", message: "greeting not found"}
	case errors.Is(err, greeting.ErrInvalidMessage), errors.Is(err, greeting.ErrInvalidID):
		return &requestError{status: http.StatusBadRequest, code: "invalid_request", message: "request contains an invalid greeting"}
	default:
		return err
	}
}

func writeError(logger *zap.Logger, c *echo.Context, status int, code, message string) {
	if err := c.JSON(status, errorEnvelope{Error: errorDTO{Code: code, Message: message}}); err != nil {
		logger.Error("write error response failed", zap.Error(err))
	}
}

func toDTO(item greeting.Greeting) greetingDTO {
	return greetingDTO{ID: uint64(item.ID), Message: item.Message}
}
