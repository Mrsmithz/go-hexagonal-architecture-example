# Go backend example

A small Echo HTTP service with a greeting feature, a storage port, and a SQLite adapter. Uber Fx composes the adapters and coordinates their lifecycle. Zap provides structured JSON logs. The greeting core remains independent of Fx, Echo, Zap, and GORM. The same server can run a configurable franz-go Kafka consumer under Fx. It validates event JSON with go-playground/validator and saves events to SQLite. Whole polled batches commit only after every event is saved or its failure is acknowledged by the DLQ. Set `kafka.enabled: true` in `configs/config.yaml` to enable it. Kafka is disabled by default.

Use Go 1.27.3. The module pins Echo v5.4.0, Fx v1.24.0, Zap v1.28.0 and current compatible dependencies in `go.mod` and `go.sum`. These stable versions were checked against the [Go releases](https://go.dev/dl/) and [Go module proxy](https://proxy.golang.org/github.com/labstack/echo/v5/@latest) during implementation.

## Run

```sh
go run ./cmd/server
```

The server listens on `:8080` by default and stores greetings in `greetings.db`. Set `HTTP_ADDR` to change the listener, for example `HTTP_ADDR=127.0.0.1:8081`. Set `SQLITE_DSN` to choose another SQLite database path. Fx opens SQLite and creates the `greetings` and `user_events` tables before binding HTTP. Shutdown cancels Kafka, drains HTTP, and joins Kafka before closing SQLite and flushing logs. Rows persist across restarts, and deleted IDs are not reused. Logs use Zap's production JSON format on stdout.

The SQLite driver uses CGO. Install a C compiler such as GCC before building or running the service.

```sh
curl -i http://localhost:8080/hello
curl http://localhost:8080/metrics
curl -i -H 'Content-Type: application/json' -d '{"message":"Hello there"}' http://localhost:8080/api/v1/greetings
curl -i http://localhost:8080/api/v1/greetings/1
curl -i http://localhost:8080/api/v1/greetings
curl -i -X PUT -H 'Content-Type: application/json' -d '{"message":"Updated"}' http://localhost:8080/api/v1/greetings/1
curl -i -X DELETE http://localhost:8080/api/v1/greetings/1
```

`GET /metrics` exposes two Kafka processing metric families, a completed-message counter and a processing-duration histogram in seconds. Both use `topic` and `status` (`success` or `failed`) labels. See the [Kafka metrics section](docs/kafka-consumer.md#metrics) for their names and semantics.

Run unit tests with `go test ./...`. Run real-listener and Kafka integration tests with `make test-integration`, which uses rootless Podman. See [API](docs/api.md), [architecture](docs/architecture.md), and the [Kafka consumer guide](docs/kafka-consumer.md).

`make check` runs `go vet` and the complete race-enabled integration suite with a weighted statement coverage gate of at least 90 percent across all application packages. `make coverage` writes `coverage.out`; set `COVERAGE_THRESHOLD` to override the gate. `make build` creates `bin/server`, which serves HTTP and optionally consumes Kafka.

The module path `example.com/go-backend-example` is a local example. Replace it and the internal imports when assigning a repository URL.
