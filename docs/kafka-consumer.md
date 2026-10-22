# Kafka consumer

The Fx server runs HTTP and an optional Kafka worker in the same process. Set `kafka.enabled: true` in `configs/config.yaml`, then run:

```sh
go run ./cmd/server
```

Kafka is disabled by default. The server loads `configs/config.yaml` when present. Set `CONFIG_FILE` to require another file. A missing default file leaves HTTP running without Kafka; a missing explicit file fails startup. Disabled Kafka does not initialize authentication, create clients, or contact brokers. The message service validates event JSON and saves it to the same SQLite database as HTTP greetings, in a separate `user_events` table. It logs only source topic, partition, and offset after a successful save.

## Configuration

`configs/config.yaml` in the repository is a local plaintext example for Kafka at `127.0.0.1:29092`. It explicitly sets `kafka.allow_insecure: true` because TLS is disabled. For normal deployments, leave TLS enabled and keep `allow_insecure: false`.

| Key | Meaning |
| --- | --- |
| `kafka.enabled` | Enable the Kafka worker inside the Fx server. Defaults to `false`. |
| `kafka.brokers` | One or more `host:port` broker addresses. |
| `kafka.group_id` | Base group ID. The worker derives `<group_id>.<topic>` so configured topics do not share offsets. |
| `kafka.auto.offset.reset` | `earliest` (default), `latest`, or `none` when no usable committed offset exists. `none` reports an error rather than choosing an offset. Valid committed offsets take precedence. |
| `kafka.fetch.min.bytes` | Minimum bytes requested per broker fetch. Defaults to `1`; accepts `0` through `fetch.max.bytes`. |
| `kafka.fetch.max.bytes` | Requested maximum bytes per broker fetch. Defaults to `52428800` (50 MiB). Positive, at most 1 GiB minus 1 MiB of protocol headroom. Brokers may exceed this for one oversized record batch. |
| `kafka.max.poll.records` | Maximum records returned by one poll per client. Defaults to `50`. Replaces the old root-level `batch_size` key. |
| `kafka.max.poll.interval.ms` | Application deadline for processing and committing a fetched batch, and the group's rebalance timeout. Defaults to `300000` (5 minutes). |
| `kafka.session.timeout.ms` | Consumer-group session timeout. Defaults to `45000` (45 seconds). Must also fit broker session-timeout limits. |
| `kafka.compression.type` | Producer batch codec for the underlying client. Defaults to `lz4`; also accepts `none`, `gzip`, `snappy`, and `zstd`. Does not select compression for consumed messages. |
| `kafka.auth` | `none`, `aws-iam`, `plain`, `scram-sha-256`, or `scram-sha-512`. |
| `kafka.username`, `kafka.password` | Credentials for PLAIN or SCRAM. Prefer `KAFKA_USERNAME` and `KAFKA_PASSWORD` environment overrides. |
| `kafka.tls.enabled` | Enable TLS. TLS is enabled by the built-in defaults. |
| `kafka.tls.ca_file` | Optional PEM CA bundle, added to system roots. |
| `kafka.tls.server_name` | Optional TLS server name override. |
| `kafka.allow_insecure` | Explicit opt-in for local plaintext testing only. IAM always requires TLS. |
| `topics[].name` | Kafka topic to consume. Duplicate or invalid Kafka topic names are rejected. |
| `topics[].consumers` | Number of consumer-group clients for this topic. |
| `topics[].dlq_topic` | Destination for failed messages. Defaults to `<name>.dlq`. Must be a valid Kafka topic and must not match any configured source topic. |
| `topics[].processing_concurrency` | Maximum concurrently executing handlers shared by this topic's clients. Defaults to `10`; must be positive. |
| `topics[].batch_timeout` | Deadline for completing a polled batch, including waiting for this topic's concurrency limit. Defaults to `30s`; must be positive. |
| `topics[].commit_timeout` | Deadline for this topic's synchronous offset commits and group leave during shutdown. Defaults to `10s`; must be positive. |

Each configured client is a member of its topic's derived group. More clients than the topic has partitions remain idle. `kafka.max.poll.records` bounds records held for processing by each client; handler concurrency is separately bounded per topic by `topics[].processing_concurrency`. Clients of one topic share its limit; other topics have independent limits, so total process concurrency can reach the sum of the topic limits. Move the former root-level `processing_concurrency`, `batch_timeout`, and `commit_timeout` settings into each topic entry; the old global keys are rejected. Omitted per-topic settings receive defaults, while explicit zero or negative values are rejected when Kafka is enabled. Migrate existing `batch_size` settings to `kafka.max.poll.records`; the old key is rejected rather than silently ignored. The dotted option names are literal YAML keys under `kafka`, as shown below. Millisecond settings accept integers from `100` through `2147483647`.

Fetch byte limits do not bound total process memory. Multiple brokers can fetch concurrently, and decompressed records can occupy more memory than compressed data. Compression is chosen by the producer or topic, not by this consumer. The client automatically decodes supported incoming codecs. Its LZ4 producer default applies to DLQ records.

A secure username/password configuration uses TLS and environment variables for credentials:

```yaml
kafka:
  enabled: true
  brokers: [broker.example.com:9094]
  group_id: greetings-worker
  auto.offset.reset: earliest
  fetch.min.bytes: 1
  fetch.max.bytes: 52428800
  max.poll.records: 100
  max.poll.interval.ms: 300000
  session.timeout.ms: 45000
  compression.type: lz4
  auth: scram-sha-512
  allow_insecure: false
  tls:
    enabled: true
    ca_file: /etc/ssl/certs/kafka-ca.pem
    server_name: broker.example.com
  username: ""
  password: ""
topics:
  - name: greetings
    dlq_topic: greetings.dlq
    consumers: 2
    processing_concurrency: 8
    batch_timeout: 30s
    commit_timeout: 10s
  - name: audit-events
    consumers: 1
    processing_concurrency: 2
    batch_timeout: 15s
    commit_timeout: 5s
```

```sh
KAFKA_USERNAME='worker' KAFKA_PASSWORD='...' CONFIG_FILE=secure-kafka.yaml go run ./cmd/server
```

Set `auth: plain` to use SASL/PLAIN instead. Both username/password modes require both credentials; do not store production secrets in the YAML file.

For Amazon MSK IAM, set `auth: aws-iam` and keep TLS enabled. The worker uses the AWS SDK v2 default credential chain (environment, shared profiles, container or instance role, and other supported sources); the cached provider is retrieved for each SASL session so expiring credentials can refresh. The franz-go IAM signer derives the region from the broker hostname, with `AWS_REGION` / `AWS_DEFAULT_REGION` as fallback. IAM requires TLS and rejects username/password fields. The repository tests the signing callback with a rotating credential provider; it does not claim a real AWS/MSK handshake test.

Unknown YAML keys and malformed values fail even when Kafka is disabled. Operational validation of bounds, topics, brokers, auth modes, and credentials applies only when Kafka is enabled. `CONFIG_FILE`, `KAFKA_USERNAME`, and `KAFKA_PASSWORD` are the supported environment overrides.

## Message service

`internal/message/service.go` owns application-level message handling through `Service.Handle(context.Context, consumer.Message) error`. Fx injects the service into the Kafka runner, and the metrics decorator surrounds `service.Handle`. `internal/app/kafka.go` only composes the consumer, service, and metrics.

The service accepts one JSON event per Kafka record:

```json
{"user_id": 101, "event": "login", "timestamp": 1729526400}
```

Validation uses `go-playground/validator/v10`. `user_id` must be a positive integer. `event` is trimmed and must not be blank, including whitespace-only text. `timestamp` must be a present, non-null integer; Unix timestamp `0` is valid. Malformed JSON, wrong field types, unknown fields, and trailing JSON are rejected. Standard Go JSON decoding applies, including last-value-wins behavior for duplicate fields.

Fx injects the existing SQLite store through the service's `Store` port. Rows in `user_events` contain `user_id`, `event`, `timestamp`, and the source topic, partition, and offset. The source coordinates form a database-enforced composite primary key. Replaying an already-saved record does not create another row or overwrite it. Greetings remain unchanged. The identity assumes one Kafka cluster and stable topics; topic recreation or switching source clusters requires an explicit data-migration decision.

## Delivery and handler requirements

Offset commits are manual; automatic commits remain disabled. The worker processes each bounded poll batch concurrently without preserving record order. A record is resolved only after its event is saved to SQLite, or its processing failure is synchronously acknowledged by the DLQ. Every record is attempted even when another handler fails. Only after all records resolve does the worker make one synchronous commit call containing the entire original polled batch.

Any DLQ publication error, cancellation, batch deadline expiry, or poll-interval expiry prevents a commit attempt for that batch, including its successfully saved records. The runner stops rather than polling and later committing higher offsets. Fx reports the failure and stops the server. Restart replays uncommitted records; SQLite's source-coordinate key prevents duplicate event rows. Successfully dead-lettered failures do not stop consumption.

SQLite writes and DLQ publications are not rolled back when another record fails. DLQ records may repeat after replay, including when a canceled publication reached Kafka but its acknowledgment was lost. Use the deterministic source-identity DLQ key for downstream deduplication. Commit failures can leave some partition offsets committed, and a timeout can leave the result uncertain. A whole-batch commit is one commit call, not an atomic transaction across Kafka partitions, SQLite, and the DLQ.

Provision DLQ topics and grant write permissions before enabling Kafka. The worker does not create them. DLQ messages use a version-1 JSON envelope with source topic, partition, offset, leader epoch, Kafka timestamp in milliseconds, error text, original key/value, and ordered headers. Byte fields use JSON base64 encoding so malformed JSON and arbitrary bytes remain recoverable. Error text is bounded; JSON decoding errors use safe classifications rather than payload-derived text. Original payload bytes are never truncated. The Kafka record key is `<source_topic>:<partition>:<offset>`. Restrict access because DLQ records contain original data. Envelope expansion can exceed producer or broker size limits; publication then fails without committing the source batch.

DLQ publication reuses the topic's authenticated franz-go client, waits for all in-sync replicas, and remains inside the topic's processing-concurrency slot and batch deadline. In-flight idempotent production can be canceled to avoid blocking shutdown on a lost reply. Internal transport retries retain producer deduplication, but application retries after cancellation can still duplicate DLQ records.

Each topic's `batch_timeout` bounds processing and DLQ publication, including waiting for its concurrency slots. Its `commit_timeout` bounds each synchronous commit. `kafka.max.poll.interval.ms` caps their combined elapsed time from a nonempty poll result to commit completion. Each deadline is cooperative; handlers must honor their context. Idle time spent polling an empty topic does not expire this processing budget. franz-go has no Java-style `MaxPollInterval` option, so the application enforces the interval and also supplies it as `kgo.RebalanceTimeout` while blocking rebalances during a batch. `session.timeout.ms` maps separately to `kgo.SessionTimeout`; the heartbeat interval is the smaller of 3 seconds and one-third of that session timeout. Configure broker limits to allow the selected timeouts.

Fx launches Kafka after SQLite and HTTP start. Startup means the runner has launched, not that a broker is reachable. Authentication, client setup, poll, unresolved DLQ, deadline, and commit failures request Fx shutdown with a nonzero exit code, stopping the HTTP server too.

On SIGINT or SIGTERM, Fx cancels Kafka, drains HTTP, and joins Kafka before closing SQLite and flushing logs. Kafka stops polling, cancels handler contexts, leaves incomplete batches uncommitted, waits for cooperative handlers, and then closes clients concurrently while allowing rebalances. Each client waits up to its topic's `commit_timeout` to leave its group before forcing local shutdown; unreachable brokers cannot multiply that wait by the client count. A forced leave can keep partitions assigned until Kafka's session timeout expires. Handlers must honor their context and should not start detached work that outlives it. A handler that ignores cancellation can prevent clean shutdown. Joining Kafka and releasing resources can exceed Fx's stop deadline; the deadline is not a hard process-exit limit.

## Metrics

Scrape `GET /metrics` on the existing HTTP listener, for example `http://localhost:8080/metrics`. Each Fx application has an isolated Prometheus registry with exactly two metric families. No Go, process, or HTTP metric collectors are registered.

| Metric | Type | Meaning |
| --- | --- | --- |
| `kafka_consumer_messages_processed_total` | Counter | Completed handler invocations by `topic` and `status`. |
| `kafka_consumer_message_processing_duration_seconds` | Histogram | Elapsed handler time in seconds by `topic` and `status`. |

`status="success"` means the handler returned `nil`; `status="failed"` means it returned an error, including a cancellation error. Timing starts when the handler is invoked and ends when it returns. It excludes time waiting for a concurrency slot, publishing to the DLQ, and committing offsets. A failed handler remains `failed` even if the DLQ successfully resolves its delivery. Messages that never reach the handler do not create observations. A panic is not recovered or counted as a returned failure.

These metrics describe processing attempts, not committed offsets. Replayed messages create new observations, and a completed attempt can be counted even if its batch is later left uncommitted. Each configured topic starts with zero-valued `success` and `failed` series. Kafka may remain disabled while `/metrics` is available. Counters reset when the process restarts. No message ID, offset, partition, or error-text labels are used.

A histogram records each message's duration without creating a separate time series for each message. Prometheus exposes its `_bucket`, `_sum`, and `_count` series. These are parts of the second metric family, not additional application metric families. For average handler duration by topic and status, query:

```promql
rate(kafka_consumer_message_processing_duration_seconds_sum[5m])
/
rate(kafka_consumer_message_processing_duration_seconds_count[5m])
```

## Tests

```sh
go test ./...
make test-integration
make check
```

`make test-integration` and `make check` run the real Kafka integration suite through the rootless Podman API socket selected by `scripts/test-integration-podman.sh`; no Docker daemon fallback or test skip is used. The Kafka integration brokers use `docker.io/confluentinc/cp-kafka:8.0.0`. Tests cover HTTP and enabled Kafka in the same server, live `/metrics` scrapes, successful and failed processing observations, lifecycle failure propagation and cleanup, batch bounds, shared per-topic concurrency and independent topic limits, per-topic deadlines, consumer-group partition distribution, validated SQLite event persistence, failed DLQ publication preventing the whole batch commit, whole-batch replay without duplicate SQLite rows, acknowledged DLQ envelopes, in-flight publication cancellation, successful-offset restart, latest-offset reset, LZ4 production/decompression, cancellation, coordinator-outage shutdown, and real SASL/PLAIN authentication with both valid and invalid passwords. The suite terminates only the containers it creates. The integration coverage remains part of the repository's 90% overall coverage gate.
