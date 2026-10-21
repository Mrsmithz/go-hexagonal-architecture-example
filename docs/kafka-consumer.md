# Kafka consumer

The Fx server runs HTTP and an optional Kafka worker in the same process. Set `kafka.enabled: true` in `config.yaml`, then run:

```sh
go run ./cmd/server
```

Kafka is disabled by default. The server loads `config.yaml` when present. Set `CONFIG_FILE` to require another file. A missing default file leaves HTTP running without Kafka; a missing explicit file fails startup. Disabled Kafka does not initialize authentication, create clients, or contact brokers. The sample handler logs only topic, partition, and offset. It does not mutate greetings or call the HTTP application's storage.

## Configuration

`config.yaml` in the repository is a local plaintext example for Kafka at `127.0.0.1:29092`. It explicitly sets `kafka.allow_insecure: true` because TLS is disabled. For normal deployments, leave TLS enabled and keep `allow_insecure: false`.

| Key | Meaning |
| --- | --- |
| `kafka.enabled` | Enable the Kafka worker inside the Fx server. Defaults to `false`. |
| `kafka.brokers` | One or more `host:port` broker addresses. |
| `kafka.group_id` | Base group ID. The worker derives `<group_id>.<topic>` so configured topics do not share offsets. |
| `kafka.auth` | `none`, `aws-iam`, `plain`, `scram-sha-256`, or `scram-sha-512`. |
| `kafka.username`, `kafka.password` | Credentials for PLAIN or SCRAM. Prefer `KAFKA_USERNAME` and `KAFKA_PASSWORD` environment overrides. |
| `kafka.tls.enabled` | Enable TLS. TLS is enabled by the built-in defaults. |
| `kafka.tls.ca_file` | Optional PEM CA bundle, added to system roots. |
| `kafka.tls.server_name` | Optional TLS server name override. |
| `kafka.allow_insecure` | Explicit opt-in for local plaintext testing only. IAM always requires TLS. |
| `topics[].name` | Kafka topic to consume. Duplicate or invalid Kafka topic names are rejected. |
| `topics[].consumers` | Number of consumer-group clients for this topic. |
| `batch_size` | Maximum records returned by one poll for each client. |
| `processing_concurrency` | Shared maximum number of concurrently executing handlers across all topics and clients. |
| `batch_timeout` | Deadline for completing a polled batch, including time waiting for the shared concurrency limit. |
| `commit_timeout` | Deadline for each synchronous offset commit and for leaving the group during shutdown. |

Each configured client is a member of its topic's derived group. More clients than the topic has partitions remain idle. `batch_size` bounds records held for processing by each client; handler concurrency is separately bounded process-wide by `processing_concurrency`.

A secure username/password configuration uses TLS and environment variables for credentials:

```yaml
kafka:
  enabled: true
  brokers: [broker.example.com:9094]
  group_id: greetings-worker
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
    consumers: 2
batch_size: 100
processing_concurrency: 8
batch_timeout: 30s
commit_timeout: 10s
```

```sh
KAFKA_USERNAME='worker' KAFKA_PASSWORD='...' CONFIG_FILE=secure-kafka.yaml go run ./cmd/server
```

Set `auth: plain` to use SASL/PLAIN instead. Both username/password modes require both credentials; do not store production secrets in the YAML file.

For Amazon MSK IAM, set `auth: aws-iam` and keep TLS enabled. The worker uses the AWS SDK v2 default credential chain (environment, shared profiles, container or instance role, and other supported sources); the cached provider is retrieved for each SASL session so expiring credentials can refresh. The franz-go IAM signer derives the region from the broker hostname, with `AWS_REGION` / `AWS_DEFAULT_REGION` as fallback. IAM requires TLS and rejects username/password fields. The repository tests the signing callback with a rotating credential provider; it does not claim a real AWS/MSK handshake test.

Unknown YAML keys and malformed values fail even when Kafka is disabled. Operational validation of bounds, topics, brokers, auth modes, and credentials applies only when Kafka is enabled. `CONFIG_FILE`, `KAFKA_USERNAME`, and `KAFKA_PASSWORD` are the supported environment overrides.

## Delivery and handler requirements

The worker disables automatic commits and processes each bounded poll batch concurrently without preserving record order. It commits the batch synchronously only after every handler succeeds. A poll, handler, or commit error stops the whole runner. No commit is attempted unless every handler in the batch succeeds. A commit failure can leave some partition offsets committed, and a timeout can leave the result uncertain. Uncommitted records can replay. Cross-partition batch commits are not atomic. The delivery guarantee is at-least-once: records whose handlers succeeded in a batch that later fails can be replayed. Handler side effects should therefore be idempotent.

The batch timeout bounds handler processing, and the commit timeout bounds synchronous commits. Since the worker blocks group rebalances while a batch is in flight, its Kafka `rebalance.timeout.ms` is set to `batch_timeout + commit_timeout + 10s`; configure broker limits to allow that value. franz-go has no `MaxPollInterval` option. The combined timeout plus margin must fit Kafka's signed 32-bit millisecond rebalance-timeout limit.

Fx launches Kafka after SQLite and HTTP start. Startup means the runner has launched, not that a broker is reachable. Authentication, client setup, poll, handler, and commit failures request Fx shutdown with a nonzero exit code, stopping the HTTP server too.

On SIGINT or SIGTERM, Fx cancels Kafka, drains HTTP, and joins Kafka before closing SQLite and flushing logs. Kafka stops polling, cancels handler contexts, leaves incomplete batches uncommitted, waits for cooperative handlers, and then closes clients concurrently while allowing rebalances. Each client waits up to `commit_timeout` to leave its group before forcing local shutdown; unreachable brokers cannot multiply that wait by the client count. A forced leave can keep partitions assigned until Kafka's session timeout expires. Handlers must honor their context and should not start detached work that outlives it. A handler that ignores cancellation can prevent clean shutdown. Joining Kafka and releasing resources can exceed Fx's stop deadline; the deadline is not a hard process-exit limit.

## Tests

```sh
go test ./...
make test-integration
make check
```

`make test-integration` and `make check` run the real Kafka integration suite through the rootless Podman API socket selected by `scripts/test-integration-podman.sh`; no Docker daemon fallback or test skip is used. The Kafka integration brokers use `docker.io/confluentinc/cp-kafka:8.0.0`. Tests cover HTTP and enabled Kafka in the same server, lifecycle failure propagation and cleanup, batch bounds, shared concurrency, consumer-group partition distribution, failed-batch replay, successful-offset restart, cancellation, coordinator-outage shutdown, and real SASL/PLAIN authentication with both valid and invalid passwords. The suite terminates only the containers it creates. The integration coverage remains part of the repository's 90% overall coverage gate.
