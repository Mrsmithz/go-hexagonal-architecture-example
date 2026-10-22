# Agent guide

- Use modern Go and run `gofmt` on changed Go files. Keep changes focused.
- `cmd/server` is the only executable. Compose dependencies and lifecycle with Uber Fx in `internal/app`.
- Keep HTTP greetings in `internal/greeting`. Kafka event validation and persistence orchestration belong in `internal/message`; polling, DLQ routing and batch acknowledgment belong in `internal/consumer`. Keep Kafka/GORM/Prometheus implementations in `internal/adapter`.
- Load Kafka settings with koanf from `configs/config.yaml` or explicit `CONFIG_FILE`. Kafka is opt-in. Keep concurrency and timeouts per topic; reject unknown YAML keys.
- Event JSON requires a positive integer `user_id`, nonblank trimmed `event`, and a present, non-null integer `timestamp`. Unix timestamp `0` is valid. Use go-playground/validator.
- SQLite event writes must be idempotent by source topic, partition and offset. Never deduplicate by user ID or timestamp. Do not change greeting behavior.
- Disable automatic offset commits. Commit the entire original polled batch only after every record is saved or its failure is acknowledged by the DLQ. Any DLQ error or incomplete/canceled batch forbids a commit attempt. DLQ records and processing attempts can repeat after replay.
- Preserve failed handler metrics even after successful DLQ routing. Keep exactly two application metric families, labeled only by topic and bounded status, on the private registry at `/metrics`.
- Honor cancellation. Join Kafka workers before closing SQLite or flushing Zap. Do not launch detached handler work.
- Do not log payloads, keys or credentials. DLQ payloads contain original data; restrict topic access. Keep authentication/TLS support intact.
- Verify behavior with tests, not mocks alone. Run `make check` for race-enabled Kafka 8.x Testcontainers integration through rootless Podman, vet and the 90% coverage gate. Do not substitute Docker or skip missing integration infrastructure.
- Run `go fix -diff ./...`, `go fix -tags=integration -diff ./...` and `go mod tidy -diff` before finishing. Report checks you could not run.
