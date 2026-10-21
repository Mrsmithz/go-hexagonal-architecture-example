# HTTP API

All success and error responses use JSON except `DELETE`, which returns an empty `204 No Content` response. Error objects have this form:

Greetings persist in SQLite. The default database is `greetings.db`; set `SQLITE_DSN` to choose another SQLite path. The server creates its schema at startup. Building and running requires CGO and an installed C compiler.

```json
{"error":{"code":"not_found","message":"greeting not found"}}
```

| Method | Path | Success |
| --- | --- | --- |
| `GET` | `/hello` | `200` with `{"message":"Hello, World!"}` |
| `POST` | `/api/v1/greetings` | `201`, greeting JSON, and a `Location` header |
| `GET` | `/api/v1/greetings` | `200` with an ascending array, including `[]` when empty |
| `GET` | `/api/v1/greetings/{id}` | `200` with one greeting |
| `PUT` | `/api/v1/greetings/{id}` | `200` with the updated greeting |
| `DELETE` | `/api/v1/greetings/{id}` | `204` |

Create and update bodies must use `Content-Type: application/json` and contain one JSON object with a non-null string `message`. Unknown fields are rejected. Field names match without regard to case. If a message field occurs more than once, the last value wins. Messages are trimmed, non-empty, valid UTF-8, and at most 1,000 Unicode code points. Request bodies may not exceed 16 KiB. IDs are positive decimal uint64 values.

Invalid JSON, fields, messages, or IDs return `400`. Missing greetings and routes return `404`. Unsupported methods return `405`, unsupported content types return `415`, and oversized bodies return `413`. Unexpected failures return a generic `500` error response.
