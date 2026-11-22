# ADR-016 Feature serving service

- Status: Accepted. Decides the contract of the feature serving service: protocol, request, per-feature status, timeouts, fallback and versions. Nothing is implemented yet.
- Date: 2026-11-20
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-003, ADR-009, ADR-010, ADR-013

## Context

- ADR-003 requires that stale and missing features are visible and never replaced by a silent default.
- `OnlineFeatureClient` (`online.py`) reports `availability` (`present`, `missing`, `error`) and `freshness` (`fresh`, `stale`, `unknown`, `future`) per feature. Reader exceptions become error records and only `error_type` reaches the consumer. The response is documented as not atomic. The client is a library with fixed feature groups, Feast for batch features, and one entity per call.
- `RedisSpecStore.read(entity_value)` reads one key, returns a validated record or `None`, and lets connection errors propagate. The key prefix already contains the spec version: `featureforge-stream:online:v{version}:{name}`.
- A spec carries `online.ttl` and `freshness_slo`.
- Nothing measures the latency of an online read. The README cannot claim low latency.

Goal: one network contract that returns a feature vector for several entities with an explicit status per value, a bounded time per request, and a measured latency in a later change.

## Decision

| Concern | Rule |
|---|---|
| Protocol | HTTP and JSON with FastAPI. gRPC only if a measurement shows that serialization matters. |
| Endpoints | `POST /v1/features/lookup`, `GET /healthz` (process is alive), `GET /readyz` (specs loaded and the store answers a ping). |
| Scope | Features defined by specs. The Feast batch features stay behind `OnlineFeatureClient` until they are specs. The first backend is Redis. Cassandra follows through the protocol of ADR-013. |
| Request | `features`: a list of `{name, version?}`. `entities`: a list of key maps, for example `{"user_id": "u1"}`. At most 100 entities and 50 features per request, otherwise 422. Optional `reference_time` with an explicit time zone and optional `budget_ms`. |
| Response | One entry for every requested pair of entity and feature. An entry is never dropped. Each entry has `status`, `values`, `observation_time`, `age_seconds`, `freshness`, `version`, `published_at`, `defaulted` and `error_type`. `values` maps the counter and derived names of the spec to numbers, because one record holds all of them. The response states `atomic: false`. |
| Status | `ok`: present and fresh. `missing`: no key, an unknown entity, or an expired key. `stale`: present, and the age is above `freshness_slo` or the observation time is in the future. `backend_error`: a timeout, a connection error or a record that fails validation. The `freshness` field keeps the exact reason of `stale`. |
| Stale values | A `stale` entry keeps its value, so the caller sees the value and its age and decides. |
| Freshness | Evaluated at request time as `reference_time - observation_time` against `freshness_slo` of the spec. |
| Reads | One `MGET` per request over the keys of all requested features. One round trip, not one per key or feature. This assumes a single Redis node. A cluster needs one call per hash slot. |
| Readers | The service reads through a `ServingReader`. `fetch(plan)` takes the pairs of spec and entity value of a request and returns one `RecordRead` per pair, in the same order. `ping()` backs `readyz`. A reader raises when its backend fails and never returns a partial result. The service turns the exception into `backend_error` for every entry of the request and applies the fallback policy per feature, as before. A reader that returns a different number of items than it was asked for is a `ProtocolError`. The reader is chosen by `online.store`. One process serves one backend, and specs that name different backends fail at startup. |
| Redis reader | One `MGET` over the keys of all pairs, as in the row above. It is the reference reader and its behavior does not change. |
| Cassandra reader | One query per entity partition: `SELECT feature, payload FROM <keyspace>.online_features WHERE entity_key = ? AND entity_value = ? AND feature IN ?`, with the key prefixes of the requested features. All queries of a request run concurrently and the request waits for all of them. A failure of any query fails the request, because a partial result would depend on timing. Reads use `LOCAL_QUORUM`, a constructor argument as in ADR-013. The timeout of a query is the budget of the request. A request starts at most one query per entity, so at most 100. Whether that fan-out is acceptable is a measurement in `docs/benchmarks.md` and not an assumption of this ADR. |
| Isolation | A corrupt record is `backend_error` for its own entry. The other entries of the request stay valid. |
| Timeouts | A budget per request, default 50 ms. A request makes one round trip to one store, so the budget is the socket and connect timeout of the Redis client. A separate timeout per backend call matters once a request reads from several backends. The values are first targets and not SLOs. A timeout is `backend_error`, and `error_type` is the name of the exception class, for example `TimeoutError`. |
| Retries | None in the request path. A retry hides tail latency inside the budget. |
| Fallback | `online.fallback` in the spec has a `policy`: `omit` (default), `default_value` or `fail`. `default_value` needs a `defaults` map that names every counter and derived field of the spec. It applies to `missing` and `backend_error`. `omit` keeps the status and a null value. `default_value` returns the configured values, keeps the status and sets `defaulted: true`. `fail` answers the whole request with 503 when this feature is a `backend_error`. `missing` is a data state and never fails a request. |
| Versions | The request names a feature and optionally a version. Without one, the highest loaded version of that name is used. The response states the resolved version. The version is part of the key. An unknown name or version is 422, not a feature status. |
| Specs | Loaded from a directory at startup. No reload at runtime. |
| Errors | The response carries `error_type` only. The message goes to the log. |
| Out of scope | Authentication, TLS and rate limiting. FeatureForge stays a local reference implementation. |

## Consequences

- Positive: a model never receives a value without a status, and a failing store cannot hang a request.
- Positive: the `fail` policy lets a caller refuse a degraded vector for a feature that must not be guessed.
- Negative: JSON and a Python service add overhead per request. The size of that overhead is a measurement, not an assumption.
- Negative: the budget is enforced by socket timeouts. It is not a hard guarantee. Validation and serialization time come on top.
- Negative: stores are read at slightly different times. The response is not an atomic snapshot.
- Negative: mapping `future` to `stale` loses a distinction in the status value. The `freshness` field carries it.
- Negative: a new dependency on FastAPI and uvicorn, as an optional extra.
- Negative: `online.fallback` is a new spec field. Existing specs stay valid through the default.

## Alternatives considered

| Alternative | Why not |
|---|---|
| gRPC first | It adds a build step and a schema language before a measurement shows a need. |
| One HTTP call per feature | It multiplies round trips and makes the tail latency of a vector the maximum of many calls. |
| Wrap `OnlineFeatureClient` directly | It has fixed groups, one entity per call and Feast on the path. |
| Return `0` for a missing value | It is the silent default that ADR-003 forbids. |
| Retry inside the budget | It hides the tail latency that the measurement has to show. |
| Reload specs at runtime | It makes the served version depend on timing. A restart is simple and explicit. |

## Verification

Every item has a test.

- A request for several entities and features returns one entry per pair, with the status `ok` for fresh values.
- An unknown entity is `missing`. An expired key is `missing`.
- A value older than `freshness_slo` is `stale` and keeps its value and age.
- A corrupt record is `backend_error` for its entry, and the other entries are `ok`.
- With Redis stopped, every entry is `backend_error`. The three fallback policies behave as decided, and `fail` answers 503.
- With a slow Redis (latency added by a proxy), the request ends within the budget plus a margin and reports `TimeoutError`.
- A request makes one `MGET` for all keys of all features, counted by a spy client.
- The version is resolved as decided, and an unknown name, an unknown version and a request above the limits answer 422.
- `readyz` fails while the store is down.

The latency of the service is measured in a separate change and reported in `docs/benchmarks.md`. This ADR states no latency figure.

## Open items

- `FeatureRecord` carries no `published_at`. The field stays null until the record gains it.
- Synchronous or asynchronous Redis client: decided after profiling.
- The Cassandra reader is implemented (`CassandraServingReader`). Its measurement against Redis is open.
- gRPC and authentication.
