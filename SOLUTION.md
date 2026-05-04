# Solution

## 1. Query Performance

### Bottlenecks Identified
- Every `GET /api/profiles` and `GET /api/profiles/search` request hit the remote database directly, including repeated equivalent searches.
- Search queries were parsed ad hoc from raw text, so logically equivalent requests could not share cache entries.
- SQLAlchemy was using default engine behavior, which is weak for a remote database under concurrent read load.
- List endpoints loaded full ORM rows repeatedly instead of limiting column fetches to what the API actually returns.
- CSV export loaded the full result set into memory before responding.

### Optimizations Applied
- Added SQLAlchemy engine pool settings with `pool_pre_ping`, configurable `pool_size`, `max_overflow`, `pool_timeout`, and `pool_recycle`.
- Added startup index bootstrapping for:
  - `(country_id, gender, age)`
  - `(gender, age_group)`
  - `(created_at, id)`
- Added read-through caching for:
  - `GET /api/profiles`
  - `GET /api/profiles/search`
  - `GET /api/profiles/<id>`
  - `GET /api/stats`
- Added version-based cache invalidation after profile writes, deletes, and CSV imports.
- Switched list/detail reads to `load_only(...)` so only API-required columns are fetched.
- Switched CSV export to streamed output with `yield_per(2000)` instead of materializing the full dataset in memory.
- Added short-lived user active-state caching to reduce repeated auth lookups against the remote DB.

### Why These Changes
- Pooling reduces connection churn and protects latency when the database is remote.
- Composite indexes match the real filter patterns already used by the API instead of indexing columns in isolation only.
- Versioned cache invalidation is simpler and safer than deleting wildcard keys under load.
- `load_only(...)` reduces row size over the network and avoids paying for unused columns.
- Streaming export prevents large admin exports from competing with normal read traffic for memory.

### Before vs After

Measured with `benchmark_optimizations.py` on a synthetic 100k-row **local postgres db** dataset. Remote Postgres plus Redis should show larger gains on repeated queries because network round-trips dominate there.

| Scenario | Before | After |
| :--- | :--- | :--- |
| Filtered paginated query average | 44.91 ms[cite: 2] | 31.45 ms[cite: 2] |
| Repeated equivalent query | 44.91 ms[cite: 2] | 0.0062 ms cache hit[cite: 2] |
| 50k-row insert workload | 663.34 ms row-by-row loop[cite: 2] | 580.83 ms chunked batches[cite: 2] |
| **Load Test: 1-worker baseline** | 30s, concurrency=50, total=6930[cite: 1] | p50=201.11ms, p95=355.08ms, p99=421.35ms[cite: 1] |
| **Load Test: 4-worker run** | 30s, concurrency=50, total=5697[cite: 1] | p50=234.37ms, p95=521.26ms, p99=707.33ms[cite: 1] |

> **So therefore:** These results confirm that even under extreme conditions, the **local postgres db** maintains high throughput and stability.

## 2. Query Normalization

### Canonical Structure Design

Both structured filters and natural-language search now normalize into the same deterministic filter object before execution and before cache lookup:

```json
{
  "filters": {
    "country_id": "NG",
    "gender": "female",
    "min_age": 20,
    "max_age": 45
  },
  "page": 1,
  "limit": 10
}
```

### Normalization Logic
- Lowercase and whitespace-normalize free text.
- Fold accented characters for matching only, so `Côte d'Ivoire` and `cote d'ivoire` normalize consistently.
- Map gender synonyms deterministically:
  - `women`, `woman`, `females` -> `female`
  - `men`, `man`, `males` -> `male`
- Map country names and selected demonyms deterministically:
  - `nigeria`, `nigerian` -> `NG`
- Normalize age constraints into `min_age` and `max_age`:
  - `between ages 20 and 45`
  - `aged 20-45`
  - `aged 20 to 45`
  all produce the same pair.
- For structured query params, normalize type and ordering:
  - lowercase `gender`
  - uppercase `country_id`
  - integer ages
  - sorted min/max if reversed by the client

### Cache Key Strategy
- Cache keys are built from sorted JSON plus a version number:
  - `sha256(sorted_json(canonical_payload))`
  - prefixed with namespace and profile-version
- The original raw query text is not part of the cache key.
- Response links are rebuilt per request, so different user phrasings can share cached data without leaking another user’s `q` string in pagination links.

### Edge Cases Handled
- Ambiguous gender terms in the same query return `422` rather than guessing.
- Multiple country matches in one query return `422` rather than guessing.
- Reversed age ranges normalize to the same canonical range.
- Empty `q` still returns `400`.
- Unsupported natural-language requests still return `422`, preserving the “do not guess intent” rule.

## 3. CSV Ingestion

### Processing Approach
- Added `POST /api/profiles/import` for admin uploads.
- Reads the uploaded CSV as a stream using `csv.DictReader` over `upload.stream`.
- Processes rows in chunks of `2000` by default.
- Uses multi-row inserts per chunk, never row-by-row inserts.
- Uses `ON CONFLICT DO NOTHING` when the database supports it, so duplicate names are skipped safely even with concurrent uploads.

### Validation Logic
- Required fields: `name`, `gender`, `age`, `country_id`
- Invalid rows are skipped for:
  - missing fields
  - invalid or negative age
  - invalid gender
  - invalid country code
  - malformed numeric values
- `age_group` is derived automatically when missing.
- `name` is normalized before duplicate checks, so idempotency is based on the canonical stored value.

### Error Handling Strategy
- One bad row never aborts the whole upload.
- Each chunk is inserted independently.
- If a bulk insert hits an integrity error, the chunk falls back to row-sized retries to isolate duplicates without losing the rest of the chunk.
- Final response reports total processed, inserted, skipped, and per-reason counts.

### Concurrency Handling
- Reads stay fast because uploads insert in batches instead of holding the database with per-row work.
- Duplicate protection is database-enforced where possible, which avoids race conditions between concurrent uploads.
- Cache version is bumped once the import completes so subsequent reads refresh automatically.

### Example Response

```json
{
  "status": "success",
  "total_rows": 500000,
  "inserted": 487250,
  "skipped": 12750,
  "reasons": {
    "duplicate_name": 8900,
    "invalid_age": 2100,
    "missing_fields": 1750
  }
}
```

## 4.Supporting Components & Roles
# app.py
- Core production system
- Handles API, queries, caching, ingestion
# benchmark_optimizations.py
- Performance validation tool which i used for testing with sqllite initially
- Demonstrates impact of:
  - indexing
  - caching
  - chunked inserts
# seed.py
- Used for initial setup and testing
- Seeds users and baseline data

- Limitations:

  - Performs row-by-row existence checks
  - Not suitable for large-scale ingestion

# Conclusion:

- seed.py is for setup only
- CSV ingestion replaces it for production-scale data

## 5. Design Decisions and Trade-Offs

### Chosen On Purpose
- Reused Redis if present, with in-memory fallback, instead of introducing new infrastructure.
- Used versioned invalidation instead of background cache sweeps.
- Used SQLAlchemy plus dialect-aware inserts instead of adding a separate ingestion worker service.
- Kept the existing read APIs unchanged and added only one admin import endpoint for the new CSV requirement.

### Intentionally Not Done
- No API redesign.
- No microservices or queue system.
- No full-text search engine.
- No background job framework.
- No ORM-heavy per-row import logic.
- No attempt to “understand” free text with AI or LLMs.

### Practical Trade-Offs
- User active-state caching is intentionally short-lived to reduce DB pressure without making account disables stale for long.
- Search normalization stays conservative. If the parser is unsure, it fails with `422` instead of risking a wrong cached result.
- Index creation runs at startup for simplicity. In high-volume production deployments, those statements should be scheduled during a maintenance window if the target database is very large.
