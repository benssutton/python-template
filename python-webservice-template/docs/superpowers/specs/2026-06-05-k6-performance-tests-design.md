---
name: k6-performance-tests
description: Design for k6 performance tests with shared lib, multiple focused scripts, and GitLab CI pipeline integration
metadata:
  type: project
---

# k6 Performance Tests Design

## Goal

Add k6 performance tests to the python-template application that illustrate best practices across the range of k6 standard features, and integrate both pytest and k6 as quality gates in a GitLab CI build pipeline.

## Architecture

Three focused k6 scripts (smoke, load, stress) share a `performance/lib/` module for reusable check helpers and threshold presets. The full application stack (FastAPI + Postgres + ClickHouse) is started via Docker Compose inside each GitLab CI performance stage. Stages are sequenced so smoke gates load, and load gates stress (which is a soft gate).

**Tech Stack:** k6 (`grafana/k6` Docker image), Docker Compose, GitLab CI (`.gitlab-ci.yml`), ES module imports for shared lib.

---

## File Structure

```
performance/
  lib/
    checks.js           — reusable response-check helpers wrapping k6 check()
    thresholds.js       — named threshold presets: STRICT_SLO, NORMAL_SLO, RELAXED_SLO
  data/
    rows_params.json    — parameterized {limit, offset} pairs for load test SharedArray
  smoke.js              — 1–2 VUs, ~30 s, validates every endpoint is reachable
  load.js               — ramping-vus scenarios, data-driven requests, groups & tags
  stress.js             — ramping-arrival-rate executor, setup/teardown lifecycle
docker-compose.yml      — extended with app + clickhouse services (existing: db)
Dockerfile              — FastAPI app image (new)
.gitlab-ci.yml          — CI pipeline (new)
```

---

## Section 1: Shared Library

### `performance/lib/checks.js`

Exports named check bundles so scripts share identical assertions without copy-pasting:

```js
import { check } from 'k6';

export function checkStatus200(res) {
  return check(res, { 'status is 200': (r) => r.status === 200 });
}

export function checkDataCount(res) {
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'has count field': (r) => JSON.parse(r.body).count !== undefined,
  });
}

export function checkDataRows(res) {
  const body = JSON.parse(res.body);
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'has rows array': () => Array.isArray(body.rows),
    'has total field': () => body.total !== undefined,
  });
}
```

### `performance/lib/thresholds.js`

Named threshold presets spread into `options.thresholds`:

```js
export const STRICT_SLO = {
  http_req_failed: ['rate<0.01'],
  http_req_duration: ['p(95)<200'],
};

export const NORMAL_SLO = {
  http_req_failed: ['rate<0.01'],
  http_req_duration: ['p(95)<500'],
};

export const RELAXED_SLO = {
  http_req_failed: ['rate<0.05'],
  http_req_duration: ['p(95)<2000'],
};
```

---

## Section 2: Scripts

### `smoke.js` — "does it work?"

**Purpose:** Quick validation that all endpoints respond correctly under minimal load.

**k6 features demonstrated:** `options` with fixed VU/duration, `BASE_URL` env var, tags per request, checks via lib, thresholds via lib.

```
options:
  vus: 1
  duration: 30s
  thresholds: STRICT_SLO

default function:
  GET /               — checkStatus200, tagged { endpoint: 'root' }
  GET /health/status  — checkStatus200, tagged { endpoint: 'health' }
  GET /data/count     — checkDataCount, tagged { endpoint: 'data_count' }
  GET /data/rows      — checkDataRows,  tagged { endpoint: 'data_rows' }
  GET /config/settings — checkStatus200, tagged { endpoint: 'config' }
```

**CI role:** Hard gate. Pipeline stops here on failure.

---

### `load.js` — "does it hold up under normal traffic?"

**Purpose:** Sustained traffic at realistic load; exercises parameterized data-driven requests.

**k6 features demonstrated:** Named scenarios, `ramping-vus` executor, `constant-vus` executor, `SharedArray` for parameterized test data, `group()` for structured output, per-tag thresholds.

```
options:
  scenarios:
    browse_data:
      executor: ramping-vus
      stages: [{ duration: 30s, target: 10 }, { duration: 60s, target: 10 }, { duration: 30s, target: 0 }]
      tags: { scenario: 'browse_data' }
    health_poll:
      executor: constant-vus
      vus: 2
      duration: 2m
      tags: { scenario: 'health_poll' }
  thresholds:
    'http_req_duration{scenario:browse_data}': NORMAL_SLO values
    'http_req_duration{scenario:health_poll}': STRICT_SLO values

SharedArray 'rowParams' loads data/rows_params.json once across all VUs

default function:
  group('data', () => {
    pick random {limit, offset} from SharedArray
    GET /data/rows?limit={limit}&offset={offset}  — checkDataRows
    GET /data/count                                — checkDataCount
  })
  group('health', () => {
    GET /health/status  — checkStatus200
  })
```

**CI role:** Hard gate. Runs after smoke passes.

---

### `stress.js` — "where does it break?"

**Purpose:** Drive the app to and beyond its limits; identify breaking point.

**k6 features demonstrated:** `ramping-arrival-rate` executor, `setup()` / `teardown()` lifecycle with data passing, `TARGET_RPS` env var for CI-configurable load, `RELAXED_SLO` thresholds.

```
options:
  scenarios:
    spike:
      executor: ramping-arrival-rate
      startRate: 10
      timeUnit: 1s
      preAllocatedVUs: 20
      maxVUs: 100
      stages:
        - { duration: 30s, target: 10 }
        - { duration: 60s, target: TARGET_RPS (default 50) }
        - { duration: 30s, target: 10 }
  thresholds: RELAXED_SLO

setup():
  GET /health/status — assert 200; return { baseUrl }

default(data):
  uses data.baseUrl
  alternates between GET /data/count and GET /data/rows

teardown(data):
  logs summary line (demonstrates data flow setup → default → teardown)
```

**CI role:** Soft gate (`allow_failure: true`). Surfaces breaking point without blocking merge.

---

## Section 3: Docker Compose

`docker-compose.yml` extended with two new services:

```yaml
clickhouse:
  image: clickhouse/clickhouse-server:latest
  ports:
    - "8123:8123"
  healthcheck:
    test: ["CMD", "wget", "--spider", "-q", "http://localhost:8123/ping"]
    interval: 5s
    retries: 10

app:
  build: .
  ports:
    - "8000:8000"
  environment:
    DATABASE_URL: postgresql+asyncpg://user:password@db:5432/appdb
    CLICKHOUSE_HOST: clickhouse
    CLICKHOUSE_PORT: 8123
  depends_on:
    db:
      condition: service_healthy
    clickhouse:
      condition: service_healthy
```

A `Dockerfile` is added at the project root to build the FastAPI app image.

---

## Section 4: GitLab CI Pipeline

```yaml
stages:
  - test
  - performance-smoke
  - performance-load
  - performance-stress

pytest:
  stage: test
  image: python:3.12
  script:
    - pip install -r requirements.txt
    - pytest tests/ -v --cov

.k6-base:  # hidden job template
  image: docker:latest
  services:
    - docker:dind
  before_script:
    - docker compose up -d --wait
  after_script:
    - docker compose down -v

k6-smoke:
  extends: .k6-base
  stage: performance-smoke
  needs: [pytest]
  script:
    - docker run --network host -e BASE_URL=http://localhost:8000
        -v $PWD/performance:/scripts grafana/k6
        run /scripts/smoke.js

k6-load:
  extends: .k6-base
  stage: performance-load
  needs: [k6-smoke]
  script:
    - docker run --network host -e BASE_URL=http://localhost:8000
        -v $PWD/performance:/scripts grafana/k6
        run /scripts/load.js

k6-stress:
  extends: .k6-base
  stage: performance-stress
  needs: [k6-load]
  allow_failure: true
  script:
    - docker run --network host
        -e BASE_URL=http://localhost:8000
        -e TARGET_RPS=${TARGET_RPS:-50}
        -v $PWD/performance:/scripts grafana/k6
        run /scripts/stress.js
  variables:
    TARGET_RPS: "50"
```

---

## Testing & Verification

1. `docker compose up -d --wait` — stack starts cleanly with health checks
2. `docker run ... grafana/k6 run performance/smoke.js` — all checks pass, thresholds met
3. `docker run ... grafana/k6 run performance/load.js` — grouped output visible, SharedArray loads
4. `docker run ... grafana/k6 run performance/stress.js` — setup/teardown logged, stress metrics visible
5. `pytest tests/ -v` — existing test suite unaffected
6. Push to GitLab — pipeline runs all four stages in sequence with correct gating
