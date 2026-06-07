# k6 Performance Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three focused k6 performance scripts (smoke, load, stress) with a shared lib, a Docker Compose full-stack definition, and a GitLab CI pipeline that uses both pytest and k6 as quality gates.

**Architecture:** A `performance/` directory holds a shared ES-module lib (`lib/checks.js`, `lib/thresholds.js`), parameterised test data, three focused scripts, and a `Dockerfile` that packages the scripts into a `perf-scripts` image. `docker-compose.yml` is extended with ClickHouse, a healthchecked Postgres, and the FastAPI app itself. GitLab CI stages sequence pytest → smoke → load → stress, with smoke and load as hard gates and stress as a soft gate.

**Tech Stack:** k6 (`grafana/k6`), Docker Compose v2, GitLab CI (`.gitlab-ci.yml`), Python 3.12 / uvicorn / alembic, ClickHouse, Postgres.

**Spec:** `docs/superpowers/specs/2026-06-05-k6-performance-tests-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `performance/lib/checks.js` | Reusable k6 check helpers |
| Create | `performance/lib/thresholds.js` | Named SLO threshold presets |
| Create | `performance/data/rows_params.json` | Parameterised limit/offset pairs for load test |
| Create | `performance/data/clickhouse-init.sql` | Creates and seeds `items` table on container start |
| Create | `performance/Dockerfile` | Packages k6 scripts into a Docker image |
| Create | `performance/smoke.js` | 1 VU / 30 s smoke test |
| Create | `performance/load.js` | Ramping-VU load test with scenarios and SharedArray |
| Create | `performance/stress.js` | Ramping-arrival-rate stress test with setup/teardown |
| Create | `Dockerfile` | Builds FastAPI app image |
| Modify | `requirements.txt` | Add `uvicorn[standard]` |
| Modify | `docker-compose.yml` | Add db healthcheck, clickhouse service, app service |
| Create | `.gitlab-ci.yml` | Four-stage CI pipeline |

---

## Task 1: Shared Library

**Files:**
- Create: `performance/lib/checks.js`
- Create: `performance/lib/thresholds.js`

- [ ] **Step 1: Create `performance/lib/checks.js`**

```javascript
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

- [ ] **Step 2: Create `performance/lib/thresholds.js`**

```javascript
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

- [ ] **Step 3: Commit**

```bash
git add performance/lib/checks.js performance/lib/thresholds.js
git commit -m "feat(perf): add shared k6 checks and threshold presets"
```

---

## Task 2: Data and Init Files

**Files:**
- Create: `performance/data/rows_params.json`
- Create: `performance/data/clickhouse-init.sql`

- [ ] **Step 1: Create `performance/data/rows_params.json`**

```json
[
  { "limit": 1, "offset": 0 },
  { "limit": 2, "offset": 0 },
  { "limit": 3, "offset": 0 },
  { "limit": 1, "offset": 1 },
  { "limit": 2, "offset": 1 },
  { "limit": 3, "offset": 1 },
  { "limit": 10, "offset": 0 },
  { "limit": 10, "offset": 2 }
]
```

- [ ] **Step 2: Create `performance/data/clickhouse-init.sql`**

This file is mounted into the ClickHouse container at `/docker-entrypoint-initdb.d/init.sql` and runs on first startup.

```sql
CREATE TABLE IF NOT EXISTS default.items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO default.items VALUES (1, 'alpha', 'a'), (2, 'beta', 'b'), (3, 'gamma', 'c');
```

- [ ] **Step 3: Verify JSON is valid**

Run:
```bash
python -c "import json; data = json.load(open('performance/data/rows_params.json')); print(f'OK — {len(data)} param sets')"
```

Expected: `OK — 8 param sets`

- [ ] **Step 4: Commit**

```bash
git add performance/data/rows_params.json performance/data/clickhouse-init.sql
git commit -m "feat(perf): add k6 test data and ClickHouse init SQL"
```

---

## Task 3: FastAPI App Dockerfile

**Files:**
- Modify: `requirements.txt`
- Create: `Dockerfile`

- [ ] **Step 1: Add `uvicorn[standard]` to `requirements.txt`**

Open `requirements.txt`. After the `# Fast API Service` block, add `uvicorn[standard]`:

```
# Fast API Service
fastapi
uvicorn[standard]
pydantic
pydantic-settings
python-dotenv
httpx
```

- [ ] **Step 2: Create `Dockerfile`**

The CMD runs `alembic upgrade head` first (creates the `configuration` table in Postgres), then starts uvicorn. The `depends_on` in docker-compose ensures Postgres and ClickHouse are healthy before this container starts.

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["sh", "-c", "alembic upgrade head && uvicorn main:app --host 0.0.0.0 --port 8000"]
```

- [ ] **Step 3: Verify the image builds**

Run:
```bash
docker build -t python-template-app .
```

Expected: build completes with no errors. The final line should be `Successfully tagged python-template-app:latest` (or equivalent Buildkit output).

- [ ] **Step 4: Commit**

```bash
git add requirements.txt Dockerfile
git commit -m "feat(perf): add Dockerfile and uvicorn dependency for docker compose stack"
```

---

## Task 4: Extend Docker Compose

**Files:**
- Modify: `docker-compose.yml`

The current file has only the `db` service. Replace the entire file with the version below, which:
- Adds a `healthcheck` to `db` (required for `depends_on: condition: service_healthy`)
- Adds defaults to `db` env vars so the stack starts without a `.env` file
- Adds a `clickhouse` service with the init SQL mounted
- Adds the `app` service built from the local `Dockerfile`
- Names the compose project `python-template` so the default network is always `python-template_default`

- [ ] **Step 1: Replace `docker-compose.yml`**

```yaml
name: python-template

services:
  db:
    image: postgres:18
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-password}
      POSTGRES_DB: ${POSTGRES_DB:-appdb}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-user} -d ${POSTGRES_DB:-appdb}"]
      interval: 5s
      timeout: 5s
      retries: 10

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
    volumes:
      - ./performance/data/clickhouse-init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8123/ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql+asyncpg://${POSTGRES_USER:-user}:${POSTGRES_PASSWORD:-password}@db:5432/${POSTGRES_DB:-appdb}
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_PORT: "8123"
    depends_on:
      db:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:8000/health/status')\""]
      interval: 5s
      timeout: 10s
      retries: 12
      start_period: 15s

volumes:
  postgres_data:
```

- [ ] **Step 2: Start the stack and verify all services become healthy**

Run:
```bash
docker compose up -d --wait
```

Expected: all three services reach `healthy`. This may take 30–60 s on first run while ClickHouse initialises.

Confirm with:
```bash
docker compose ps
```

Expected output (all `Status` columns show `healthy`):
```
NAME                     STATUS
python-template-db-1         running (healthy)
python-template-clickhouse-1 running (healthy)
python-template-app-1        running (healthy)
```

- [ ] **Step 3: Verify the app responds**

Run:
```bash
docker run --rm --network python-template_default \
  curlimages/curl curl -s http://app:8000/health/status
```

Expected: `{"status":"running"}`

- [ ] **Step 4: Tear down**

```bash
docker compose down -v
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(perf): extend docker-compose with clickhouse and app services"
```

---

## Task 5: smoke.js and k6 Scripts Image

**Files:**
- Create: `performance/smoke.js`
- Create: `performance/Dockerfile`

- [ ] **Step 1: Create `performance/Dockerfile`**

This image is used both locally and in CI to run k6 scripts without volume-mount issues in docker:dind environments.

```dockerfile
FROM grafana/k6:latest
COPY . /scripts
WORKDIR /scripts
```

- [ ] **Step 2: Create `performance/smoke.js`**

Features demonstrated: fixed VU/duration, `BASE_URL` env var, per-request tags, shared check helpers, shared SLO thresholds.

```javascript
import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus200, checkDataCount, checkDataRows } from './lib/checks.js';
import { STRICT_SLO } from './lib/thresholds.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export const options = {
  vus: 1,
  duration: '30s',
  thresholds: { ...STRICT_SLO },
};

export default function () {
  checkStatus200(http.get(`${BASE_URL}/`,              { tags: { endpoint: 'root' } }));
  checkStatus200(http.get(`${BASE_URL}/health/status`, { tags: { endpoint: 'health' } }));
  checkDataCount(http.get(`${BASE_URL}/data/count`,    { tags: { endpoint: 'data_count' } }));
  checkDataRows( http.get(`${BASE_URL}/data/rows`,     { tags: { endpoint: 'data_rows' } }));
  checkStatus200(http.get(`${BASE_URL}/config/`,       { tags: { endpoint: 'config' } }));
  sleep(1);
}
```

- [ ] **Step 3: Start the stack**

```bash
docker compose up -d --wait
```

- [ ] **Step 4: Build the k6 scripts image**

```bash
docker build -t perf-scripts ./performance
```

Expected: build succeeds, image tagged `perf-scripts`.

- [ ] **Step 5: Run smoke.js and verify it passes**

```bash
docker run --rm \
  --network python-template_default \
  -e BASE_URL=http://app:8000 \
  perf-scripts run /scripts/smoke.js
```

Expected:
- All 5 check names listed as passing (`✓`)
- `http_req_failed` rate is `0.00%` (threshold met)
- `http_req_duration` p95 < 200 ms (threshold met)
- Exit code 0

- [ ] **Step 6: Tear down**

```bash
docker compose down -v
```

- [ ] **Step 7: Commit**

```bash
git add performance/Dockerfile performance/smoke.js
git commit -m "feat(perf): add smoke.js and k6 scripts Dockerfile"
```

---

## Task 6: load.js

**Files:**
- Create: `performance/load.js`

Features demonstrated: named scenarios with `exec`, `ramping-vus` executor, `constant-vus` executor, `SharedArray` for parameterised requests, `group()` for structured output, per-scenario threshold tags.

- [ ] **Step 1: Create `performance/load.js`**

```javascript
import http from 'k6/http';
import { group, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { checkStatus200, checkDataCount, checkDataRows } from './lib/checks.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

const rowParams = new SharedArray('rowParams', function () {
  return JSON.parse(open('./data/rows_params.json'));
});

export const options = {
  scenarios: {
    browse_data: {
      executor: 'ramping-vus',
      exec: 'browseData',
      stages: [
        { duration: '30s', target: 10 },
        { duration: '60s', target: 10 },
        { duration: '30s', target: 0 },
      ],
      tags: { scenario: 'browse_data' },
    },
    health_poll: {
      executor: 'constant-vus',
      exec: 'healthPoll',
      vus: 2,
      duration: '2m',
      tags: { scenario: 'health_poll' },
    },
  },
  thresholds: {
    'http_req_duration{scenario:browse_data}': ['p(95)<500'],
    'http_req_failed{scenario:browse_data}':   ['rate<0.01'],
    'http_req_duration{scenario:health_poll}': ['p(95)<200'],
    'http_req_failed{scenario:health_poll}':   ['rate<0.01'],
  },
};

export function browseData() {
  const p = rowParams[Math.floor(Math.random() * rowParams.length)];
  group('data', () => {
    checkDataRows(http.get(`${BASE_URL}/data/rows?limit=${p.limit}&offset=${p.offset}`));
    checkDataCount(http.get(`${BASE_URL}/data/count`));
  });
  sleep(1);
}

export function healthPoll() {
  group('health', () => {
    checkStatus200(http.get(`${BASE_URL}/health/status`));
  });
  sleep(1);
}
```

- [ ] **Step 2: Start the stack and rebuild the k6 image**

```bash
docker compose up -d --wait
docker build -t perf-scripts ./performance
```

- [ ] **Step 3: Run load.js and verify it passes**

```bash
docker run --rm \
  --network python-template_default \
  -e BASE_URL=http://app:8000 \
  perf-scripts run /scripts/load.js
```

Expected:
- Two scenario names (`browse_data`, `health_poll`) visible in output
- `group::data` and `group::health` sections in the summary
- All four per-scenario thresholds met
- Exit code 0

Note: this test runs for 2 minutes total.

- [ ] **Step 4: Tear down**

```bash
docker compose down -v
```

- [ ] **Step 5: Commit**

```bash
git add performance/load.js
git commit -m "feat(perf): add load.js with ramping-vus scenarios and SharedArray"
```

---

## Task 7: stress.js

**Files:**
- Create: `performance/stress.js`

Features demonstrated: `ramping-arrival-rate` executor, `setup()` / `teardown()` lifecycle with data passing, `TARGET_RPS` env var, `RELAXED_SLO` thresholds.

- [ ] **Step 1: Create `performance/stress.js`**

```javascript
import http from 'k6/http';
import { check } from 'k6';
import { checkDataCount, checkDataRows } from './lib/checks.js';
import { RELAXED_SLO } from './lib/thresholds.js';

const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '50', 10);

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '60s', target: TARGET_RPS },
        { duration: '30s', target: 10 },
      ],
    },
  },
  thresholds: { ...RELAXED_SLO },
};

export function setup() {
  const baseUrl = __ENV.BASE_URL || 'http://localhost:8000';
  const res = http.get(`${baseUrl}/health/status`);
  check(res, { 'app is healthy before stress': (r) => r.status === 200 });
  return { baseUrl };
}

export default function (data) {
  if (__ITER % 2 === 0) {
    checkDataCount(http.get(`${data.baseUrl}/data/count`));
  } else {
    checkDataRows(http.get(`${data.baseUrl}/data/rows`));
  }
}

export function teardown(data) {
  console.log(`Stress test complete. Target RPS: ${TARGET_RPS}. Base URL: ${data.baseUrl}`);
}
```

- [ ] **Step 2: Start the stack and rebuild the k6 image**

```bash
docker compose up -d --wait
docker build -t perf-scripts ./performance
```

- [ ] **Step 3: Run stress.js and verify setup/teardown appear in output**

```bash
docker run --rm \
  --network python-template_default \
  -e BASE_URL=http://app:8000 \
  -e TARGET_RPS=20 \
  perf-scripts run /scripts/stress.js
```

Note: `TARGET_RPS=20` keeps the run short and sensible locally. CI uses 50.

Expected:
- `app is healthy before stress` check passes in the init section
- `Stress test complete.` log line appears at teardown
- `RELAXED_SLO` thresholds may or may not pass (stress tests are informational)
- Exit code 0 or 99 — both are acceptable locally; CI marks this `allow_failure: true`

- [ ] **Step 4: Tear down**

```bash
docker compose down -v
```

- [ ] **Step 5: Commit**

```bash
git add performance/stress.js
git commit -m "feat(perf): add stress.js with ramping-arrival-rate and setup/teardown"
```

---

## Task 8: GitLab CI Pipeline

**Files:**
- Create: `.gitlab-ci.yml`

The pipeline has four stages. The three k6 jobs use `docker:latest` with `docker:dind` as a service. The `before_script` starts the compose stack; the script builds the `perf-scripts` image from the checked-out repo (Docker build sends the context from the client to dind, so no volume-mount issues) and runs k6 against the named compose network.

- [ ] **Step 1: Create `.gitlab-ci.yml`**

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

.k6-base:
  image: docker:latest
  services:
    - name: docker:dind
      alias: docker
  variables:
    DOCKER_HOST: tcp://docker:2375
    DOCKER_TLS_CERTDIR: ""
  before_script:
    - docker compose up -d --wait
    - docker build -t perf-scripts ./performance
  after_script:
    - docker compose down -v

k6-smoke:
  extends: .k6-base
  stage: performance-smoke
  needs: [pytest]
  script:
    - >
      docker run --rm
      --network python-template_default
      -e BASE_URL=http://app:8000
      perf-scripts run /scripts/smoke.js

k6-load:
  extends: .k6-base
  stage: performance-load
  needs: [k6-smoke]
  script:
    - >
      docker run --rm
      --network python-template_default
      -e BASE_URL=http://app:8000
      perf-scripts run /scripts/load.js

k6-stress:
  extends: .k6-base
  stage: performance-stress
  needs: [k6-load]
  allow_failure: true
  variables:
    TARGET_RPS: "50"
  script:
    - >
      docker run --rm
      --network python-template_default
      -e BASE_URL=http://app:8000
      -e TARGET_RPS=$TARGET_RPS
      perf-scripts run /scripts/stress.js
```

- [ ] **Step 2: Verify YAML syntax**

Run:
```bash
python -c "import yaml; yaml.safe_load(open('.gitlab-ci.yml')); print('YAML syntax OK')"
```

Expected: `YAML syntax OK`

If `yaml` is not installed: `pip install pyyaml` then retry.

- [ ] **Step 3: Commit**

```bash
git add .gitlab-ci.yml
git commit -m "feat(ci): add GitLab CI pipeline with pytest and k6 quality gates"
```

---

## Verification Checklist

After all tasks are complete, do a final end-to-end check:

- [ ] `pytest tests/ -v` — all existing tests pass (k6 work should not affect them)
- [ ] `docker compose up -d --wait && docker compose ps` — all three services show `healthy`
- [ ] `docker build -t perf-scripts ./performance` — image builds cleanly
- [ ] `docker run --rm --network python-template_default -e BASE_URL=http://app:8000 perf-scripts run /scripts/smoke.js` — exit 0, all checks pass
- [ ] `docker compose down -v` — stack stops and volumes are removed
