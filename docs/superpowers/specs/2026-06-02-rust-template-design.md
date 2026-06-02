# Rust Template Service — Design Spec

**Date:** 2026-06-02  
**Status:** Approved  
**Context:** This document describes the design for a Rust rewrite of the Python FastAPI template
service in this repository. It is intended as both a technical spec and a learning reference for
developers moving from Python to Rust.

---

## 1. Motivation

The Python template demonstrates best practices for a REST-first web service: clear module
boundaries, separation of concerns, dependency injection for testability, and MCP endpoint support.
The Rust version preserves all of those patterns while targeting use cases where Python is not
suitable — specifically CPU-bound data processing with heavy DataFrame querying, filtering, and
aggregation using Polars.

The Rust service is a drop-in replacement at the API level: same JSON shapes, same MCP tool names,
same Apache Arrow IPC stream file format. Path structure is preserved where practical.

---

## 2. Constraints

| Constraint | Detail |
|---|---|
| API contract | Same JSON response shapes as the Python service |
| MCP tool names | `get_health_service_tool`, `get_data_service_tool` |
| Data format | Apache Arrow IPC Stream (`.ipc_stream`) — same files, same format |
| Deployment | Kubernetes container; all configuration via environment variables |
| Build targets | Windows (development) and Linux (container) |
| Accessibility | Code should be readable to developers unfamiliar with Rust |
| Crate preference | Standard library and well-established crates over obscure alternatives |

---

## 3. Technology Stack

| Concern | Crate(s) | Replaces (Python) | Decision rationale |
|---|---|---|---|
| HTTP framework | `axum`, `tokio` | `fastapi`, `uvicorn` | Tower-native; in-process test client is first-class |
| MCP server | `rmcp` | `mcp.server.fastmcp.FastMCP` | Official Rust MCP SDK; `#[tool]` macro mirrors `@mcp.tool()` |
| Configuration | `serde`, `envy` | `pydantic_settings.BaseSettings` | Env-var-native; aligns directly with K8s ConfigMap/Secret injection |
| Dependency wiring | `AppState` + `Arc<dyn Trait>` | Custom `Container` + `dependency_overrides` | Type system replaces runtime registry; no framework needed |
| Data layer | `polars` | `polars` (Python binding) | Same crate; identical IPC file compatibility; supports heavy DataFrame ops |
| In-process test client | `tower::ServiceExt::oneshot` | `httpx.AsyncClient(ASGITransport)` | Calls the real Router with no TCP; same wiring as production |
| OpenAPI / Swagger UI | `utoipa`, `utoipa-swagger-ui` | FastAPI auto-generated | Proc-macro annotations on handlers and schemas; Swagger UI served at `/docs` |
| Logging | `tracing`, `tracing-subscriber` | `logging` | Structured by default; async-aware |
| JSON | `serde_json`, `serde` | Pydantic / `json` | `#[derive(Serialize, Deserialize)]` replaces Pydantic model definitions |

### Fallback: MCP hand-rolled handler

If `rmcp`'s streamable-HTTP transport diverges from the Python server's session behaviour (e.g.
`mcp-session-id` header handling), the MCP layer can be replaced with a plain Axum handler
implementing JSON-RPC 2.0 over HTTP POST. The REST and service layers are unaffected by this swap.
The Python integration tests treat MCP as raw HTTP, so they would pass against either implementation.

---

## 4. Folder Structure

```
rust-template/
├── Cargo.toml
├── src/
│   ├── main.rs              # Entry point: load settings, build AppState, start server
│   ├── lib.rs               # Re-exports build_app + public types for integration tests
│   ├── app.rs               # build_app(state: AppState) -> Router
│   ├── config.rs            # Settings struct (serde + envy)
│   ├── openapi.rs           # ApiDoc struct — collects all paths and schemas for OpenAPI
│   ├── state.rs             # AppState struct — the single wiring point
│   ├── routers/
│   │   ├── mod.rs
│   │   ├── health.rs        # GET /health/status
│   │   └── data.rs          # GET /data/shape
│   ├── mcp/
│   │   ├── mod.rs
│   │   └── tools.rs         # #[tool] implementations
│   ├── services/
│   │   ├── mod.rs
│   │   ├── health.rs        # HealthServiceTrait + HealthService
│   │   └── data.rs          # DataServiceTrait + DataService (Polars)
│   └── schemas/
│       ├── mod.rs
│       ├── health.rs        # HealthStatusResponse
│       └── data.rs          # DataShapeResponse
├── tests/
│   ├── common/
│   │   └── mod.rs           # test_state() helper — equivalent of conftest.py
│   ├── test_rest.rs         # REST endpoint integration tests
│   └── test_mcp.rs          # MCP protocol integration tests
└── data/
    └── data.ipc_stream      # Production data (Arrow IPC stream format)
```

**Python → Rust file mapping:**

| Python | Rust |
|---|---|
| `main.py` | `src/main.rs` + `src/app.rs` |
| `core/settings.py` | `src/config.rs` |
| FastAPI `openapi_tags` + auto-generation | `src/openapi.rs` |
| `core/container.py` + `core/dependencies.py` | `src/state.rs` |
| `routers/health.py`, `routers/data.py` | `src/routers/health.rs`, `src/routers/data.rs` |
| `mcp_routers/tools.py` | `src/mcp/tools.rs` |
| `services/health.py`, `services/data.py` | `src/services/health.rs`, `src/services/data.rs` |
| `schemas/health.py`, `schemas/data.py` | `src/schemas/health.rs`, `src/schemas/data.rs` |
| `tests/conftest.py` | `tests/common/mod.rs` |
| `tests/test_examples.py` | `tests/test_rest.rs` + `tests/test_mcp.rs` |

---

## 5. Module Design

### 5.1 `src/config.rs` — Settings

`Settings` is a plain Rust struct that derives `serde::Deserialize`. The `Default` impl defines
all default values. `envy::from_env()` overlays any environment variables that are present.

```rust
#[derive(serde::Deserialize, Clone)]
#[serde(default)]
pub struct Settings {
    pub app_title:   String,
    pub app_version: String,
    pub status:      String,
    pub data_dir:    String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            app_title:   "Rust Template Service".into(),
            app_version: "1.0.0".into(),
            status:      "running".into(),
            data_dir:    "./data".into(),
        }
    }
}
```

**Loading in production (`main.rs`):**
```rust
let settings = envy::from_env::<Settings>().unwrap_or_default();
```

**Loading in tests (`tests/common/mod.rs`):**
```rust
let test_settings = Settings {
    status:   "testing".into(),
    data_dir: "./tests/test_data".into(),
    ..Settings::default()  // all other fields take their defaults
};
```

No framework is involved in the test override — the struct is constructed directly.
This is equivalent to `Settings(status="testing", data_dir="./tests/test_data")` in Python.

**Python equivalent:** `pydantic_settings.BaseSettings`  
**K8s note:** In Kubernetes, `ConfigMap` and `Secret` values are injected as environment variables.
`envy` reads these at startup. No file mounting or secret volume needed.

---

### 5.2 `src/state.rs` — AppState (the wiring point)

`AppState` holds all application services as trait objects behind `Arc`. It is the only place in
the codebase that names concrete service types. Every other module (routers, MCP tools, tests)
depends only on the traits.

```rust
use std::sync::Arc;
use crate::services::{
    health::{HealthServiceTrait, HealthService},
    data::{DataServiceTrait, DataService},
};

#[derive(Clone)]
pub struct AppState {
    pub health_service: Arc<dyn HealthServiceTrait>,
    pub data_service:   Arc<dyn DataServiceTrait>,
}

impl AppState {
    pub fn new(settings: &Settings) -> Self {
        Self {
            health_service: Arc::new(HealthService::new(settings.clone())),
            data_service:   Arc::new(DataService::new(settings.clone())),
        }
    }
}
```

`Arc` (Atomic Reference Counted pointer) allows the state to be cloned cheaply and shared safely
across async tasks. `dyn ServiceTrait` is a trait object — the concrete type is erased, so callers
depend only on the interface.

**Dependency injection Goal 1 (test data injection):** Tests construct `AppState::new(&test_settings)`.
The `DataService` inside points to `./tests/test_data`. The router is built with this state.
No runtime override mechanism is needed.

**Dependency injection Goal 2 (single point of change):** When a service is replaced with a new
implementation, only `state.rs` (and the new `impl` file) change. Routers and MCP tools call trait
methods and are untouched.

**Python equivalent:** `core/container.py` + `core/dependencies.py` combined.

---

### 5.3 `src/services/` — Business Logic

Each service module defines a trait (the interface) and a struct (the implementation).

```rust
// src/services/data.rs

pub trait DataServiceTrait: Send + Sync {
    fn get_shape(&self) -> DataShapeResponse;
}

pub struct DataService {
    settings: Settings,
}

impl DataService {
    pub fn new(settings: Settings) -> Self { Self { settings } }
}

impl DataServiceTrait for DataService {
    fn get_shape(&self) -> DataShapeResponse {
        let path = format!("{}/data.ipc_stream", self.settings.data_dir);
        let file = std::fs::File::open(&path).expect("data file not found");
        let df = polars::io::ipc::IpcStreamReader::new(file)
            .finish()
            .expect("failed to read IPC stream");
        DataShapeResponse {
            height: df.height() as u64,
            width:  df.width()  as u64,
        }
    }
}
```

`Send + Sync` bounds on the trait are required because `Arc<dyn Trait>` must be safe to share
across async tasks on different threads. The compiler enforces this.

**Polars note:** `polars` in Rust is the same crate that the Python `polars` library wraps. The
`.ipc_stream` files written by Python Polars are directly readable by the Rust crate. For
production use with heavy querying, filtering, and aggregation, use the full Polars DataFrame API
(`LazyFrame`, `filter`, `groupby`, `join`, etc.) — the same operations available in Python.

**Python equivalent:** `services/data.py` (`DataService` class).

---

### 5.4 `src/schemas/` — Response Types

Schemas are plain Rust structs with `serde` derives. They replace Pydantic model classes.

```rust
// src/schemas/data.rs
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct DataShapeResponse {
    pub height: u64,
    pub width:  u64,
}
```

`#[derive(Serialize)]` generates the JSON serialisation code at compile time.
`axum::Json<T>` wraps any `Serialize` type and sets `Content-Type: application/json` automatically.
`#[derive(ToSchema)]` registers the struct with `utoipa` so it appears in the generated OpenAPI
document — the equivalent of Pydantic's automatic schema generation.

**Python equivalent:** `schemas/data.py` (Pydantic `BaseModel`).

---

### 5.5 `src/routers/` — HTTP Handlers

Handlers are plain async functions. They receive state via Axum's `State` extractor and return
`Json<T>`. Business logic stays in services; handlers only extract, call, and wrap.

```rust
// src/routers/data.rs
use axum::{extract::State, Json};
use crate::{state::AppState, schemas::data::DataShapeResponse};

#[utoipa::path(
    get,
    path = "/data/shape",
    tag = "Data Service",
    responses(
        (status = 200, description = "Row and column count of the dataset", body = DataShapeResponse)
    )
)]
pub async fn get_shape(State(state): State<AppState>) -> Json<DataShapeResponse> {
    Json(state.data_service.get_shape())
}
```

`State(state): State<AppState>` is Axum's extractor syntax. The `State(state)` part destructures
the wrapper, giving direct access to `AppState`. This replaces FastAPI's `data_service: DataServiceDep`.

`#[utoipa::path]` annotates the handler with its HTTP method, path, tag, and response schema.
`utoipa` reads these annotations at compile time to build the OpenAPI document — the equivalent of
FastAPI inferring this from type hints and docstrings automatically. The `tag` value maps to the
`open_api_tags` list in the Python `Settings` class.

**Python equivalent:** `routers/data.py`.

---

### 5.6 `src/app.rs` — Router Construction

`src/mcp/mod.rs` exposes a `build_router(state: AppState) -> Router` function that initialises
the `rmcp` server with `McpHandler { state }` and returns an Axum sub-router mounted at `/mcp`.
This keeps MCP wiring internal to the `mcp` module.



`build_app` is the single function that assembles the complete application. It is called by
`main.rs` in production and by tests directly — this is what makes in-process testing possible.

```rust
// src/app.rs
use axum::{routing::get, Router};
use utoipa_swagger_ui::SwaggerUi;
use crate::{routers::{health, data}, mcp, openapi::ApiDoc, state::AppState};

pub fn build_app(state: AppState) -> Router {
    let mcp_router = mcp::build_router(state.clone());
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .route("/health/status", get(health::get_status))
        .route("/data/shape",    get(data::get_shape))
        .nest("/mcp", mcp_router)
        .with_state(state)
}
```

Swagger UI is served at `/docs`. The raw OpenAPI JSON is available at `/api-doc/openapi.json`.
Both are served by the same in-process router, so they work in tests via `oneshot()` as well as
in production.

**Python equivalent:** The route registration block in `main.py`
(`app.include_router(...)`, `app.mount(...)`). FastAPI serves Swagger UI at `/docs` by default —
the path is intentionally preserved here.

---

### 5.7 `src/openapi.rs` — OpenAPI Document

`ApiDoc` is a single struct that aggregates all annotated paths and schemas into one OpenAPI
document. This is the Rust equivalent of FastAPI's automatic collection of routes and Pydantic
models — except it must be declared explicitly. The trade-off is verbosity for clarity: every
path and schema that appears in the generated doc is named here.

```rust
// src/openapi.rs
use utoipa::OpenApi;
use crate::{routers::{health, data}, schemas::{health::HealthStatusResponse, data::DataShapeResponse}};

#[derive(OpenApi)]
#[openapi(
    paths(
        health::get_status,
        data::get_shape,
    ),
    components(schemas(
        HealthStatusResponse,
        DataShapeResponse,
    )),
    tags(
        (name = "Health",       description = "Endpoints for checking service health and status"),
        (name = "Data Service", description = "Endpoints for querying the dataset"),
    ),
    info(
        title   = "Rust Template Service",
        version = "1.0.0",
        description = "A Rust service with REST and MCP endpoints, ready for Claude",
    )
)]
pub struct ApiDoc;
```

When a new endpoint is added, its handler function is added to `paths(...)` and any new response
type is added to `components(schemas(...))`. This is the single registration point for the OpenAPI
document — analogous to the `open_api_tags` list in the Python `Settings` class, but covering
paths and schemas as well.

**Python equivalent:** FastAPI handles this automatically from type annotations. `src/openapi.rs`
makes the same information explicit and visible.

---

### 5.9 `src/mcp/tools.rs` — MCP Tools

MCP tools are methods on a struct that holds `AppState`. The `#[tool]` proc macro from `rmcp`
generates the JSON-RPC dispatch boilerplate, equivalent to FastMCP's `@mcp.tool()` decorator.

```rust
use rmcp::tool;
use crate::state::AppState;

pub struct McpHandler {
    pub state: AppState,
}

#[tool(tool_box)]
impl McpHandler {
    #[tool(description = "Get health service status")]
    async fn get_health_service_tool(&self) -> String {
        self.state.health_service.get_status()
    }

    #[tool(description = "Get data shape")]
    async fn get_data_service_tool(&self) -> String {
        serde_json::to_string(&self.state.data_service.get_shape())
            .unwrap_or_default()
    }
}
```

Tool names (`get_health_service_tool`, `get_data_service_tool`) match the Python service exactly,
preserving the MCP API contract.

**Python equivalent:** `mcp_routers/tools.py`.

---

### 5.10 `src/main.rs` — Entry Point

`main.rs` is intentionally thin. Its only job is to wire the pieces together and start the server.

```rust
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = envy::from_env::<Settings>().unwrap_or_default();
    let state    = AppState::new(&settings);
    let app      = build_app(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
```

`#[tokio::main]` starts the Tokio async runtime. Everything below it is async. This is the Rust
equivalent of `uvicorn.run(app)`.

---

## 6. Testing

### 6.1 `tests/common/mod.rs` — Test Fixtures

This file is the Rust equivalent of `tests/conftest.py`. It provides a `test_state()` function
that constructs an `AppState` pointing at test data.

```rust
// tests/common/mod.rs
use rust_template::{config::Settings, state::AppState};

pub fn test_state() -> AppState {
    let settings = Settings {
        status:   "testing".into(),
        data_dir: "./tests/test_data".into(),
        ..Settings::default()
    };
    AppState::new(&settings)
}
```

### 6.2 `tests/test_rest.rs` — REST Tests

```rust
use tower::ServiceExt;       // provides .oneshot()
use axum::body::Body;
use http::{Request, StatusCode};
use http_body_util::BodyExt; // provides .collect()

mod common;

#[tokio::test]
async fn test_health_status() {
    let app = rust_template::build_app(common::test_state());

    let response = app
        .oneshot(Request::get("/health/status").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value, "testing");
}

#[tokio::test]
async fn test_get_shape() {
    let app = rust_template::build_app(common::test_state());

    let response = app
        .oneshot(Request::get("/data/shape").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let shape: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(shape["height"], 3);
    assert_eq!(shape["width"],  2);
}
```

`tower::ServiceExt::oneshot()` sends a single request to the router in-process. No TCP socket is
created. The router, all middleware, and all services execute exactly as in production.

**Python equivalent:** `async with AsyncClient(transport=ASGITransport(app=app)) as client:`

### 6.3 `tests/test_mcp.rs` — MCP Tests

MCP tests follow the same JSON-RPC 2.0 over HTTP pattern as the Python tests. The `initialize`
→ `tools/list` sequence and `mcp-session-id` header assertion are preserved.

```rust
#[tokio::test]
async fn test_mcp_tool_list() {
    let app = rust_template::build_app(common::test_state());

    // 1. Initialize session
    let init_body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"}
        }
    });
    let response = app.clone()
        .oneshot(Request::post("/mcp/")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(Body::from(init_body.to_string()))
            .unwrap())
        .await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let session_id = response.headers()
        .get("mcp-session-id")
        .unwrap()
        .to_str().unwrap()
        .to_string();

    // 2. List tools
    let list_body = serde_json::json!({
        "jsonrpc": "2.0", "method": "tools/list", "id": 2
    });
    let response = app
        .oneshot(Request::post("/mcp/")
            .header("content-type", "application/json")
            .header("mcp-session-id", &session_id)
            .body(Body::from(list_body.to_string()))
            .unwrap())
        .await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let tool_names: Vec<&str> = result["result"]["tools"]
        .as_array().unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    assert!(tool_names.contains(&"get_health_service_tool"));
    assert!(tool_names.contains(&"get_data_service_tool"));
}
```

---

## 7. Cargo.toml Dependencies

```toml
[package]
name    = "rust-template"
version = "0.1.0"
edition = "2021"

[dependencies]
axum              = "0.7"
tokio             = { version = "1", features = ["full"] }
tower             = "0.4"
serde             = { version = "1", features = ["derive"] }
serde_json        = "1"
envy              = "0.4"
polars            = { version = "0.44", features = ["ipc_streaming"] }
rmcp              = { version = "0.1", features = ["server", "transport-streamable-http-server"] }
utoipa            = { version = "4", features = ["axum_extras"] }
utoipa-swagger-ui = { version = "7", features = ["axum"] }
tracing           = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
tower             = { version = "0.4", features = ["util"] }
http-body-util    = "0.1"
```

**Notes:**
- `polars` features should be extended as data operations are added (e.g. `lazy`, `csv`, `parquet`).
- `rmcp` version and feature flags should be verified against the latest release at implementation time.
- `utoipa` version must match `utoipa-swagger-ui` major version (both on v4/v7 respectively above — verify at implementation time as these track together).
- `utoipa`'s `axum_extras` feature enables the `IntoResponses` derive and tighter Axum type integration.
- `tracing-subscriber`'s `env-filter` feature enables log level control via the `RUST_LOG` env var,
  which integrates naturally with K8s log configuration.

---

## 8. Python → Rust Concept Map

This section is a reference for developers familiar with the Python service learning the Rust equivalent.

| Concept | Python | Rust | Key difference |
|---|---|---|---|
| Config | `pydantic_settings.BaseSettings` | `#[derive(Deserialize)]` + `envy` | Defaults in `Default` impl; `envy` overlays env vars at startup once |
| HTTP framework | `fastapi.FastAPI()` | `axum::Router::new()` | Router is a value, not an object; no global app instance |
| Route registration | `app.include_router(router, prefix=...)` | `Router::nest("/prefix", sub_router)` | Composable; registered at build time |
| Route handler | `async def handler(dep: DepType)` | `async fn handler(State(s): State<AppState>)` | Extractor pattern; state destructured in signature |
| Dependency injection | `fastapi.Depends(get_service)` | `State<AppState>` extractor | Compile-time; no runtime resolver |
| DI override in tests | `app.dependency_overrides[fn] = lambda` | Construct `AppState` with test services | Type-safe; no framework mechanism |
| DI container | Custom `Container` class | `AppState` struct | Container is a Python workaround; `AppState` is idiomatic Rust |
| Single wiring point | `core/container.py` | `src/state.rs` | Concrete types named in one place only |
| Response serialisation | Pydantic model (auto) | `Json<T>` where `T: Serialize` | Explicit wrapper; `serde` generates serialiser at compile time |
| In-process test client | `httpx.AsyncClient(ASGITransport(app))` | `router.oneshot(Request)` | Tower `Service` trait; no transport layer whatsoever |
| MCP server | `FastMCP` + `@mcp.tool()` | `rmcp` + `#[tool]` | Same mental model; slightly more explicit Rust syntax |
| IPC stream reading | `polars.read_ipc_stream(path)` | `IpcStreamReader::new(file).finish()` | Same crate; byte-for-byte compatible files |
| Async runtime | `asyncio` (implicit) | `tokio` (explicit via `#[tokio::main]`) | Must be declared; same concurrency model |
| Logging | `logging.getLogger(__name__)` | `tracing::info!(...)` | Structured and async-aware; level set via `RUST_LOG` env var |
| Shared state across tasks | Not needed (GIL / thread-per-request) | `Arc<T>` | Reference-counted pointer; cheap clone, safe concurrent access |
| Interface / protocol | Duck typing / `Protocol` class | `trait` + `impl` | Explicit; compiler enforces implementation completeness |
| OpenAPI schema | Pydantic model (auto-registered) | `#[derive(ToSchema)]` | Opt-in per struct; same result |
| OpenAPI path | FastAPI infers from type hints | `#[utoipa::path(...)]` on handler | Explicit annotation; same expressiveness |
| OpenAPI document | FastAPI builds automatically | `#[derive(OpenApi)]` on `ApiDoc` | Single explicit registry in `openapi.rs` |
| Swagger UI | `/docs` (built-in) | `SwaggerUi::new("/docs")` in `build_app` | Same URL; served in-process |

### The one genuinely new concept: `Arc<dyn Trait>`

Python's `dependency_overrides` works at runtime because Python is dynamically typed. Rust
replaces this with `Arc<dyn ServiceTrait>`:

- `Arc` — a reference-counted smart pointer. Cloning it is cheap (increments a counter). When the
  last clone is dropped, the value is freed. Required because `AppState` is shared across many
  concurrent requests.
- `dyn ServiceTrait` — a trait object. The concrete type is hidden; only the trait's methods are
  visible. This is how routers stay decoupled from concrete service types.

Together, `Arc<dyn ServiceTrait>` is the Rust idiom that gives you the same test-injection
capability as `dependency_overrides`, with the concrete type enforced at compile time.

---

## 9. What the Python Template Has That This Design Omits

| Feature | Status | Notes |
|---|---|---|
| OpenAPI / Swagger UI | **Included** | `utoipa` + `utoipa-swagger-ui`; served at `/docs`. See sections 5.7 and 5.6. |
| MCP resources and prompts | Not included | `mcp_routers/resources.py` and `mcp_routers/prompts.py` have no content in the Python template. Include in Rust when Python versions are implemented. |
| `.env` file loading | Not included | `envy` reads env vars only. Add `dotenvy` crate for local development convenience; not needed in K8s. |

---

*This document was produced as part of a design brainstorm session on 2026-06-02.*
