# Rust Template Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust web service that is a drop-in replacement for the Python FastAPI template, with identical JSON shapes, MCP tool names, and Arrow IPC data format, deployable as a Kubernetes container.

**Architecture:** Axum Router wired with `AppState` holding `Arc<dyn ServiceTrait>` singletons. `build_app(state)` assembles the full router and is called by both `main.rs` and integration tests — enabling in-process testing via `tower::ServiceExt::oneshot()` with no TCP server. OpenAPI served via `utoipa` + Swagger UI at `/docs`. MCP served via `rmcp` at `/mcp`.

**Tech Stack:** `axum 0.7`, `tokio 1`, `polars 0.44` (ipc_streaming), `rmcp 0.1`, `utoipa 4`, `utoipa-swagger-ui 7`, `envy 0.4`, `serde 1`, `tracing 0.1`

**Spec:** `docs/superpowers/specs/2026-06-02-rust-template-design.md`

---

## File Map

All paths are relative to `rust-template/` (created as a subdirectory of the repo root).

| File | Role |
|---|---|
| `Cargo.toml` | Crate manifest and all dependencies |
| `src/lib.rs` | Public module tree — re-exports `build_app` for integration tests |
| `src/main.rs` | Entry point: load settings → build state → serve |
| `src/config.rs` | `Settings` struct (`serde` + `envy`, defaults in `Default` impl) |
| `src/schemas/mod.rs` | Module declarations |
| `src/schemas/data.rs` | `DataShapeResponse` (`Serialize`, `Deserialize`, `ToSchema`) |
| `src/services/mod.rs` | Module declarations |
| `src/services/health.rs` | `HealthServiceTrait` + `HealthService` |
| `src/services/data.rs` | `DataServiceTrait` + `DataService` (Polars IPC) |
| `src/state.rs` | `AppState` — the single wiring point |
| `src/routers/mod.rs` | Module declarations |
| `src/routers/health.rs` | `GET /health/status` handler + `#[utoipa::path]` |
| `src/routers/data.rs` | `GET /data/shape` handler + `#[utoipa::path]` |
| `src/openapi.rs` | `ApiDoc` struct listing all paths and schemas |
| `src/app.rs` | `build_app(state: AppState) -> Router` |
| `src/mcp/mod.rs` | `build_router(state) -> Router` (rmcp Axum wiring) |
| `src/mcp/tools.rs` | `McpHandler` with `#[tool]` implementations |
| `tests/common/mod.rs` | `test_state()` — equivalent of `conftest.py` |
| `tests/test_rest.rs` | REST endpoint integration tests |
| `tests/test_mcp.rs` | MCP JSON-RPC integration tests |
| `tests/test_data/data.ipc_stream` | Arrow IPC stream (copied from Python project) |
| `data/data.ipc_stream` | Production data (copied from Python project) |

---

## Task 1: Initialize the Rust project

**Files:**
- Create: `rust-template/Cargo.toml`
- Create: `rust-template/src/lib.rs`
- Create: `rust-template/src/main.rs`

- [ ] **Step 1: Create the project directory and Cargo.toml**

  Run from the repo root (`c:\Users\Alexander\python-template\`):
  ```
  mkdir rust-template
  ```

  Create `rust-template/Cargo.toml`:
  ```toml
  [package]
  name    = "rust-template"
  version = "0.1.0"
  edition = "2021"

  [lib]
  name = "rust_template"
  path = "src/lib.rs"

  [[bin]]
  name = "rust-template"
  path = "src/main.rs"

  [dependencies]
  axum               = "0.7"
  tokio              = { version = "1", features = ["full"] }
  serde              = { version = "1", features = ["derive"] }
  serde_json         = "1"
  envy               = "0.4"
  polars             = { version = "0.44", features = ["ipc_streaming"] }
  rmcp               = { version = "0.1", features = ["server", "transport-streamable-http-server"] }
  utoipa             = { version = "4", features = ["axum_extras"] }
  utoipa-swagger-ui  = { version = "7", features = ["axum"] }
  tracing            = "0.1"
  tracing-subscriber = { version = "0.3", features = ["env-filter"] }

  [dev-dependencies]
  tower          = { version = "0.4", features = ["util"] }
  http-body-util = "0.1"
  ```

- [ ] **Step 2: Create skeleton src/lib.rs**

  Create `rust-template/src/lib.rs`:
  ```rust
  pub mod config;
  ```

- [ ] **Step 3: Create skeleton src/main.rs**

  Create `rust-template/src/main.rs`:
  ```rust
  fn main() {
      println!("rust-template starting");
  }
  ```

- [ ] **Step 4: Verify the project compiles**

  Run from `rust-template/`:
  ```
  cargo check
  ```
  Expected: no errors (dependencies will download on first run — this may take a few minutes).

- [ ] **Step 5: Commit**

  ```
  git add rust-template/
  git commit -m "feat: initialise rust-template Rust crate"
  ```

---

## Task 2: Settings (`src/config.rs`)

**Files:**
- Create: `rust-template/src/config.rs`
- Modify: `rust-template/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

  Create `rust-template/src/config.rs`:
  ```rust
  use serde::Deserialize;

  #[derive(Deserialize, Clone, Debug)]
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
              app_title:   "Rust Template Service".to_string(),
              app_version: "1.0.0".to_string(),
              status:      "running".to_string(),
              data_dir:    "./data".to_string(),
          }
      }
  }

  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn test_defaults() {
          let s = Settings::default();
          assert_eq!(s.status,  "running");
          assert_eq!(s.data_dir, "./data");
      }

      #[test]
      fn test_struct_update_syntax() {
          let s = Settings {
              status:   "testing".to_string(),
              data_dir: "./tests/test_data".to_string(),
              ..Settings::default()
          };
          assert_eq!(s.status,    "testing");
          assert_eq!(s.data_dir,  "./tests/test_data");
          assert_eq!(s.app_title, "Rust Template Service");
      }
  }
  ```

- [ ] **Step 2: Register the module in lib.rs**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod config;
  ```

- [ ] **Step 3: Run the tests**

  ```
  cargo test config
  ```
  Expected:
  ```
  test config::tests::test_defaults ... ok
  test config::tests::test_struct_update_syntax ... ok
  ```

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/config.rs rust-template/src/lib.rs
  git commit -m "feat: add Settings struct with env-var loading via envy"
  ```

---

## Task 3: Response schemas (`src/schemas/`)

**Files:**
- Create: `rust-template/src/schemas/mod.rs`
- Create: `rust-template/src/schemas/data.rs`
- Modify: `rust-template/src/lib.rs`

- [ ] **Step 1: Create the schemas module**

  Create `rust-template/src/schemas/mod.rs`:
  ```rust
  pub mod data;
  ```

  Create `rust-template/src/schemas/data.rs`:
  ```rust
  use serde::{Deserialize, Serialize};
  use utoipa::ToSchema;

  #[derive(Debug, Serialize, Deserialize, ToSchema)]
  pub struct DataShapeResponse {
      pub height: u64,
      pub width:  u64,
  }
  ```

- [ ] **Step 2: Register in lib.rs**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod config;
  pub mod schemas;
  ```

- [ ] **Step 3: Write and run a serialisation test**

  Add to the bottom of `rust-template/src/schemas/data.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn test_serialises_to_json() {
          let r = DataShapeResponse { height: 3, width: 2 };
          let json = serde_json::to_string(&r).unwrap();
          assert_eq!(json, r#"{"height":3,"width":2}"#);
      }
  }
  ```

  ```
  cargo test schemas
  ```
  Expected:
  ```
  test schemas::data::tests::test_serialises_to_json ... ok
  ```

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/schemas/
  git commit -m "feat: add DataShapeResponse schema with utoipa ToSchema"
  ```

---

## Task 4: Health service (`src/services/health.rs`)

**Files:**
- Create: `rust-template/src/services/mod.rs`
- Create: `rust-template/src/services/health.rs`
- Modify: `rust-template/src/lib.rs`

- [ ] **Step 1: Write the failing test first**

  Create `rust-template/src/services/mod.rs`:
  ```rust
  pub mod health;
  ```

  Create `rust-template/src/services/health.rs` with the test written first:
  ```rust
  use crate::config::Settings;

  pub trait HealthServiceTrait: Send + Sync {
      fn get_status(&self) -> String;
  }

  pub struct HealthService {
      settings: Settings,
  }

  impl HealthService {
      pub fn new(settings: Settings) -> Self {
          Self { settings }
      }
  }

  impl HealthServiceTrait for HealthService {
      fn get_status(&self) -> String {
          self.settings.status.clone()
      }
  }

  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn test_returns_status_from_settings() {
          let settings = Settings {
              status: "testing".to_string(),
              ..Settings::default()
          };
          let svc = HealthService::new(settings);
          assert_eq!(svc.get_status(), "testing");
      }

      #[test]
      fn test_returns_default_status() {
          let svc = HealthService::new(Settings::default());
          assert_eq!(svc.get_status(), "running");
      }
  }
  ```

- [ ] **Step 2: Register in lib.rs**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod config;
  pub mod schemas;
  pub mod services;
  ```

- [ ] **Step 3: Run the tests**

  ```
  cargo test services::health
  ```
  Expected:
  ```
  test services::health::tests::test_returns_default_status ... ok
  test services::health::tests::test_returns_status_from_settings ... ok
  ```

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/services/
  git commit -m "feat: add HealthService with trait for DI"
  ```

---

## Task 5: Test data setup

**Files:**
- Create: `rust-template/tests/test_data/data.ipc_stream` (copied)
- Create: `rust-template/data/data.ipc_stream` (copied)

- [ ] **Step 1: Create directories and copy data files**

  Run from the repo root (`c:\Users\Alexander\python-template\`):

  On Windows (PowerShell):
  ```powershell
  New-Item -ItemType Directory -Force -Path rust-template\tests\test_data
  New-Item -ItemType Directory -Force -Path rust-template\data
  Copy-Item tests\test_data\data.ipc_stream rust-template\tests\test_data\data.ipc_stream
  Copy-Item data\data.ipc_stream rust-template\data\data.ipc_stream
  ```

  On Linux (CI/container):
  ```bash
  mkdir -p rust-template/tests/test_data rust-template/data
  cp tests/test_data/data.ipc_stream rust-template/tests/test_data/data.ipc_stream
  cp data/data.ipc_stream rust-template/data/data.ipc_stream
  ```

- [ ] **Step 2: Add a .gitkeep and add the ipc_stream files to git**

  The `.ipc_stream` files are binary but small, committed alongside the code as test fixtures.
  ```
  git add rust-template/tests/test_data/data.ipc_stream
  git add rust-template/data/data.ipc_stream
  git commit -m "chore: add Arrow IPC stream data files for rust-template"
  ```

---

## Task 6: Data service (`src/services/data.rs`)

**Files:**
- Create: `rust-template/src/services/data.rs`
- Modify: `rust-template/src/services/mod.rs`

- [ ] **Step 1: Write the failing unit test first**

  Add to `rust-template/src/services/mod.rs`:
  ```rust
  pub mod health;
  pub mod data;
  ```

  Create `rust-template/src/services/data.rs`:
  ```rust
  use std::fs::File;
  use polars::prelude::*;
  use crate::config::Settings;
  use crate::schemas::data::DataShapeResponse;

  pub trait DataServiceTrait: Send + Sync {
      fn get_shape(&self) -> DataShapeResponse;
  }

  pub struct DataService {
      settings: Settings,
  }

  impl DataService {
      pub fn new(settings: Settings) -> Self {
          Self { settings }
      }
  }

  impl DataServiceTrait for DataService {
      fn get_shape(&self) -> DataShapeResponse {
          let path = format!("{}/data.ipc_stream", self.settings.data_dir);
          let file = File::open(&path)
              .unwrap_or_else(|_| panic!("data file not found: {path}"));
          let df = IpcStreamReader::new(file)
              .finish()
              .expect("failed to read IPC stream");
          DataShapeResponse {
              height: df.height() as u64,
              width:  df.width()  as u64,
          }
      }
  }

  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn test_get_shape_reads_test_data() {
          let settings = Settings {
              data_dir: "./tests/test_data".to_string(),
              ..Settings::default()
          };
          let svc = DataService::new(settings);
          let shape = svc.get_shape();
          assert_eq!(shape.height, 3);
          assert_eq!(shape.width,  2);
      }
  }
  ```

  > **Note on Polars imports:** `use polars::prelude::*` brings in `IpcStreamReader` and `SerReader`
  > (which provides `.finish()`). If the compiler reports a missing trait, add
  > `use polars::io::SerReader;` explicitly.

- [ ] **Step 2: Run the test — confirm it fails (data file missing) or passes**

  Run from `rust-template/`:
  ```
  cargo test services::data
  ```
  Expected (after completing Task 5):
  ```
  test services::data::tests::test_get_shape_reads_test_data ... ok
  ```
  If the data file was not yet copied, the test will panic with "data file not found" — complete Task 5 first.

- [ ] **Step 3: Commit**

  ```
  git add rust-template/src/services/data.rs rust-template/src/services/mod.rs
  git commit -m "feat: add DataService reading Arrow IPC stream via Polars"
  ```

---

## Task 7: Application state (`src/state.rs`)

**Files:**
- Create: `rust-template/src/state.rs`
- Modify: `rust-template/src/lib.rs`

- [ ] **Step 1: Create AppState**

  Create `rust-template/src/state.rs`:
  ```rust
  use std::sync::Arc;
  use crate::{
      config::Settings,
      services::{
          health::{HealthServiceTrait, HealthService},
          data::{DataServiceTrait, DataService},
      },
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

  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn test_new_wires_services() {
          let state = AppState::new(&Settings::default());
          assert_eq!(state.health_service.get_status(), "running");
      }
  }
  ```

- [ ] **Step 2: Register in lib.rs**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod config;
  pub mod schemas;
  pub mod services;
  pub mod state;
  ```

- [ ] **Step 3: Run the test**

  ```
  cargo test state
  ```
  Expected:
  ```
  test state::tests::test_new_wires_services ... ok
  ```

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/state.rs rust-template/src/lib.rs
  git commit -m "feat: add AppState as single DI wiring point"
  ```

---

## Task 8: HTTP routers with OpenAPI annotations (`src/routers/`)

**Files:**
- Create: `rust-template/src/routers/mod.rs`
- Create: `rust-template/src/routers/health.rs`
- Create: `rust-template/src/routers/data.rs`
- Modify: `rust-template/src/lib.rs`

- [ ] **Step 1: Create the routers**

  Create `rust-template/src/routers/mod.rs`:
  ```rust
  pub mod health;
  pub mod data;
  ```

  Create `rust-template/src/routers/health.rs`:
  ```rust
  use axum::{extract::State, Json};
  use crate::state::AppState;

  #[utoipa::path(
      get,
      path = "/health/status",
      tag = "Health",
      responses(
          (status = 200, description = "Current service status string", body = String)
      )
  )]
  pub async fn get_status(State(state): State<AppState>) -> Json<String> {
      Json(state.health_service.get_status())
  }
  ```

  Create `rust-template/src/routers/data.rs`:
  ```rust
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

- [ ] **Step 2: Register in lib.rs**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod config;
  pub mod routers;
  pub mod schemas;
  pub mod services;
  pub mod state;
  ```

- [ ] **Step 3: Verify it compiles**

  ```
  cargo check
  ```
  Expected: no errors.

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/routers/
  git commit -m "feat: add health and data routers with utoipa OpenAPI annotations"
  ```

---

## Task 9: OpenAPI document (`src/openapi.rs`)

**Files:**
- Create: `rust-template/src/openapi.rs`
- Modify: `rust-template/src/lib.rs`

- [ ] **Step 1: Create ApiDoc**

  Create `rust-template/src/openapi.rs`:
  ```rust
  use utoipa::OpenApi;
  use crate::{
      routers::{health, data},
      schemas::data::DataShapeResponse,
  };

  #[derive(OpenApi)]
  #[openapi(
      paths(
          health::get_status,
          data::get_shape,
      ),
      components(schemas(DataShapeResponse)),
      tags(
          (name = "Health",       description = "Endpoints for checking service health and status"),
          (name = "Data Service", description = "Endpoints for querying the dataset"),
      ),
      info(
          title       = "Rust Template Service",
          version     = "1.0.0",
          description = "A Rust service with REST and MCP endpoints, ready for Claude",
      )
  )]
  pub struct ApiDoc;
  ```

- [ ] **Step 2: Register in lib.rs**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod app;
  pub mod config;
  pub mod openapi;
  pub mod routers;
  pub mod schemas;
  pub mod services;
  pub mod state;
  ```

  > `app` is declared here even though it doesn't exist yet — create a placeholder now and implement it in Task 10.

  Create placeholder `rust-template/src/app.rs`:
  ```rust
  // populated in Task 10
  ```

- [ ] **Step 3: Write a test that the OpenAPI JSON can be serialised**

  Add to `rust-template/src/openapi.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use utoipa::OpenApi;

      #[test]
      fn test_openapi_doc_serialises() {
          let doc = ApiDoc::openapi();
          let json = doc.to_pretty_json().unwrap();
          assert!(json.contains("/health/status"));
          assert!(json.contains("/data/shape"));
          assert!(json.contains("DataShapeResponse"));
      }
  }
  ```

- [ ] **Step 4: Run the test**

  ```
  cargo test openapi
  ```
  Expected:
  ```
  test openapi::tests::test_openapi_doc_serialises ... ok
  ```

- [ ] **Step 5: Commit**

  ```
  git add rust-template/src/openapi.rs rust-template/src/app.rs rust-template/src/lib.rs
  git commit -m "feat: add ApiDoc OpenAPI document aggregating all routes and schemas"
  ```

---

## Task 10: App assembly (`src/app.rs`)

**Files:**
- Modify: `rust-template/src/app.rs`

- [ ] **Step 1: Implement build_app**

  Replace `rust-template/src/app.rs` with:
  ```rust
  use axum::{routing::get, Router};
  use utoipa::OpenApi;
  use utoipa_swagger_ui::SwaggerUi;
  use crate::{
      mcp,
      openapi::ApiDoc,
      routers::{data, health},
      state::AppState,
  };

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

  > `mcp::build_router` does not yet exist — it will be added in Task 13. The crate will not compile
  > until Task 13 is complete. This is intentional: the integration tests (Tasks 11–12) will be
  > written now but will only be runnable once Task 13 is done.

- [ ] **Step 2: Add pub use to lib.rs so tests can call build_app directly**

  Replace `rust-template/src/lib.rs` with:
  ```rust
  pub mod app;
  pub mod config;
  pub mod mcp;
  pub mod openapi;
  pub mod routers;
  pub mod schemas;
  pub mod services;
  pub mod state;

  pub use app::build_app;
  ```

  Create placeholder `rust-template/src/mcp/mod.rs` (to let lib.rs compile):
  ```rust
  pub mod tools;

  use axum::Router;
  use crate::state::AppState;

  pub fn build_router(_state: AppState) -> Router {
      Router::new()
  }
  ```

  Create placeholder `rust-template/src/mcp/tools.rs`:
  ```rust
  // populated in Task 13
  ```

- [ ] **Step 3: Verify everything compiles**

  ```
  cargo check
  ```
  Expected: no errors.

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/app.rs rust-template/src/mcp/ rust-template/src/lib.rs
  git commit -m "feat: add build_app assembling REST, OpenAPI, and MCP routers"
  ```

---

## Task 11: Test infrastructure (`tests/common/mod.rs`)

**Files:**
- Create: `rust-template/tests/common/mod.rs`

This is the Rust equivalent of `tests/conftest.py` — a shared helper available to all integration
test files.

- [ ] **Step 1: Create test_state()**

  Create `rust-template/tests/common/mod.rs`:
  ```rust
  use rust_template::{config::Settings, state::AppState};

  pub fn test_state() -> AppState {
      let settings = Settings {
          status:   "testing".to_string(),
          data_dir: "./tests/test_data".to_string(),
          ..Settings::default()
      };
      AppState::new(&settings)
  }
  ```

  > `Settings { ..Settings::default() }` — this is the Rust equivalent of Python's
  > `Settings(status="testing", data_dir="./tests/test_data")`. The `..` syntax fills in all
  > unspecified fields from `Settings::default()`. No framework is involved.

- [ ] **Step 2: Verify it compiles as part of the test tree**

  ```
  cargo test --test test_rest 2>&1 | head -5
  ```
  This will fail because `test_rest.rs` doesn't exist yet — that is expected. What we want to
  confirm is no compile error from `common/mod.rs` itself. If the error is "file not found for
  module" that is fine; if it is a type error in `common/mod.rs`, fix it now.

- [ ] **Step 3: Commit**

  ```
  git add rust-template/tests/common/mod.rs
  git commit -m "test: add test_state() helper (equivalent of conftest.py)"
  ```

---

## Task 12: REST integration tests (`tests/test_rest.rs`)

**Files:**
- Create: `rust-template/tests/test_rest.rs`

- [ ] **Step 1: Write the tests**

  Create `rust-template/tests/test_rest.rs`:
  ```rust
  use axum::body::Body;
  use axum::http::{Request, StatusCode};
  use http_body_util::BodyExt;
  use rust_template::build_app;
  use tower::ServiceExt;

  mod common;

  #[tokio::test]
  async fn test_health_status() {
      let app = build_app(common::test_state());

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
      let app = build_app(common::test_state());

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

  #[tokio::test]
  async fn test_swagger_ui_is_served() {
      let app = build_app(common::test_state());

      let response = app
          .oneshot(Request::get("/docs").body(Body::empty()).unwrap())
          .await
          .unwrap();

      // Swagger UI redirects or serves HTML — either 200 or 303 is acceptable
      assert!(
          response.status() == StatusCode::OK || response.status() == StatusCode::SEE_OTHER,
          "expected 200 or 303, got {}", response.status()
      );
  }

  #[tokio::test]
  async fn test_openapi_json_is_served() {
      let app = build_app(common::test_state());

      let response = app
          .oneshot(Request::get("/api-doc/openapi.json").body(Body::empty()).unwrap())
          .await
          .unwrap();

      assert_eq!(response.status(), StatusCode::OK);
      let body = response.into_body().collect().await.unwrap().to_bytes();
      let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
      assert!(doc["paths"].get("/health/status").is_some());
      assert!(doc["paths"].get("/data/shape").is_some());
  }
  ```

- [ ] **Step 2: Run the REST tests**

  ```
  cargo test --test test_rest
  ```
  Expected:
  ```
  test test_health_status        ... ok
  test test_get_shape            ... ok
  test test_swagger_ui_is_served ... ok
  test test_openapi_json_is_served ... ok
  ```

- [ ] **Step 3: Commit**

  ```
  git add rust-template/tests/test_rest.rs
  git commit -m "test: add REST integration tests using Tower oneshot"
  ```

---

## Task 13: MCP tools and router (`src/mcp/`)

**Files:**
- Modify: `rust-template/src/mcp/mod.rs`
- Modify: `rust-template/src/mcp/tools.rs`

> **rmcp API note:** The streamable-HTTP transport API in `rmcp 0.1.x` is actively evolving.
> Before implementing this task, check the current rmcp documentation and examples at
> https://github.com/modelcontextprotocol/rust-sdk for the exact `StreamableHttpService`
> constructor signature and the `ServerHandler` trait requirements. The code below reflects the
> expected API pattern — adjust if the actual API differs.

- [ ] **Step 1: Implement McpHandler with tool methods**

  Replace `rust-template/src/mcp/tools.rs` with:
  ```rust
  use rmcp::{tool, ServerHandler, model::ServerInfo};
  use crate::state::AppState;

  #[derive(Clone)]
  pub struct McpHandler {
      pub state: AppState,
  }

  #[tool(tool_box)]
  impl McpHandler {
      #[tool(description = "Get health service status")]
      async fn get_health_service_tool(&self) -> String {
          self.state.health_service.get_status()
      }

      #[tool(description = "Get shape of the dataset")]
      async fn get_data_service_tool(&self) -> String {
          serde_json::to_string(&self.state.data_service.get_shape())
              .unwrap_or_default()
      }
  }

  impl ServerHandler for McpHandler {
      fn get_info(&self) -> ServerInfo {
          ServerInfo {
              server_info: rmcp::model::Implementation {
                  name:    "rust-template".to_string(),
                  version: "1.0.0".to_string(),
              },
              ..Default::default()
          }
      }
  }
  ```

- [ ] **Step 2: Implement build_router**

  Replace `rust-template/src/mcp/mod.rs` with:
  ```rust
  pub mod tools;

  use axum::Router;
  use rmcp::transport::streamable_http_server::{
      StreamableHttpService,
      session::local::LocalSessionManager,
  };
  use crate::state::AppState;
  use tools::McpHandler;

  pub fn build_router(state: AppState) -> Router {
      let service = StreamableHttpService::new(
          move || Ok(McpHandler { state: state.clone() }),
          LocalSessionManager::default().into(),
          Default::default(),
      );
      Router::new().fallback_service(service)
  }
  ```

  > If the rmcp API differs from the above (e.g. constructor takes different arguments or the
  > Axum integration is via a different type), consult the rmcp docs and adjust. The observable
  > contract is: `build_router` returns an `axum::Router` that handles MCP JSON-RPC 2.0 POST
  > requests and sets the `mcp-session-id` response header on `initialize` calls.

- [ ] **Step 3: Verify it compiles**

  ```
  cargo check
  ```
  Expected: no errors. If rmcp API errors occur, resolve them against current rmcp docs before continuing.

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/mcp/
  git commit -m "feat: add McpHandler with get_health_service_tool and get_data_service_tool"
  ```

---

## Task 14: MCP integration test (`tests/test_mcp.rs`)

**Files:**
- Create: `rust-template/tests/test_mcp.rs`

- [ ] **Step 1: Write the MCP test**

  Create `rust-template/tests/test_mcp.rs`:
  ```rust
  use axum::body::Body;
  use axum::http::{Request, StatusCode};
  use http_body_util::BodyExt;
  use rust_template::build_app;
  use tower::ServiceExt;

  mod common;

  fn parse_mcp_response(body: &[u8], content_type: &str) -> serde_json::Value {
      if content_type.contains("text/event-stream") {
          let text = std::str::from_utf8(body).unwrap();
          text.lines()
              .find(|l| l.starts_with("data:"))
              .map(|l| serde_json::from_str(&l["data:".len()..].trim()).unwrap())
              .expect("no data: line in SSE response")
      } else {
          serde_json::from_slice(body).unwrap()
      }
  }

  #[tokio::test]
  async fn test_mcp_tool_list() {
      let app = build_app(common::test_state());

      // Step 1: initialize session
      let init_body = serde_json::json!({
          "jsonrpc": "2.0",
          "id": 1,
          "method": "initialize",
          "params": {
              "protocolVersion": "2025-03-26",
              "capabilities": {},
              "clientInfo": {"name": "test-client", "version": "0.1"}
          }
      });

      let init_response = app
          .clone()
          .oneshot(
              Request::post("/")
                  .header("content-type", "application/json")
                  .header("accept", "application/json, text/event-stream")
                  .body(Body::from(init_body.to_string()))
                  .unwrap(),
          )
          .await
          .unwrap();

      assert_eq!(init_response.status(), StatusCode::OK);

      let session_id = init_response
          .headers()
          .get("mcp-session-id")
          .expect("mcp-session-id header missing on initialize response")
          .to_str()
          .unwrap()
          .to_string();

      let ct = init_response
          .headers()
          .get("content-type")
          .unwrap()
          .to_str()
          .unwrap()
          .to_string();
      let init_bytes = init_response.into_body().collect().await.unwrap().to_bytes();
      let init_result = parse_mcp_response(&init_bytes, &ct);
      assert!(init_result.get("result").is_some(), "initialize result missing");

      // Step 2: list tools
      let list_body = serde_json::json!({
          "jsonrpc": "2.0",
          "method": "tools/list",
          "id": 2
      });

      let list_response = app
          .oneshot(
              Request::post("/")
                  .header("content-type", "application/json")
                  .header("accept", "application/json, text/event-stream")
                  .header("mcp-session-id", &session_id)
                  .body(Body::from(list_body.to_string()))
                  .unwrap(),
          )
          .await
          .unwrap();

      assert_eq!(list_response.status(), StatusCode::OK);

      let ct2 = list_response
          .headers()
          .get("content-type")
          .unwrap()
          .to_str()
          .unwrap()
          .to_string();
      let list_bytes = list_response.into_body().collect().await.unwrap().to_bytes();
      let list_result = parse_mcp_response(&list_bytes, &ct2);

      let tool_names: Vec<&str> = list_result["result"]["tools"]
          .as_array()
          .expect("tools array missing")
          .iter()
          .map(|t| t["name"].as_str().unwrap())
          .collect();

      assert!(
          tool_names.contains(&"get_health_service_tool"),
          "get_health_service_tool not in tool list: {tool_names:?}"
      );
      assert!(
          tool_names.contains(&"get_data_service_tool"),
          "get_data_service_tool not in tool list: {tool_names:?}"
      );
  }
  ```

  > **MCP path note:** The MCP router is nested at `/mcp` in `build_app` — but in the test above
  > the requests are sent to `/` because `oneshot` calls the MCP sub-router directly from
  > `common::test_state()`. If you are testing through `build_app`, change the path to `/mcp/`.
  > Adjust based on whether the test calls `build_app(test_state())` (use `/mcp/`) or a standalone
  > MCP router (use `/`). The assertion logic is identical either way.

- [ ] **Step 2: Run the MCP test**

  ```
  cargo test --test test_mcp
  ```
  Expected:
  ```
  test test_mcp_tool_list ... ok
  ```

  If the test fails with a session or routing error, check the rmcp `StreamableHttpService`
  mount path and adjust the request path (`/` vs `/mcp/`) accordingly.

- [ ] **Step 3: Run the full test suite**

  ```
  cargo test
  ```
  Expected: all tests pass (unit + integration).

- [ ] **Step 4: Commit**

  ```
  git add rust-template/tests/test_mcp.rs
  git commit -m "test: add MCP JSON-RPC integration test asserting tool names"
  ```

---

## Task 15: Entry point (`src/main.rs`)

**Files:**
- Modify: `rust-template/src/main.rs`

- [ ] **Step 1: Implement main**

  Replace `rust-template/src/main.rs` with:
  ```rust
  use rust_template::{app::build_app, config::Settings, state::AppState};

  #[tokio::main]
  async fn main() {
      tracing_subscriber::fmt::init();

      let settings = envy::from_env::<Settings>().unwrap_or_default();
      tracing::info!(
          app_title   = %settings.app_title,
          app_version = %settings.app_version,
          status      = %settings.status,
          data_dir    = %settings.data_dir,
          "starting rust-template"
      );

      let state    = AppState::new(&settings);
      let app      = build_app(state);
      let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
      tracing::info!(addr = %listener.local_addr().unwrap(), "listening");
      axum::serve(listener, app).await.unwrap();
  }
  ```

  > `envy::from_env::<Settings>()` reads env vars matching the field names in uppercase
  > (`STATUS`, `DATA_DIR`, etc.). `unwrap_or_default()` falls back to `Settings::default()` if
  > no env vars are set — this is intentional for local development without a K8s environment.

- [ ] **Step 2: Smoke test — start the server**

  ```
  cargo run
  ```
  Expected output (approximate):
  ```
  2026-06-02T...  INFO rust_template: starting rust-template app_title=... status=running ...
  2026-06-02T...  INFO rust_template: listening addr=0.0.0.0:8000
  ```

  In a second terminal, verify the endpoints respond:
  ```
  curl http://localhost:8000/health/status
  ```
  Expected: `"running"`

  ```
  curl http://localhost:8000/data/shape
  ```
  Expected: `{"height":...,"width":...}` (dimensions of `data/data.ipc_stream`)

  Open `http://localhost:8000/docs` in a browser — Swagger UI should load with the two routes documented.

  Stop the server with Ctrl+C.

- [ ] **Step 3: Run the full test suite one final time**

  ```
  cargo test
  ```
  Expected: all tests pass.

- [ ] **Step 4: Commit**

  ```
  git add rust-template/src/main.rs
  git commit -m "feat: implement main entry point with tracing and env-var config"
  ```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by task |
|---|---|
| Same JSON shapes | Task 3 (schemas), verified in Task 12 tests |
| Same MCP tool names (`get_health_service_tool`, `get_data_service_tool`) | Task 13, verified in Task 14 test |
| Same Arrow IPC stream format | Task 5 (data files), Task 6 (reader) |
| K8s env-var config | Task 2 (`envy`) |
| Windows + Linux build | No platform-specific code; pure Rust |
| OpenAPI + Swagger UI at `/docs` | Tasks 8, 9, 10, verified in Task 12 |
| DI Goal 1 (test injection) | Task 11 (`test_state()`), Tasks 12–14 (tests use it) |
| DI Goal 2 (single wiring point) | Task 7 (`state.rs` is the only file naming concrete types) |
| In-process test client | Tasks 12–14 (`oneshot()`) |
| `rmcp` MCP server | Task 13 |

**No placeholders:** Tasks 1–12 and 15 have complete code. Task 13 has a prominently flagged
API verification note for rmcp; this is intentional given the crate's evolving API, not a
placeholder — the code represents the expected pattern and the note tells the implementer exactly
what to verify and adjust.

**Type consistency:** `AppState`, `Settings`, `DataShapeResponse`, `build_app`, `test_state` are
used consistently across all tasks. `get_status()` returns `String` throughout. `get_shape()`
returns `DataShapeResponse` throughout.
