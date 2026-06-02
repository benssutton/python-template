# Python Template

A Python FastAPI service with MCP endpoints and Rust extensions, ready for Claude.

This application is intended as an illustration and re-usable of best practices when creating a REST-first webservice using FastAPI. See the following sections in this document for details of those best practices.

## Architecture
```
main.py               FastAPI app
  core/               Custom DI container, FastAPI dependency getters and Pydantic `BaseSettings` cofig
  mcp/                MCP Server
  routers/            REST endpoings
  schemas/            Pydantic data classes
  services/           All business logic should reside in services or in a child folder in services
  test/               Pytests
```

## Stack
Fast API
Pydantic
Polars / Arrow if datashaping is required
Pytest

## Key Patterns
- DI: custom `Container` in `core/container.py` holds singletons; `core/dependencies.py` provides getting functions and `Annotated` type aliases for FastAPI routes.  The FastAPI 'app' object should never be imported outside of test fixtures.
- Async: all I/O is async
- Config: `settings.py` use Pydantic `BaseSettings` - env vars override defaults
- Testing: tests invoke REST endpoings via use of a test client, and override application behaviour and data as needed via dependency injection.
- Clear separation between Routers, Schemas and Services.  Routers should implement minimal business logic instead call methods in the service class.
- Clear separation between MCP tools, resources and prompts.  Similar to Routers these should implement minimal business logic and instead call methose in the service class.