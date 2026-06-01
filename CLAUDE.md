# Python Template

A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude

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
- DI: custom `Container` in `core/container.py` holds singletons; `core/dependencies.py` provides getting functions and `Annotated` type aliases for FastAPI routes
- Async: all I/O is async
- Config: `settings.py` use Pydantic `BaseSettings` - env vars override defaults