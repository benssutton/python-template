# Python Template - Getting Started Guide

## Prerequisites
- Docker Desktop running
- Python 3.12+

## Getting Set Up

### 1. Create Local Docker Services 

```bash
# Start all services (Postgres, Clickhouse, Redis & Flight Server)
docker compose up -d --build

# Verify all containers are running
docker compose ps
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate Self-signed SSL Certificates for HTTPS

```bash
python ./certs/generate_self_signed_cert.py
```

## Running the Application

```bash
python main.py
```

Once running, swagger documentation will be available at https://localhost/docs#/

## Running the Tests

To run the functional pytests with a coverage report at htmlcov/index.html:
```bash
pytest tests/ -v --cov --cov-report=html
```

To run the smoke tests:

First, make sure the k6 image is built:
```bash
docker build -t perf-scripts ./tests/performance
```

```bash
docker run --rm --network python-template_default -e BASE_URL=http://app:8000 perf-scripts run /scripts/smoke.js
```

To run the performance tests:
```bash
docker run --rm --network python-template_default -e BASE_URL=http://app:8000 perf-scripts run /scripts/load.js
```

To run the stress tests:
```bash
docker run --rm --network python-template_default -e BASE_URL=http://app:8000 -e TARGET_RPS=50 perf-scripts run /scripts/stress.js
```

## Integrating with Claude Desktop

Start the FastAPI app — /mcp is mounted at http://localhost:8000/mcp.
Add to Claude Desktop's claude_desktop_config.json:

  "mcpServers": {
    "python-template": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8000/mcp"]
    }
  },

Restart Claude Desktop. The health tool should appear.

