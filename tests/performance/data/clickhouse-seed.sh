#!/bin/bash
set -e
clickhouse-client --query="INSERT INTO default.items FORMAT Arrow" < /tmp/clickhouse_seed_data.ipc
