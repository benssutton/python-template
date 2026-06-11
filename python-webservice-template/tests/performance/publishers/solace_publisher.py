"""Continuous Solace publisher for performance test runs.

Environment variables:
  SOLACE_HOST, SOLACE_PORT, SOLACE_VPN, SOLACE_USERNAME, SOLACE_PASSWORD,
  SOLACE_TOPIC, PUBLISH_INTERVAL (seconds between publishes, default 0.1)
"""
import os
import time

from tests.publishers.flight_server import make_batch
from tests.publishers.solace_publisher import SolacePublisher

BATCHES = [
    make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")]),
    make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")]),
    make_batch([(2, "beta", "v1", "delete")]),
]


def main() -> None:
    publisher = SolacePublisher(
        host=os.environ.get("SOLACE_HOST", "localhost"),
        port=int(os.environ.get("SOLACE_PORT", "55555")),
        vpn=os.environ.get("SOLACE_VPN", "default"),
        username=os.environ.get("SOLACE_USERNAME", "admin"),
        password=os.environ.get("SOLACE_PASSWORD", "admin"),
        topic=os.environ.get("SOLACE_TOPIC", "ingest/batches"),
    )
    interval = float(os.environ.get("PUBLISH_INTERVAL", "0.1"))
    try:
        while True:
            for batch in BATCHES:
                publisher.publish_batch(batch)
                time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
