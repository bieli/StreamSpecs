#!/usr/bin/env python3
"""Generate clean and dirty order events into the Kafka incoming topic.

Requires: pip install kafka-python
Usage:    python scripts/generate_events.py [--bootstrap localhost:9092] [--count 50]

Compatible with Python 3.8+.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from typing import Any, Dict, Union

try:
    from kafka import KafkaProducer
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Install kafka-python: pip install kafka-python") from exc


CLEAN = [
    {"id": "ORD-1001", "price": 99.99, "email": "alice@example.com"},
    {"id": "ORD-1002", "price": 45.50, "email": "bob@example.com"},
    {"id": "ORD-1003", "price": 120.00, "email": "carol@example.com"},
]

DIRTY = [
    {"id": "", "price": 10.0, "email": "missing-id@example.com"},  # missing id
    {"id": "ORD-2001", "price": -3.5, "email": "neg@example.com"},  # negative price
    {"id": "ORD-2002", "price": 20.0, "email": "not-an-email"},  # bad email (pass-through warn)
    "{this is not json",  # parse failure
]

Payload = Union[Dict[str, Any], str]


def encode_payload(payload: Payload) -> bytes:
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce sample events for StreamSpecs")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="incoming-orders")
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--interval", type=float, default=0.4)
    args = parser.parse_args()

    # Pass raw bytes - avoids kafka-python 3.x Serializer deprecation warnings
    producer = KafkaProducer(bootstrap_servers=args.bootstrap)

    print("Producing {} events to {} @ {}".format(args.count, args.topic, args.bootstrap))
    for i in range(args.count):
        # ~70% clean, ~30% dirty
        if random.random() < 0.7:
            base = random.choice(CLEAN)
            payload = dict(
                base,
                id="ORD-{}".format(1000 + i),
                price=round(random.uniform(20, 150), 2),
            )
        else:
            payload = random.choice(DIRTY)

        producer.send(args.topic, value=encode_payload(payload))
        kind = (
            "CLEAN"
            if isinstance(payload, dict) and payload.get("id") and payload.get("price", 0) > 0
            else "DIRTY"
        )
        print("  [{}] {}".format(kind, payload))
        time.sleep(args.interval)

    producer.flush()
    print("Done.")


if __name__ == "__main__":
    main()
