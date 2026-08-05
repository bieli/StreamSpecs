#!/usr/bin/env python3
"""Produce IoT TemperatureSensorEvent samples for StreamSpecs examples.

Requires: pip install kafka-python
Usage:    python scripts/generate_iot_events.py [--bootstrap localhost:9092]
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


Payload = Union[Dict[str, Any], str]


def encode_payload(payload: Payload) -> bytes:
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce IoT sensor events for StreamSpecs")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="incoming-sensors")
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--interval", type=float, default=0.4)
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    now = int(time.time() * 1000)

    print("Producing {} events to {} @ {}".format(args.count, args.topic, args.bootstrap))
    for i in range(args.count):
        if random.random() < 0.7:
            payload = {
                "deviceId": "sensor-{:02d}".format(i % 5),
                "temperature": round(random.uniform(-10, 40), 2),
                "humidity": round(random.uniform(20, 80), 2),
                "timestamp": now + i,
            }
            kind = "CLEAN"
        else:
            dirty = [
                {
                    "deviceId": "sensor-x",
                    "temperature": 150.0,
                    "humidity": 40.0,
                    "timestamp": now + i,
                },
                {
                    "deviceId": "sensor-y",
                    "temperature": 20.0,
                    "humidity": 140.0,
                    "timestamp": now + i,
                },
                {"deviceId": "", "temperature": 20.0, "humidity": 50.0, "timestamp": now + i},
                "{not-json",
            ]
            payload = random.choice(dirty)
            kind = "DIRTY"

        producer.send(args.topic, value=encode_payload(payload))
        print("  [{}] {}".format(kind, payload))
        time.sleep(args.interval)

    producer.flush()
    print("Done.")


if __name__ == "__main__":
    main()
