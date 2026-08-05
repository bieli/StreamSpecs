#!/usr/bin/env python3
"""Produce IoT TemperatureSensorEvent samples to NATS JetStream for StreamSpecs.

Requires: pip install nats-py
Usage:    python scripts/generate_iot_events_nats.py [--servers nats://localhost:4222]
Compatible with Python 3.8+.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from typing import Any, Dict, Union

try:
    import nats
    from nats.js.api import StreamConfig
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Install nats-py: pip install nats-py") from exc


Payload = Union[Dict[str, Any], str]


def encode_payload(payload: Payload) -> bytes:
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload).encode("utf-8")


def make_payload(i: int, now: int) -> tuple[Payload, str]:
    if random.random() < 0.7:
        payload = {
            "deviceId": "sensor-{:02d}".format(i % 5),
            "temperature": round(random.uniform(-10, 40), 2),
            "humidity": round(random.uniform(20, 80), 2),
            "timestamp": now + i,
        }
        return payload, "CLEAN"

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
    return random.choice(dirty), "DIRTY"


async def run(args: argparse.Namespace) -> None:
    nc = await nats.connect(args.servers)
    js = nc.jetstream()

    try:
        await js.add_stream(
            StreamConfig(
                name=args.stream,
                subjects=[args.subject, args.valid_subject, args.dlq_subject],
            )
        )
    except Exception:
        # Stream may already exist (docker nats-init / app create-stream-if-missing).
        pass

    now = int(time.time() * 1000)
    print(
        "Producing {} events to subject={} stream={} @ {}".format(
            args.count, args.subject, args.stream, args.servers
        )
    )

    for i in range(args.count):
        payload, kind = make_payload(i, now)
        await js.publish(args.subject, encode_payload(payload))
        print("  [{}] {}".format(kind, payload))
        await asyncio.sleep(args.interval)

    await nc.drain()
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Produce IoT sensor events to NATS JetStream for StreamSpecs"
    )
    parser.add_argument("--servers", default="nats://localhost:4222")
    parser.add_argument("--stream", default="SENSORS")
    parser.add_argument("--subject", default="incoming-sensors")
    parser.add_argument("--valid-subject", default="valid-sensors")
    parser.add_argument("--dlq-subject", default="sensors-dlq")
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--interval", type=float, default=0.4)
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
