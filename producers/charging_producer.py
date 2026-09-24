#!/usr/bin/env python3
"""
Kafka producer for charging_sessions.csv.

Topic:
    charging-telemetry

Examples:
    python charging_producer.py --create-topic
    python charging_producer.py --dry-run --limit 20
    python charging_producer.py --speed 200
    python charging_producer.py --loop --speed 500

The event_time column controls replay timing, exactly like app_producer.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

TOPIC = "charging-telemetry"
CSV_NAME = "charging_sessions.csv"
BOOTSTRAP = "localhost:9092"
PARTITIONS = 3
RETENTION_HOURS = 72

INT_COLS = {"journey_id", "user_id"}
FLOAT_COLS = {
    "vehicle_soc_start_pct", "target_soc_pct", "battery_capacity_kwh",
    "charger_power_kw", "energy_required_kwh", "energy_delivered_kwh",
    "expected_charge_duration_min", "actual_charge_duration_min",
    "delay_ratio", "availability_ratio_at_start",
    "estimated_wait_at_start_min",
}
BOOL_COLS = {
    "is_fast_charger", "operational_anomaly", "severe_delay", "is_weekend"
}

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka import errors as kerr
    KAFKA_READY = True
    KAFKA_IMPORT_ERROR = None
except Exception as exc:
    KAFKA_READY = False
    KAFKA_IMPORT_ERROR = exc
    KafkaProducer = KafkaAdminClient = NewTopic = None
    kerr = None

if KAFKA_READY:
    TopicExists = getattr(kerr, "TopicAlreadyExistsError", None)
else:
    TopicExists = None


def find_csv():
    here = Path(__file__).resolve().parent
    project_root = here.parent
    candidates = [
        here / CSV_NAME,
        project_root / "data" / CSV_NAME,
        Path.cwd() / CSV_NAME,
        Path.cwd() / "data" / CSV_NAME,
    ]
    for path in candidates:
        if path.exists():
            return path
    sys.exit(f"Could not find {CSV_NAME}. Looked in: {candidates}")


def cast_value(key, value):
    value = (value or "").strip()
    if key in FLOAT_COLS:
        return float(value)
    if key in BOOL_COLS:
        return value.upper() in ("TRUE", "1", "YES")
    return value


def load_rows(limit=None):
    path = find_csv()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            sys.exit(f"{path} has no header")
        rows = []
        for row in reader:
            clean = {}
            for key, value in row.items():
                if key is not None:
                    clean[key] = cast_value(key, value)
            rows.append(clean)

    rows.sort(key=lambda r: r["event_time"])
    if limit:
        rows = rows[:limit]
    if not rows:
        sys.exit("No charging records found.")
    return rows, path


def timeouts(cls, seconds=8):
    wanted = {
        "bootstrap_timeout_ms": seconds * 1000,
        "api_version_auto_timeout_ms": seconds * 1000,
        "request_timeout_ms": seconds * 1000,
        "max_block_ms": seconds * 1000,
    }
    allowed = getattr(cls, "DEFAULT_CONFIG", {})
    return {k: v for k, v in wanted.items() if k in allowed}


def require_kafka():
    if not KAFKA_READY:
        sys.exit(
            "kafka-python is not available. Install with:\n"
            "pip install kafka-python"
        )


def create_topic(bootstrap):
    require_kafka()
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        **timeouts(KafkaAdminClient),
    )
    try:
        admin.create_topics([
            NewTopic(
                name=TOPIC,
                num_partitions=PARTITIONS,
                replication_factor=1,
                topic_configs={
                    "retention.ms": str(RETENTION_HOURS * 3600 * 1000)
                },
            )
        ])
        print(f"created {TOPIC}")
    except Exception as exc:
        if (TopicExists and isinstance(exc, TopicExists)) or \
           "TopicAlreadyExists" in type(exc).__name__:
            print(f"exists  {TOPIC}")
        else:
            raise
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--speed", type=float, default=200.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--burst", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    args = parser.parse_args()

    if args.create_topic:
        create_topic(args.bootstrap)
        return

    rows, path = load_rows(args.limit)

    print(f"source   : {path}")
    print(f"topic    : {TOPIC}")
    print(f"messages : {len(rows):,}")
    print(f"window   : {rows[0]['event_time']} -> {rows[-1]['event_time']}")

    if args.dry_run:
        print("mode     : DRY RUN")

    producer = None
    if not args.dry_run:
        require_kafka()
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            acks="all",
            linger_ms=20,
            retries=3,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            key_serializer=lambda x: str(x).encode("utf-8"),
            **timeouts(KafkaProducer),
        )

    sent = 0
    errors = 0

    try:
        while True:
            first = datetime.fromisoformat(rows[0]["event_time"])
            wall_start = time.time()

            for row in rows:
                if not args.burst:
                    simulated = (
                        datetime.fromisoformat(row["event_time"]) - first
                    ).total_seconds()
                    target_elapsed = simulated / max(args.speed, 0.001)
                    actual_elapsed = time.time() - wall_start
                    delay = target_elapsed - actual_elapsed
                    if delay > 0:
                        time.sleep(delay)

                if producer:
                    producer.send(
                        TOPIC,
                        key=row["selected_site_id"],
                        value=row,
                    )
                sent += 1

                if not args.quiet:
                    print(
                        f"{row['event_time']} | "
                        f"{row['selected_site_id']} | "
                        f"delay={row['delay_ratio']}x | "
                        f"{row['operating_condition']}",
                        flush=True,
                    )

            if not args.loop:
                break

            print("--- charging pass complete; looping ---")

    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        if producer:
            producer.flush()
            producer.close()

    print(f"sent/processed: {sent:,}")


if __name__ == "__main__":
    main()
