#!/usr/bin/env python3
"""
Kafka producer for station_telemetry.csv.

Topic:
    station-telemetry

Station ID is used as the Kafka key so events for a station remain ordered
within its partition.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

TOPIC = "station-telemetry"
CSV_NAME = "station_telemetry.csv"
BOOTSTRAP = "localhost:9092"
PARTITIONS = 3
RETENTION_HOURS = 72

FLOAT_COLS = {
    "utilization_pct",
    "estimated_wait_min",
    "average_session_duration_min",
    "price_per_kwh",
}
INT_COLS = {
    "total_bays",
    "operational_bays",
    "occupied_bays",
    "available_bays",
    "active_chargers",
    "queue_length",
}
BOOL_COLS = {
    "is_weekend",
    "is_fast_charger_site",
    "incident_flag",
}

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka import errors as kerr
    KAFKA_READY = True
except Exception:
    KAFKA_READY = False
    KafkaProducer = KafkaAdminClient = NewTopic = None
    kerr = None

TopicExists = getattr(kerr, "TopicAlreadyExistsError", None) if KAFKA_READY else None


def find_csv():
    here = Path(__file__).resolve().parent
    candidates = [here / CSV_NAME, Path.cwd() / CSV_NAME]
    for path in candidates:
        if path.exists():
            return path
    sys.exit(f"Could not find {CSV_NAME}. Looked in: {candidates}")


def cast(key, value):
    value = (value or "").strip()
    if key in INT_COLS:
        return int(float(value))
    if key in FLOAT_COLS:
        return float(value)
    if key in BOOL_COLS:
        return value.upper() in ("TRUE", "1", "YES")
    return value


def load_rows(limit=None):
    path = find_csv()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({
                k: cast(k, v)
                for k, v in row.items()
                if k is not None
            })

    rows.sort(key=lambda r: r["event_time"])
    if limit:
        rows = rows[:limit]
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
        sys.exit("Install kafka-python with: pip install kafka-python")


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
            print(f"exists {TOPIC}")
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
    if not rows:
        sys.exit("No station telemetry records found.")

    print(f"source   : {path}")
    print(f"topic    : {TOPIC}")
    print(f"messages : {len(rows):,}")
    print(f"stations : {len({r['selected_site_id'] for r in rows})}")

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
                        f"availability={row['available_bays']}/"
                        f"{row['total_bays']} | "
                        f"wait={row['estimated_wait_min']}m",
                        flush=True,
                    )

            if not args.loop:
                break

            print("--- station pass complete; looping ---")

    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        if producer:
            producer.flush()
            producer.close()

    print(f"sent/processed: {sent:,}")


if __name__ == "__main__":
    main()
