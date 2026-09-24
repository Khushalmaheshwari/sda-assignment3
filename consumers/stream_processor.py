#!/usr/bin/env python3
"""
Kafka stream processor for the EV charging project.

Consumes:
    app-events
    charging-telemetry
    station-telemetry

Produces:
    system-alerts

Writes:
    MySQL charging_sessions
    MySQL station_telemetry
    MySQL charging_risk_events

The processor maintains small in-memory state keyed by journey/session/station.
It is intentionally simple enough for a project/demo while demonstrating
real stream processing concepts.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer

try:
    from risk_engine import evaluate
except ImportError:  # run as `python -m consumers.stream_processor` from root
    from consumers.risk_engine import evaluate
from mysql_writer import MySQLWriter


BOOTSTRAP = "localhost:9092"
APP_TOPIC = "app-events"
CHARGING_TOPIC = "charging-telemetry"
STATION_TOPIC = "station-telemetry"
ALERT_TOPIC = "system-alerts"


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value)


class StreamProcessor:
    def __init__(
        self,
        bootstrap: str,
        group_id: str,
        mysql_writer: MySQLWriter,
    ):
        self.mysql = mysql_writer

        self.consumer = KafkaConsumer(
            APP_TOPIC,
            CHARGING_TOPIC,
            STATION_TOPIC,
            bootstrap_servers=bootstrap,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            key_serializer=lambda x: str(x).encode("utf-8"),
        )

        # Current station snapshot.
        self.stations: Dict[str, Dict[str, Any]] = {}

        # Last known app state for a journey.
        self.journeys: Dict[str, Dict[str, Any]] = {}

        # Charging session lookup.
        self.sessions: Dict[str, Dict[str, Any]] = {}

        # Historical wait observations used to build a rolling baseline.
        # Key = (station, day_part, weekend)
        self.wait_history: Dict[tuple, deque] = {}

    def run(self):
        print("Stream processor started.")
        print(
            f"Consuming: {APP_TOPIC}, {CHARGING_TOPIC}, {STATION_TOPIC}"
        )
        print(f"Alerts   : {ALERT_TOPIC}")

        for message in self.consumer:
            topic = message.topic
            row = message.value

            try:
                if topic == STATION_TOPIC:
                    self.handle_station(row)

                elif topic == CHARGING_TOPIC:
                    self.handle_charging(row)

                elif topic == APP_TOPIC:
                    self.handle_app(row)

            except Exception as exc:
                print(
                    f"ERROR processing topic={topic}: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

    # ------------------------------------------------------------------
    # Station stream
    # ------------------------------------------------------------------

    def handle_station(self, row: Dict[str, Any]):
        station_id = row["selected_site_id"]

        self.stations[station_id] = row

        # Store telemetry for Grafana.
        self.mysql.insert_station(row)

        key = (
            station_id,
            row.get("day_part"),
            bool(row.get("is_weekend")),
        )

        history = self.wait_history.setdefault(
            key,
            deque(maxlen=288),  # roughly 24h at 5-minute telemetry
        )

        current_wait = float(row.get("estimated_wait_min", 0) or 0)
        history.append(current_wait)

    # ------------------------------------------------------------------
    # Charging stream
    # ------------------------------------------------------------------

    def handle_charging(self, row: Dict[str, Any]):
        session_id = row["charging_session_id"]

        self.sessions[session_id] = row

        # Store the raw/generated charging session for historical analysis.
        self.mysql.insert_charging(row)

        station = self.stations.get(row["selected_site_id"])

        # If station telemetry has not arrived yet, use the snapshot included
        # in the charging generator.
        if station is None:
            station = {
                "selected_site_id": row["selected_site_id"],
                "available_bays": round(
                    float(row["availability_ratio_at_start"])
                    * 10
                ),
                "total_bays": 10,
                "estimated_wait_min": row[
                    "estimated_wait_at_start_min"
                ],
                "day_part": row.get("day_part"),
                "is_weekend": row.get("is_weekend", False),
            }

        baseline = self.get_wait_baseline(
            station_id=row["selected_site_id"],
            day_part=station.get("day_part"),
            is_weekend=station.get("is_weekend", False),
            fallback=float(
                row.get("estimated_wait_at_start_min", 0) or 0
            ),
        )

        risk = evaluate(
            actual_charge_duration_min=float(
                row["actual_charge_duration_min"]
            ),
            expected_charge_duration_min=float(
                row["expected_charge_duration_min"]
            ),
            available_bays=float(station.get("available_bays", 0)),
            total_bays=float(station.get("total_bays", 1)),
            current_wait_min=float(
                station.get(
                    "estimated_wait_min",
                    row["estimated_wait_at_start_min"],
                )
            ),
            baseline_wait_min=baseline,
            vehicle_soc_pct=float(row["vehicle_soc_start_pct"]),
            range_left_km=self.get_journey_value(
                row.get("journey_id"), "range_left_km"
            ),
            nearest_distance_km=self.get_journey_value(
                row.get("journey_id"), "nearest_distance_km"
            ),
        )

        result = {
            "event_time": row["event_time"],
            "journey_id": row["journey_id"],
            "charging_session_id": session_id,
            "user_id": row["user_id"],
            "selected_site_id": row["selected_site_id"],
            "city": row["city"],

            "expected_charge_duration_min":
                risk.expected_charge_duration_min,
            "actual_charge_duration_min":
                float(row["actual_charge_duration_min"]),
            "delay_ratio": risk.delay_ratio,
            "delay_score": risk.delay_score,

            "available_bays":
                int(station.get("available_bays", 0)),
            "total_bays":
                int(station.get("total_bays", 1)),
            "availability_ratio":
                risk.availability_ratio,
            "congestion_factor":
                risk.congestion_factor,

            "current_wait_min":
                float(station.get("estimated_wait_min", 0) or 0),
            "baseline_wait_min": baseline,
            "wait_surge": risk.wait_surge,
            "wait_surge_factor": risk.wait_surge_factor,

            "vehicle_soc_pct":
                float(row["vehicle_soc_start_pct"]),
            "range_left_km":
                self.get_journey_value(
                    row.get("journey_id"), "range_left_km"
                ),
            "nearest_distance_km":
                self.get_journey_value(
                    row.get("journey_id"), "nearest_distance_km"
                ),
            "urgency_factor": risk.urgency_factor,

            "risk_score": risk.risk_score,
            "risk_level": risk.risk_level,
            "alert_triggered": risk.alert_triggered,
            "alert_reason": risk.alert_reason,
        }

        self.mysql.insert_risk(result)

        # Publish every risk evaluation, not only critical alerts. This allows
        # Grafana/MySQL and downstream consumers to analyse the full risk
        # distribution.
        self.producer.send(
            ALERT_TOPIC,
            key=row["selected_site_id"],
            value=result,
        )

        if risk.alert_triggered:
            print(
                f"ALERT | {risk.risk_level:<8} | "
                f"{row['selected_site_id']} | "
                f"score={risk.risk_score:.2f} | "
                f"{risk.alert_reason}",
                flush=True,
            )

    # ------------------------------------------------------------------
    # App stream
    # ------------------------------------------------------------------

    def handle_app(self, row: Dict[str, Any]):
        journey_id = row.get("journey_id")

        if journey_id:
            previous = self.journeys.setdefault(journey_id, {})
            previous.update(row)

    def get_journey_value(self, journey_id, field):
        if not journey_id:
            return None
        return self.journeys.get(journey_id, {}).get(field)

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------

    def get_wait_baseline(
        self,
        station_id: str,
        day_part: str,
        is_weekend: bool,
        fallback: float,
    ) -> float:
        history = self.wait_history.get(
            (station_id, day_part, bool(is_weekend))
        )

        if history and len(history) >= 5:
            # Median is robust against one temporary queue spike.
            values = sorted(history)
            middle = len(values) // 2
            if len(values) % 2:
                baseline = values[middle]
            else:
                baseline = (values[middle - 1] + values[middle]) / 2

            # Avoid a zero baseline because it would make surge undefined.
            return max(float(baseline), 1.0)

        # Cold-start fallback.
        return max(float(fallback), 1.0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument(
        "--group",
        default="ev-risk-processor",
        help="Kafka consumer group",
    )
    parser.add_argument("--mysql-host", default="localhost")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="root")
    parser.add_argument("--mysql-database", default="EV-Stations")

    args = parser.parse_args()

    writer = MySQLWriter(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
    )

    try:
        writer.connect()
        writer.ensure_tables()

        processor = StreamProcessor(
            bootstrap=args.bootstrap,
            group_id=args.group,
            mysql_writer=writer,
        )
        processor.run()

    except KeyboardInterrupt:
        print("\nprocessor stopped")
    finally:
        writer.close()


if __name__ == "__main__":
    main()
