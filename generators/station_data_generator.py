#!/usr/bin/env python3
"""
Generate realistic station telemetry from the supplied app_events.csv.

The source CSV already contains station snapshots such as:
    selected_site_id
    city
    site_category
    available_bays
    total_bays
    estimated_wait_min
    price_per_kwh
    event_time

This script turns those observations into a higher-frequency station telemetry
stream suitable for Kafka -> MySQL -> Grafana.

Run:
    python station_data_generator.py
    python station_data_generator.py --input app_events.csv --output station_telemetry.csv
    python station_data_generator.py --interval 5 --seed 42

The generator uses guardrails:
    * station capacity is anchored to observed CSV values
    * available bays can never exceed total bays
    * occupancy changes gradually instead of jumping randomly every row
    * queue length rises when utilization is high
    * wait time is tied to queue and service duration
    * demand follows day-part/weekend patterns
    * rare incidents temporarily reduce operational capacity
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "app_events.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "station_telemetry.csv"


# ---------------------------------------------------------------------------
# Realism guardrails
# ---------------------------------------------------------------------------

CATEGORY_DEMAND = {
    "MALL": {
        "base": 0.48,
        "morning": 0.38,
        "afternoon": 0.58,
        "evening": 0.70,
        "night": 0.20,
        "weekend_boost": 0.12,
        "session_min": 32,
    },
    "OFFICE": {
        "base": 0.48,
        "morning": 0.70,
        "afternoon": 0.62,
        "evening": 0.28,
        "night": 0.10,
        "weekend_boost": -0.12,
        "session_min": 38,
    },
    "HIGHWAY": {
        "base": 0.55,
        "morning": 0.58,
        "afternoon": 0.60,
        "evening": 0.62,
        "night": 0.42,
        "weekend_boost": 0.08,
        "session_min": 42,
    },
    "RESIDENTIAL": {
        "base": 0.42,
        "morning": 0.28,
        "afternoon": 0.35,
        "evening": 0.58,
        "night": 0.62,
        "weekend_boost": 0.05,
        "session_min": 30,
    },
}

DEFAULT_PROFILE = {
    "base": 0.48,
    "morning": 0.50,
    "afternoon": 0.55,
    "evening": 0.60,
    "night": 0.25,
    "weekend_boost": 0.05,
    "session_min": 35,
}


def clamp(value, low, high):
    return max(low, min(high, value))


def day_part(hour: int) -> str:
    if 6 <= hour < 12:
        return "MORNING"
    if 12 <= hour < 17:
        return "AFTERNOON"
    if 17 <= hour < 23:
        return "EVENING"
    return "NIGHT"


def make_station_master(df: pd.DataFrame) -> pd.DataFrame:
    selected = df[df["selected_site_id"].astype(str) != "NONE"].copy()

    if selected.empty:
        raise ValueError("No selected charging stations found in app_events.csv.")

    grouped = (
        selected.groupby("selected_site_id", as_index=False)
        .agg(
            city=("city", "first"),
            site_category=("site_category", "first"),
            total_bays=("total_bays", "median"),
            observed_available=("available_bays", "median"),
            observed_wait=("estimated_wait_min", "median"),
            price_per_kwh=("price_per_kwh", "median"),
        )
    )

    # Capacity is anchored to the actual dataset.
    grouped["total_bays"] = grouped["total_bays"].round().clip(lower=1).astype(int)

    # Median observed availability is used as a calibration point, not copied
    # into every generated row.
    grouped["observed_utilization"] = (
        1
        - grouped["observed_available"]
        / grouped["total_bays"].replace(0, np.nan)
    ).clip(0, 1)

    return grouped


def generate(
    input_path: Path,
    output_path: Path,
    seed: int = 42,
    interval_minutes: int = 5,
) -> pd.DataFrame:
    if interval_minutes <= 0:
        raise ValueError("--interval must be greater than zero.")

    rng = np.random.default_rng(seed)
    df = pd.read_csv(input_path)

    required = {
        "event_time",
        "selected_site_id",
        "city",
        "site_category",
        "total_bays",
        "available_bays",
        "estimated_wait_min",
        "price_per_kwh",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["event_time"] = pd.to_datetime(df["event_time"])

    master = make_station_master(df)

    start = df["event_time"].min().floor(f"{interval_minutes}min")
    end = df["event_time"].max().ceil(f"{interval_minutes}min")

    timestamps = pd.date_range(
        start=start,
        end=end,
        freq=f"{interval_minutes}min",
    )

    records = []

    # Initial state is calibrated to the source data for each station.
    state = {}

    for _, station in master.iterrows():
        sid = station["selected_site_id"]
        total = int(station["total_bays"])

        observed_util = float(station["observed_utilization"])
        initial_util = clamp(
            observed_util + rng.normal(0, 0.05),
            0.05,
            0.98,
        )

        state[sid] = {
            "utilization": initial_util,
            "incident_remaining": 0,
            "incident_type": "NONE",
        }

    for ts in timestamps:
        current_part = day_part(ts.hour)
        weekend = ts.weekday() >= 5

        for _, station in master.iterrows():
            sid = station["selected_site_id"]
            city = station["city"]
            category = station["site_category"]
            total = int(station["total_bays"])

            profile = CATEGORY_DEMAND.get(category, DEFAULT_PROFILE)
            target = profile["base"]

            if current_part == "MORNING":
                target = profile["morning"]
            elif current_part == "AFTERNOON":
                target = profile["afternoon"]
            elif current_part == "EVENING":
                target = profile["evening"]
            elif current_part == "NIGHT":
                target = profile["night"]

            if weekend:
                target += profile["weekend_boost"]

            # Station-specific calibration from the original CSV.
            observed_util = float(station["observed_utilization"])
            target = 0.65 * target + 0.35 * observed_util

            s = state[sid]

            # Rare maintenance/power events. These are deliberately short and
            # rare so they create realistic bursts instead of permanent faults.
            if s["incident_remaining"] <= 0 and rng.random() < 0.0015:
                s["incident_remaining"] = int(rng.integers(3, 13))
                s["incident_type"] = str(
                    rng.choice(
                        ["POWER_LIMITATION", "CHARGER_MAINTENANCE"],
                        p=[0.65, 0.35],
                    )
                )

            if s["incident_remaining"] > 0:
                s["incident_remaining"] -= 1
                incident = s["incident_type"]
            else:
                incident = "NONE"

            incident_capacity_loss = 0
            if incident == "POWER_LIMITATION":
                incident_capacity_loss = 1
            elif incident == "CHARGER_MAINTENANCE":
                incident_capacity_loss = min(1, total - 1)

            operational_bays = max(1, total - incident_capacity_loss)

            # Smooth state transition: real station utilization tends to move
            # rather than independently jumping from 20% to 100%.
            shock = rng.normal(0, 0.025)
            mean_reversion = (target - s["utilization"]) * 0.12
            s["utilization"] += mean_reversion + shock

            # Incident conditions can temporarily raise utilization pressure.
            if incident != "NONE":
                s["utilization"] += 0.035

            s["utilization"] = clamp(s["utilization"], 0.02, 0.995)

            occupied = int(round(operational_bays * s["utilization"]))
            occupied = int(clamp(occupied, 0, operational_bays))
            available = operational_bays - occupied

            utilization = occupied / operational_bays

            # Queue is tied to high utilization. A station with plenty of
            # available bays should almost never have a long queue.
            if utilization < 0.65:
                queue = int(rng.poisson(0.15))
            elif utilization < 0.80:
                queue = int(rng.poisson(0.8))
            elif utilization < 0.92:
                queue = int(rng.poisson(2.0))
            else:
                queue = int(rng.poisson(3.5))

            if available == 0:
                queue = max(queue, int(rng.integers(1, 4)))

            queue = int(clamp(queue, 0, 12))

            avg_session = max(
                15.0,
                profile["session_min"] * rng.normal(1.0, 0.06),
            )

            # Basic queueing relationship:
            # wait ≈ queue × service time / number of operational chargers.
            base_wait = queue * avg_session / max(operational_bays, 1)

            # Small operational noise, but never negative.
            wait = max(0.0, base_wait + rng.normal(0, 1.2))

            # Full station should have a meaningful wait unless an unusual
            # immediate handoff is occurring.
            if available == 0:
                wait = max(wait, avg_session * 0.35)

            wait = clamp(wait, 0.0, 180.0)

            # Use observed site price as the anchor. Price changes slowly, not
            # randomly every five minutes.
            base_price = float(station["price_per_kwh"])
            price = base_price * rng.normal(1.0, 0.008)
            price = round(clamp(price, 8.0, 30.0), 2)

            fast_charger = category in {"HIGHWAY", "MALL"}

            records.append(
                {
                    "event_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "selected_site_id": sid,
                    "city": city,
                    "site_category": category,
                    "day_part": current_part,
                    "is_weekend": weekend,
                    "total_bays": total,
                    "operational_bays": operational_bays,
                    "occupied_bays": occupied,
                    "available_bays": available,
                    "active_chargers": max(0, operational_bays - occupied),
                    "queue_length": queue,
                    "utilization_pct": round(utilization * 100, 2),
                    "estimated_wait_min": round(wait, 2),
                    "average_session_duration_min": round(avg_session, 2),
                    "price_per_kwh": price,
                    "is_fast_charger_site": fast_charger,
                    "incident_flag": incident != "NONE",
                    "incident_type": incident,
                }
            )

    result = pd.DataFrame(records)

    # -----------------------------------------------------------------------
    # Hard consistency checks — these are the "guardrails".
    # -----------------------------------------------------------------------

    assert (result["total_bays"] >= 1).all()
    assert (result["operational_bays"] >= 1).all()
    assert (result["operational_bays"] <= result["total_bays"]).all()

    assert (result["occupied_bays"] >= 0).all()
    assert (result["available_bays"] >= 0).all()
    assert (
        result["occupied_bays"] + result["available_bays"]
        == result["operational_bays"]
    ).all()

    assert (result["queue_length"] >= 0).all()
    assert (result["estimated_wait_min"] >= 0).all()
    assert (result["utilization_pct"].between(0, 100)).all()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    print(f"Generated {len(result):,} station telemetry records")
    print(f"Stations: {result['selected_site_id'].nunique()}")
    print(f"Interval: {interval_minutes} minutes")
    print(f"Input   : {input_path}")
    print(f"Output  : {output_path}")

    print("\nRisk-oriented summary:")
    print(
        result.groupby("selected_site_id")
        .agg(
            avg_utilization_pct=("utilization_pct", "mean"),
            avg_wait_min=("estimated_wait_min", "mean"),
            max_wait_min=("estimated_wait_min", "max"),
            zero_bay_pct=("available_bays", lambda x: (x == 0).mean() * 100),
        )
        .round(2)
        .to_string()
    )

    print("\nIncident rate:")
    print(result["incident_type"].value_counts(normalize=True).round(4))

    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Telemetry interval in minutes (default: 5)",
    )
    args = parser.parse_args()

    generate(
        Path(args.input),
        Path(args.output),
        seed=args.seed,
        interval_minutes=args.interval,
    )


if __name__ == "__main__":
    main()
