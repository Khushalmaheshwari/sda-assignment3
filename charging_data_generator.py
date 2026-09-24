#!/usr/bin/env python3
"""
Generate realistic charging-session telemetry from the existing app_events.csv.

Source dataset:
    app_events.csv

Input relationship:
    One charging record is generated for each START_CHARGE app event.

Important:
    This is synthetic telemetry derived from real rows in the supplied dataset.
    It is NOT claimed to be real charger telemetry.

Run:
    python charging_data_generator.py
    python charging_data_generator.py --input app_events.csv --output charging_sessions.csv
    python charging_data_generator.py --seed 42 --limit 100

The generator deliberately uses guardrails so the synthetic data behaves like a
plausible EV charging system rather than independent random columns.
"""

from __future__ import annotations

import argparse
import math
import uuid
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Guardrails / domain assumptions
# ---------------------------------------------------------------------------

MIN_BATTERY_KWH = 40.0
MAX_BATTERY_KWH = 90.0

MIN_SOC = 5.0
MAX_SOC = 95.0

# Approximate vehicle efficiency range. The source CSV gives range_left_km and
# SOC but does not provide battery capacity or vehicle model.
MIN_EFFICIENCY_KM_PER_KWH = 4.2
MAX_EFFICIENCY_KM_PER_KWH = 5.2

# We normally charge to 80-90%. Very occasionally a driver targets 95%.
TARGET_SOC_CHOICES = [80.0, 85.0, 90.0, 95.0]
TARGET_SOC_PROBS = [0.35, 0.40, 0.20, 0.05]

# Charger power guardrails by actual connector.
POWER_RANGES_KW = {
    "CCS2": (50.0, 150.0),
    "TYPE2_AC": (7.2, 22.0),
    "CHAdeMO": (50.0, 100.0),
}

# Fast charging should not be represented as a constant name only. It also
# constrains the actual charger power.
FAST_CONNECTORS = {"CCS2", "CHAdeMO"}

# Expected charging time uses an effective-power factor to account for normal
# conversion losses and charging taper.
BASE_CHARGING_EFFICIENCY = 0.90


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def choose_actual_connector(row: pd.Series, rng: np.random.Generator) -> str:
    """
    Convert the user's filter into a plausible actual connector.

    The source has filter_connector and filter_fast_only, but no actual
    connector field. Therefore we use the filter as a preference, not as truth.
    """
    preferred = str(row["filter_connector"]).upper()
    fast_only = bool(row["filter_fast_only"])

    if fast_only:
        if preferred == "CHADEMO":
            return "CHAdeMO"
        # CCS2 is the dominant synthetic fast option.
        return "CCS2" if rng.random() < 0.78 else "CHAdeMO"

    if preferred == "CCS2":
        return "CCS2" if rng.random() < 0.92 else "TYPE2_AC"
    if preferred == "TYPE2_AC":
        return "TYPE2_AC" if rng.random() < 0.94 else "CCS2"
    if preferred == "CHADEMO":
        return "CHAdeMO" if rng.random() < 0.90 else "CCS2"

    # ANY
    choices = ["CCS2", "TYPE2_AC", "CHAdeMO"]
    probs = [0.52, 0.33, 0.15]
    return str(rng.choice(choices, p=probs))


def choose_power(connector: str, fast_only: bool, rng: np.random.Generator) -> float:
    low, high = POWER_RANGES_KW[connector]

    # Draw from a few realistic charger tiers rather than a completely
    # continuous uniform distribution.
    if connector == "TYPE2_AC":
        tiers = np.array([7.2, 11.0, 22.0])
        probs = np.array([0.20, 0.35, 0.45])
    elif connector == "CCS2":
        tiers = np.array([60.0, 90.0, 120.0, 150.0])
        probs = np.array([0.15, 0.30, 0.40, 0.15])
    else:
        tiers = np.array([50.0, 75.0, 100.0])
        probs = np.array([0.35, 0.45, 0.20])

    power = float(rng.choice(tiers, p=probs))

    # Guardrail: fast-only sessions cannot accidentally receive AC power.
    if fast_only and connector == "TYPE2_AC":
        power = 22.0

    return clamp(power, low, high)


def infer_battery_capacity(row: pd.Series, rng: np.random.Generator) -> float:
    """
    Infer a plausible battery size from the supplied range/SOC relationship.

    full_range ≈ range_left / SOC
    battery ≈ full_range / vehicle_efficiency

    This keeps the generated battery capacity logically tied to the CSV instead
    of assigning an unrelated random battery size.
    """
    soc = max(float(row["vehicle_soc_pct"]), MIN_SOC)
    remaining_range = max(float(row["range_left_km"]), 1.0)

    inferred_full_range = remaining_range / (soc / 100.0)

    # Use efficiency as a latent variable because the source CSV does not
    # contain vehicle model or battery capacity.
    efficiency = rng.uniform(
        MIN_EFFICIENCY_KM_PER_KWH,
        MAX_EFFICIENCY_KM_PER_KWH,
    )

    battery = inferred_full_range / efficiency

    # Real-world guardrail.
    battery = clamp(battery, MIN_BATTERY_KWH, MAX_BATTERY_KWH)

    # Small manufacturing/model variation.
    battery *= rng.normal(1.0, 0.025)
    return round(clamp(battery, MIN_BATTERY_KWH, MAX_BATTERY_KWH), 1)


def choose_target_soc(start_soc: float, rng: np.random.Generator) -> float:
    target = float(rng.choice(TARGET_SOC_CHOICES, p=TARGET_SOC_PROBS))

    # Never generate a target below the starting SOC.
    if target <= start_soc + 5:
        target = min(95.0, math.ceil(start_soc / 5.0) * 5.0 + 15.0)

    return round(clamp(target, start_soc + 5.0, MAX_SOC), 1)


def calculate_expected_duration(
    energy_required_kwh: float,
    charger_power_kw: float,
    target_soc: float,
) -> float:
    """
    Expected time includes normal charging losses and a taper penalty as SOC
    approaches the upper end of the battery.
    """
    taper_factor = 1.0 + max(0.0, target_soc - 80.0) / 100.0
    effective_power = charger_power_kw * BASE_CHARGING_EFFICIENCY

    hours = energy_required_kwh / max(effective_power, 0.1)
    minutes = hours * 60.0 * taper_factor

    return max(5.0, minutes)


def choose_operational_factor(row: pd.Series, rng: np.random.Generator) -> tuple[float, str]:
    """
    Most sessions are normal. A small minority have operational degradation.

    The rare severe event is the mechanism that creates genuine streaming
    alerts later. It is deliberately rare so the dashboard does not become
    permanently critical.
    """
    u = rng.random()

    if u < 0.012:
        return float(rng.uniform(1.55, 2.30)), "CHARGER_FAULT"
    if u < 0.035:
        return float(rng.uniform(1.25, 1.55)), "POWER_LIMITATION"
    if u < 0.10:
        return float(rng.uniform(1.10, 1.25)), "MINOR_SLOWDOWN"

    return float(rng.uniform(0.92, 1.10)), "NORMAL"


def generate(input_path: Path, output_path: Path, seed: int = 42, limit: int | None = None) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    df = pd.read_csv(input_path)

    required = {
        "event_time",
        "journey_id",
        "user_id",
        "city",
        "selected_site_id",
        "site_category",
        "filter_connector",
        "filter_fast_only",
        "vehicle_soc_pct",
        "range_left_km",
        "available_bays",
        "total_bays",
        "estimated_wait_min",
        "day_part",
        "is_weekend",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    starts = (
        df[df["action"].astype(str).str.upper().eq("START_CHARGE")]
        .copy()
        .sort_values("event_time")
    )

    if limit:
        starts = starts.head(limit)

    if starts.empty:
        raise ValueError("No START_CHARGE rows found in the input CSV.")

    records = []

    for _, row in starts.iterrows():
        start_time = pd.Timestamp(row["event_time"])

        start_soc = clamp(float(row["vehicle_soc_pct"]), MIN_SOC, MAX_SOC)
        battery_capacity = infer_battery_capacity(row, rng)
        target_soc = choose_target_soc(start_soc, rng)

        connector = choose_actual_connector(row, rng)
        charger_power = choose_power(
            connector,
            bool(row["filter_fast_only"]),
            rng,
        )

        energy_required = (
            battery_capacity * (target_soc - start_soc) / 100.0
        )

        # Prevent impossible energy values.
        energy_required = clamp(
            energy_required,
            2.0,
            battery_capacity * 0.95,
        )

        expected_duration = calculate_expected_duration(
            energy_required,
            charger_power,
            target_soc,
        )

        # Congestion is derived from the app snapshot. It affects actual time,
        # but only moderately; otherwise every busy station becomes an alert.
        total_bays = max(int(row["total_bays"]), 1)
        available_bays = clamp(
            float(row["available_bays"]),
            0,
            total_bays,
        )
        availability_ratio = available_bays / total_bays

        congestion_factor = (
            1.00 if availability_ratio > 0.50 else
            1.05 if availability_ratio > 0.25 else
            1.12 if availability_ratio > 0.10 else
            1.20
        )

        wait = max(float(row["estimated_wait_min"]), 0.0)
        wait_factor = 1.0 + min(wait / 60.0, 0.30)

        operational_factor, operating_condition = choose_operational_factor(
            row, rng
        )

        actual_duration = (
            expected_duration
            * congestion_factor
            * wait_factor
            * operational_factor
        )

        # Real-world session bounds.
        actual_duration = clamp(actual_duration, 5.0, 360.0)

        end_time = start_time + pd.Timedelta(
            minutes=float(actual_duration)
        )

        # Delivered energy is slightly below the ideal requested energy in
        # normal operation, but remains physically plausible.
        delivery_efficiency = clamp(
            rng.normal(0.96, 0.015),
            0.90,
            0.995,
        )
        energy_delivered = energy_required * delivery_efficiency

        delay_ratio = actual_duration / max(expected_duration, 1.0)

        # Synthetic session-level flags. These are NOT copied from is_anomaly.
        severe_delay = delay_ratio > 1.50
        operational_anomaly = operating_condition != "NORMAL"

        records.append(
            {
                "charging_session_id": f"CHG_{uuid.uuid4().hex[:10].upper()}",
                "event_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "charge_start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "charge_end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "journey_id": row["journey_id"],
                "user_id": row["user_id"],
                "selected_site_id": row["selected_site_id"],
                "city": row["city"],
                "site_category": row["site_category"],
                "vehicle_soc_start_pct": round(start_soc, 1),
                "target_soc_pct": round(target_soc, 1),
                "battery_capacity_kwh": round(battery_capacity, 1),
                "charger_connector": connector,
                "charger_power_kw": round(charger_power, 1),
                "is_fast_charger": connector in FAST_CONNECTORS and charger_power >= 50,
                "energy_required_kwh": round(energy_required, 2),
                "energy_delivered_kwh": round(energy_delivered, 2),
                "expected_charge_duration_min": round(expected_duration, 2),
                "actual_charge_duration_min": round(actual_duration, 2),
                "delay_ratio": round(delay_ratio, 3),
                "availability_ratio_at_start": round(availability_ratio, 3),
                "estimated_wait_at_start_min": round(wait, 1),
                "operating_condition": operating_condition,
                "operational_anomaly": operational_anomaly,
                "severe_delay": severe_delay,
                "day_part": row["day_part"],
                "is_weekend": bool(row["is_weekend"]),
            }
        )

    result = pd.DataFrame(records)

    # Final consistency checks.
    assert (result["vehicle_soc_start_pct"] >= MIN_SOC).all()
    assert (result["target_soc_pct"] > result["vehicle_soc_start_pct"]).all()
    assert (result["battery_capacity_kwh"].between(MIN_BATTERY_KWH, MAX_BATTERY_KWH)).all()
    assert (result["actual_charge_duration_min"] >= 5).all()
    assert (result["energy_delivered_kwh"] <= result["energy_required_kwh"]).all()
    assert (result["availability_ratio_at_start"].between(0, 1)).all()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    print(f"Generated {len(result):,} charging sessions")
    print(f"Input : {input_path}")
    print(f"Output: {output_path}")
    print("\nConnector distribution:")
    print(result["charger_connector"].value_counts(normalize=True).round(3))
    print("\nOperating conditions:")
    print(result["operating_condition"].value_counts(normalize=True).round(3))
    print("\nDelay ratio:")
    print(result["delay_ratio"].describe().round(3))

    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="app_events.csv")
    parser.add_argument("--output", default="charging_sessions.csv")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    generate(
        Path(args.input),
        Path(args.output),
        seed=args.seed,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
