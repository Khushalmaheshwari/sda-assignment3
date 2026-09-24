"""
Business/risk logic for the EV charging streaming project.

Core principle:
    Do not create a large arbitrary weighted average.

The risk score is driven by four logically connected factors:

    1. Charging delay
    2. Station availability pressure
    3. Waiting-time surge
    4. Vehicle urgency

The score is multiplicative because severe operational conditions compound
one another.

All thresholds are project guardrails and should be validated/tuned against
observed streaming data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class RiskResult:
    expected_charge_duration_min: float
    delay_ratio: float
    delay_score: float

    availability_ratio: float
    congestion_factor: float

    wait_surge: float
    wait_surge_factor: float

    urgency_factor: float

    risk_score: float
    risk_level: str
    alert_triggered: bool
    alert_reason: str


def safe_ratio(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator is None or denominator <= 0:
        return default
    return numerator / denominator


def expected_charge_duration(
    battery_capacity_kwh: float,
    start_soc_pct: float,
    target_soc_pct: float,
    charger_power_kw: float,
    efficiency: float = 0.90,
) -> float:
    """
    Expected duration from energy requirement and effective charging power.

    A taper factor is added above 80% SOC because high-SOC charging generally
    takes longer than a simple constant-power calculation.
    """
    start_soc_pct = max(0.0, min(start_soc_pct, 100.0))
    target_soc_pct = max(start_soc_pct + 1.0, min(target_soc_pct, 100.0))

    energy = battery_capacity_kwh * (
        target_soc_pct - start_soc_pct
    ) / 100.0

    taper_factor = 1.0 + max(0.0, target_soc_pct - 80.0) / 100.0

    effective_power = max(charger_power_kw * efficiency, 0.1)

    duration = (energy / effective_power) * 60.0 * taper_factor
    return max(duration, 1.0)


def delay_score(delay_ratio: float) -> float:
    """
    Discrete severity score.

        <=1.10     Normal
        1.10-1.25  Slight
        1.25-1.50  Moderate
        1.50-2.00  Severe
        >2.00      Extreme
    """
    if delay_ratio <= 1.10:
        return 0.0
    if delay_ratio <= 1.25:
        return 1.0
    if delay_ratio <= 1.50:
        return 2.0
    if delay_ratio <= 2.00:
        return 3.0
    return 4.0


def congestion_factor(
    available_bays: float,
    total_bays: float,
) -> tuple[float, float]:
    availability = safe_ratio(available_bays, total_bays, 0.0)

    if availability > 0.50:
        factor = 1.00
    elif availability > 0.25:
        factor = 1.25
    elif availability > 0.10:
        factor = 1.50
    elif availability > 0:
        factor = 2.00
    else:
        factor = 2.50

    return availability, factor


def wait_surge_factor(
    current_wait_min: float,
    baseline_wait_min: float,
) -> tuple[float, float]:
    """
    Baseline should ideally be station + day-part + weekday/weekend specific.
    """
    if baseline_wait_min is None or baseline_wait_min <= 0:
        return 1.0, 1.0

    surge = max(current_wait_min, 0.0) / baseline_wait_min

    if surge < 1.25:
        factor = 1.00
    elif surge < 1.50:
        factor = 1.25
    elif surge < 2.00:
        factor = 1.50
    elif surge <= 3.00:
        factor = 2.00
    else:
        factor = 2.50

    return surge, factor


def vehicle_urgency_factor(
    soc_pct: float,
    range_left_km: Optional[float] = None,
    nearest_distance_km: Optional[float] = None,
) -> float:
    """
    SOC is the primary urgency signal.

    Range/distance can strengthen urgency when their semantics are available.
    We do not make a distance-based claim when those fields are missing or
    unusable.
    """
    soc = max(0.0, min(float(soc_pct), 100.0))

    if soc < 10:
        factor = 1.50
    elif soc < 20:
        factor = 1.25
    elif soc < 30:
        factor = 1.10
    else:
        factor = 1.00

    # Additional pressure only when both range and destination distance exist.
    if (
        factor >= 1.25
        and range_left_km is not None
        and nearest_distance_km is not None
        and nearest_distance_km >= 0
        and range_left_km > 0
    ):
        pressure = nearest_distance_km / range_left_km
        if pressure >= 0.80:
            factor *= 1.15

    # Hard cap keeps this a multiplier rather than an uncontrolled amplifier.
    return min(factor, 2.00)


def classify_risk(score: float) -> str:
    if score <= 2:
        return "NORMAL"
    if score <= 5:
        return "WATCH"
    if score <= 9:
        return "WARNING"
    return "CRITICAL"


def evaluate(
    *,
    actual_charge_duration_min: float,
    expected_charge_duration_min: float,
    available_bays: float,
    total_bays: float,
    current_wait_min: float,
    baseline_wait_min: float,
    vehicle_soc_pct: float,
    range_left_km: Optional[float] = None,
    nearest_distance_km: Optional[float] = None,
    max_score: float = 15.0,
) -> RiskResult:

    delay_ratio = safe_ratio(
        actual_charge_duration_min,
        expected_charge_duration_min,
        1.0,
    )

    d_score = delay_score(delay_ratio)

    availability, c_factor = congestion_factor(
        available_bays,
        total_bays,
    )

    surge, w_factor = wait_surge_factor(
        current_wait_min,
        baseline_wait_min,
    )

    u_factor = vehicle_urgency_factor(
        vehicle_soc_pct,
        range_left_km,
        nearest_distance_km,
    )

    raw_score = d_score * c_factor * w_factor * u_factor
    score = min(max_score, raw_score)

    level = classify_risk(score)

    # Guardrail:
    # A charging-delay alert requires a genuine delay AND supporting context.
    delay_condition = delay_ratio > 1.25

    supporting_conditions = (
        availability < 0.25
        or surge >= 1.50
        or vehicle_soc_pct < 20
    )

    alert = bool(delay_condition and supporting_conditions)

    reasons = []
    if delay_ratio > 1.25:
        reasons.append(f"charging delay {delay_ratio:.2f}x")
    if availability < 0.25:
        reasons.append(f"low availability {availability:.0%}")
    if surge >= 1.50:
        reasons.append(f"wait surge {surge:.2f}x")
    if vehicle_soc_pct < 20:
        reasons.append(f"low SOC {vehicle_soc_pct:.1f}%")

    reason = "; ".join(reasons) if reasons else "no alert condition"

    return RiskResult(
        expected_charge_duration_min=expected_charge_duration_min,
        delay_ratio=delay_ratio,
        delay_score=d_score,
        availability_ratio=availability,
        congestion_factor=c_factor,
        wait_surge=surge,
        wait_surge_factor=w_factor,
        urgency_factor=u_factor,
        risk_score=score,
        risk_level=level,
        alert_triggered=alert,
        alert_reason=reason,
    )
