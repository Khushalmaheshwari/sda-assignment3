"""
Validate app_events.csv against the rules in app_events_rules.md.

    python validate_app_events.py
    python validate_app_events.py path/to/app_events.csv

Exits 0 if everything passes, 1 otherwise.
"""
import sys
from pathlib import Path

import pandas as pd

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / "app_events.csv"
PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PATH
if not PATH.exists():
    sys.exit(f"not found: {PATH}")

df = pd.read_csv(PATH)
df["event_time"] = pd.to_datetime(df["event_time"])
for c in ["filter_fast_only", "converted_to_charge", "is_weekend", "is_anomaly"]:
    df[c] = df[c].astype(str).str.upper() == "TRUE"

passed, failed = 0, 0


def chk(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}" + (f"   [{detail}]" if detail else ""))


ordered = df.sort_values(["journey_id", "journey_step"])
g = ordered.groupby("journey_id")

CONST = ["user_id", "user_tier", "device_platform", "app_version",
         "network_type", "city", "search_radius_km", "filter_connector",
         "filter_fast_only", "results_returned", "nearest_site_id",
         "nearest_distance_km", "vehicle_soc_pct", "range_left_km", "is_weekend"]

print("=" * 62)
print(f"INVARIANTS   {PATH.name}   {len(df):,} rows")
print("=" * 62)

chk("1  no nulls or blank cells",
    df.isna().sum().sum() == 0 and (df.astype(str) == "").sum().sum() == 0)
chk("2  event_id unique", df.event_id.is_unique)
chk("3  journey_step sequential from 1",
    bool(g.journey_step.apply(lambda s: list(s) == list(range(1, len(s) + 1))).all()))
chk("4  exactly one SEARCH, at step 1",
    bool(g.apply(lambda x: (x.action == "SEARCH").sum() == 1
                 and x.iloc[0].action == "SEARCH", include_groups=False).all()))
chk("5  journey-constant fields never vary",
    bool(all(g[c].nunique().max() == 1 for c in CONST)))
chk("6  event_time increases within a journey",
    bool(g.event_time.apply(lambda s: s.is_monotonic_increasing).all()))
chk("7  available_bays <= total_bays",
    bool((df.available_bays <= df.total_bays).all()))
chk("8  converted_to_charge only on START_CHARGE",
    bool((df[df.converted_to_charge].action == "START_CHARGE").all()))
chk("9  reservation_id present only from RESERVE onward",
    bool(set(df[df.reservation_id != "NONE"].action)
         <= {"RESERVE", "START_CHARGE", "ABANDON"}))
chk("10 reservation_window > 0 iff reservation_id set",
    bool(((df.reservation_id != "NONE") == (df.reservation_window_min > 0)).all()))
chk("11 selected_site_id is NONE only on SEARCH",
    bool((df[df.action == "SEARCH"].selected_site_id == "NONE").all()
         and (df[df.action != "SEARCH"].selected_site_id != "NONE").all()))
z = df[df.results_returned == 0]
chk("12 zero results => NONE site and journey of length 1",
    bool((z.nearest_site_id == "NONE").all()
         and g.size()[z.journey_id].max() == 1))
nz = df[df.results_returned > 0]
chk("13 nearest_distance <= search_radius",
    bool((nz.nearest_distance_km <= nz.search_radius_km).all()))
plenty = df[(df.available_bays >= 3) & (df.results_returned > 0)]
share = (plenty.estimated_wait_min == 0).mean()
chk("14 wait mostly zero when 3+ bays free", 0.75 <= share <= 0.90,
    f"{share:.1%} zero")
chk("15 site_category consistent per site",
    bool(df[df.selected_site_id != "NONE"]
         .groupby("selected_site_id").site_category.nunique().max() == 1))
first = g.first()
chk("16 is_weekend matches journey's first event",
    bool((first.is_weekend == (first.event_time.dt.weekday >= 5)).all()))


def daypart(h):
    return ("NIGHT" if h < 6 else "MORNING" if h < 12
            else "AFTERNOON" if h < 17 else "EVENING" if h < 21 else "NIGHT")


chk("17 day_part matches hour of event_time",
    bool((df.day_part == df.event_time.dt.hour.map(daypart)).all()))
chk("18 ABANDON always preceded by NAVIGATE",
    set(df[df.action == "ABANDON"].journey_id)
    <= set(df[df.action == "NAVIGATE"].journey_id))

# ------------------------------------------------------------ distributions
print()
print("=" * 62)
print("DISTRIBUTIONS")
print("=" * 62)

n0 = df[df.action == "SEARCH"].journey_id.nunique()
conv = set(df[df.action == "START_CHARGE"].journey_id)
nav = df[df.action == "NAVIGATE"][["journey_id", "available_bays",
                                   "estimated_wait_min"]].copy()
nav["won"] = nav.journey_id.isin(conv)
res = set(df[df.action == "RESERVE"].journey_id)
navj = set(df[df.action == "NAVIGATE"].journey_id)
soc = df[df.action == "SEARCH"][["journey_id", "vehicle_soc_pct"]].copy()
soc["won"] = soc.journey_id.isin(conv)

CHECKS = [
    ("SEARCH -> VIEW_STATION",
     df[df.action == "VIEW_STATION"].journey_id.nunique() / n0, .74, .78),
    ("SEARCH -> NAVIGATE",
     df[df.action == "NAVIGATE"].journey_id.nunique() / n0, .52, .57),
    ("SEARCH -> START_CHARGE", len(conv) / n0, .37, .41),
    ("reservation rate among navigators", len(res) / len(navj), .18, .20),
    ("positive estimated_wait_min", (df.estimated_wait_min > 0).mean(), .48, .53),
    ("zero-result searches",
     (df[df.action == "SEARCH"].results_returned == 0).mean(), .04, .05),
    ("conversion when 0 bays free",
     nav[nav.available_bays == 0].won.mean(), .33, .38),
    ("conversion when wait > 25 min",
     nav[nav.estimated_wait_min > 25].won.mean(), .45, .50),
    ("conversion when SoC < 20%",
     soc[soc.vehicle_soc_pct < 20].won.mean(), .56, .60),
    ("conversion when reserved", len(res & conv) / len(res), .84, .88),
]

for name, val, lo, hi in CHECKS:
    flag = "PASS" if lo <= val <= hi else "OFF "
    if flag == "PASS":
        passed += 1
    else:
        failed += 1
    print(f"  {flag}  {name:36s} {val:6.3f}   target {lo}-{hi}")

print()
print("=" * 62)
print(f"{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
