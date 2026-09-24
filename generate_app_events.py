"""
app-events — single-topic data source for the EV charging network.

Models complete user journeys through the mobile app:
    SEARCH -> VIEW_STATION -> NAVIGATE -> RESERVE -> START_CHARGE

Conversion at each step depends on what the user actually saw: how many results
came back, how many bays were free, the estimated wait, the price, and how much
charge they had left. Those dependencies are what make the funnel worth
analysing rather than just counting.
"""
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta

SEED = 7
rng = np.random.default_rng(SEED)

START = datetime(2026, 8, 3)
DAYS = 14

# ------------------------------------------------------------------ sites
SITES = [
    # site_id, city, category, bays, price, lat, lon
    ("STN_JPR_01", "Jaipur",    "MALL",        5, 18.5, 26.9124, 75.7873),
    ("STN_JPR_02", "Jaipur",    "HIGHWAY",     5, 21.0, 26.8500, 75.8100),
    ("STN_DEL_01", "Delhi",     "MALL",        5, 19.5, 28.6139, 77.2090),
    ("STN_DEL_02", "Delhi",     "OFFICE",      4, 14.0, 28.5355, 77.3910),
    ("STN_DEL_03", "Delhi",     "HIGHWAY",     5, 21.0, 28.7041, 77.1025),
    ("STN_GGN_01", "Gurugram",  "OFFICE",      4, 14.0, 28.4595, 77.0266),
    ("STN_GGN_02", "Gurugram",  "MALL",        5, 19.5, 28.4089, 77.0507),
    ("STN_MUM_01", "Mumbai",    "MALL",        5, 20.0, 19.0760, 72.8777),
    ("STN_MUM_02", "Mumbai",    "RESIDENTIAL", 4, 12.0, 19.1136, 72.8697),
    ("STN_BLR_01", "Bengaluru", "OFFICE",      4, 14.0, 12.9716, 77.5946),
]
SITE = {s[0]: dict(site_id=s[0], city=s[1], category=s[2], bays=s[3],
                   price=s[4], lat=s[5], lon=s[6]) for s in SITES}
BY_CITY = {}
for s in SITES:
    BY_CITY.setdefault(s[1], []).append(s[0])
CITIES = sorted(BY_CITY)
CITY_WEIGHT = np.array([0.28, 0.14, 0.18, 0.22, 0.18])   # Delhi, Blr, Ggn, Mum, Jpr
CITY_ORDER = ["Delhi", "Bengaluru", "Gurugram", "Mumbai", "Jaipur"]

HOUR_W = np.array([1, 1, 1, 1, 2, 4, 7, 10, 12, 11, 10, 10,
                   11, 11, 10, 10, 12, 15, 18, 16, 11, 7, 4, 2], float)

PLATFORMS = (["ANDROID"] * 78 + ["IOS"] * 22)
VERSIONS = ["4.2.1", "4.2.0", "4.1.6", "4.1.2", "3.9.8"]
VERSION_P = [0.44, 0.26, 0.15, 0.10, 0.05]
CONNECTORS = ["CCS2", "TYPE2_AC", "CHAdeMO", "ANY"]
CONNECTOR_P = [0.46, 0.24, 0.06, 0.24]
NETWORKS = ["5G", "4G", "WIFI"]
NETWORK_P = [0.38, 0.49, 0.13]


def day_part(h):
    if h < 6:
        return "NIGHT"
    if h < 12:
        return "MORNING"
    if h < 17:
        return "AFTERNOON"
    if h < 21:
        return "EVENING"
    return "NIGHT"


def site_availability(site_id, when):
    """Free bays at a site, driven by hour of day and site category."""
    s = SITE[site_id]
    h = when.hour
    load = HOUR_W[h] / HOUR_W.max()
    if s["category"] == "OFFICE":
        load *= 1.35 if 9 <= h <= 18 else 0.35
    elif s["category"] == "RESIDENTIAL":
        load *= 1.5 if (h >= 19 or h <= 7) else 0.3
    elif s["category"] == "HIGHWAY":
        load *= 1.2
    busy = int(np.clip(rng.binomial(s["bays"], min(load * 0.85, 0.95)),
                       0, s["bays"]))
    return s["bays"] - busy


rows = []
_id = [0]


def emit(**kw):
    _id[0] += 1
    kw["event_id"] = f"APP_{_id[0]:07d}"
    rows.append(kw)


# ------------------------------------------------------------------ journeys
JOURNEYS = 11000

for _ in range(JOURNEYS):
    day = int(rng.integers(DAYS))
    d0 = START + timedelta(days=day)
    weekend = d0.weekday() >= 5
    w = HOUR_W * (0.78 if weekend else 1.0)
    hour = int(rng.choice(24, p=w / w.sum()))
    t = d0 + timedelta(hours=hour, minutes=int(rng.integers(60)),
                       seconds=int(rng.integers(60)))

    journey_id = f"JNY_{rng.integers(10**8, 10**9)}"
    user_id = f"USR_{rng.integers(10000, 99999)}"
    platform = PLATFORMS[rng.integers(100)]
    version = VERSIONS[rng.choice(5, p=VERSION_P)]
    tier = "SUBSCRIBER" if rng.random() < 0.31 else "FREE"
    city = CITY_ORDER[rng.choice(5, p=CITY_WEIGHT)]
    network = NETWORKS[rng.choice(3, p=NETWORK_P)]

    conn = CONNECTORS[rng.choice(4, p=CONNECTOR_P)]
    fast_only = bool(rng.random() < 0.42)
    radius = float(rng.choice([3, 5, 10, 15, 25], p=[.18, .32, .28, .14, .08]))

    soc = float(np.clip(stats.truncnorm.rvs(-1.4, 1.8, 34, 16,
                                            random_state=rng.integers(1e9)), 4, 88))
    rng_km = round(soc / 100 * float(rng.uniform(280, 420)), 1)
    urgency = 1.0 if soc > 40 else (1.25 if soc > 20 else 1.55)

    pool = BY_CITY[city]
    # coverage gap: a small share of searches return nothing at all
    no_results = rng.random() < 0.045
    n_results = 0 if no_results else int(min(len(pool), max(1, rng.binomial(len(pool), 0.75))))

    if n_results:
        picks = list(rng.choice(pool, size=n_results, replace=False))
        dists = sorted(round(float(rng.uniform(0.4, radius)), 1) for _ in picks)
        near_id, near_km = picks[0], dists[0]
    else:
        picks, dists = [], []
        near_id, near_km = "NONE", 0.0

    def snapshot(site_id, when):
        if site_id == "NONE":
            return 0, 0, 0.0, 0
        s = SITE[site_id]
        free = site_availability(site_id, when)
        if free == 0:                                   # queue for a bay
            wait = int(np.clip(rng.exponential(18) + 6, 5, 75))
        elif free == 1:                                 # last bay, may be taken
            wait = int(np.clip(rng.exponential(6) + 2, 2, 30))
        elif free == 2:                                 # scarce
            wait = 0 if rng.random() < 0.55 else int(rng.integers(2, 12))
        else:                                           # plenty
            wait = 0 if rng.random() < 0.82 else int(rng.integers(1, 6))
        return free, s["bays"], s["price"], wait

    free, total, price, wait = snapshot(near_id, t)

    step = 0

    def row(action, when, sel_id, sel_free, sel_total, sel_price, sel_wait,
            res_id="NONE", res_win=0, converted="FALSE", anomaly="FALSE"):
        global step
        step += 1
        emit(
            event_time=when.strftime("%Y-%m-%d %H:%M:%S"),
            journey_id=journey_id, journey_step=step, action=action,
            user_id=user_id, user_tier=tier,
            device_platform=platform, app_version=version, network_type=network,
            city=city,
            search_lat=round(SITE[near_id]["lat"] + rng.normal(0, .02), 4)
            if near_id != "NONE" else round(rng.uniform(12, 29), 4),
            search_lon=round(SITE[near_id]["lon"] + rng.normal(0, .02), 4)
            if near_id != "NONE" else round(rng.uniform(72, 78), 4),
            search_radius_km=radius,
            filter_connector=conn,
            filter_fast_only=str(fast_only).upper(),
            results_returned=n_results,
            nearest_site_id=near_id, nearest_distance_km=near_km,
            selected_site_id=sel_id,
            site_category=SITE[sel_id]["category"] if sel_id != "NONE" else "NONE",
            available_bays=sel_free, total_bays=sel_total,
            price_per_kwh=sel_price, estimated_wait_min=sel_wait,
            vehicle_soc_pct=round(soc, 1), range_left_km=rng_km,
            reservation_id=res_id, reservation_window_min=res_win,
            converted_to_charge=converted,
            response_time_ms=int(np.clip(rng.lognormal(5.5, .55), 60, 4000)),
            day_part=day_part(when.hour),
            is_weekend=str(weekend).upper(),
            is_anomaly=anomaly,
        )

    # ---- step 1: SEARCH
    row("SEARCH", t, "NONE", free, total, price, wait,
        anomaly="TRUE" if no_results else "FALSE")

    if no_results:
        continue

    # ---- step 2: VIEW_STATION
    p_view = 0.74 * urgency ** 0.4
    if rng.random() > min(p_view, 0.95):
        continue
    t += timedelta(seconds=int(rng.integers(8, 70)))
    sel = picks[0] if rng.random() < 0.62 else str(rng.choice(picks))
    s_free, s_total, s_price, s_wait = snapshot(sel, t)
    row("VIEW_STATION", t, sel, s_free, s_total, s_price, s_wait)

    # ---- step 3: NAVIGATE — availability and wait drive this hard
    p_nav = 0.70 * urgency
    if s_free == 0:
        p_nav *= 0.32
    if s_wait > 25:
        p_nav *= 0.55
    if s_price > 20:
        p_nav *= 0.88
    if rng.random() > min(p_nav, 0.94):
        continue
    t += timedelta(seconds=int(rng.integers(10, 120)))
    row("NAVIGATE", t, sel, s_free, s_total, s_price, s_wait)

    # ---- step 4: RESERVE (optional, subscribers far more likely)
    reserved, res_id, res_win = False, "NONE", 0
    p_res = 0.34 if tier == "SUBSCRIBER" else 0.09
    if s_free <= 1:
        p_res *= 1.6
    if rng.random() < min(p_res, 0.6):
        reserved = True
        res_id = f"RSV_{rng.integers(10**6, 10**7)}"
        res_win = int(rng.choice([15, 20, 30, 45]))
        t += timedelta(seconds=int(rng.integers(5, 40)))
        row("RESERVE", t, sel, s_free, s_total, s_price, s_wait, res_id, res_win)

    # ---- step 5: START_CHARGE
    p_start = 0.80 if reserved else 0.63
    if s_free == 0 and not reserved:
        p_start *= 0.30
    p_start *= urgency ** 0.5
    if rng.random() > min(p_start, 0.96):
        # abandoned after navigating — worth flagging when a bay was free
        if s_free > 0 and rng.random() < 0.4:
            t += timedelta(minutes=int(rng.integers(6, 30)))
            row("ABANDON", t, sel, s_free, s_total, s_price, s_wait,
                res_id, res_win, anomaly="TRUE")
        continue

    t += timedelta(minutes=float(np.clip(rng.exponential(9) + 2, 2, 55)))
    row("START_CHARGE", t, sel, s_free, s_total, s_price, s_wait,
        res_id, res_win, converted="TRUE")

# ------------------------------------------------------------------ write
COLS = ["event_id", "event_time", "journey_id", "journey_step", "action",
        "user_id", "user_tier", "device_platform", "app_version", "network_type",
        "city", "search_lat", "search_lon", "search_radius_km",
        "filter_connector", "filter_fast_only", "results_returned",
        "nearest_site_id", "nearest_distance_km",
        "selected_site_id", "site_category", "available_bays", "total_bays",
        "price_per_kwh", "estimated_wait_min",
        "vehicle_soc_pct", "range_left_km",
        "reservation_id", "reservation_window_min", "converted_to_charge",
        "response_time_ms", "day_part", "is_weekend", "is_anomaly"]

df = pd.DataFrame(rows)[COLS].sort_values("event_time").reset_index(drop=True)

assert df.isna().sum().sum() == 0, "nulls present"
assert (df.astype(str) == "").sum().sum() == 0, "empty strings present"

df.to_csv("/mnt/user-data/outputs/app_events.csv", index=False)

print(f"rows    : {len(df):,}")
print(f"columns : {len(COLS)}")
print(f"journeys: {df.journey_id.nunique():,}")
print(f"window  : {df.event_time.min()} -> {df.event_time.max()}")
print()
print(df.action.value_counts().to_string())
