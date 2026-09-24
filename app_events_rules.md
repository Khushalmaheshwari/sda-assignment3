# app_events — Rules, Constraints and Data Dictionary

Specification for `app_events.csv`, the mobile-app event stream of the EV
charging network project.

| | |
|---|---|
| Rows | 31,264 |
| Columns | 34 |
| Journeys | 11,000 |
| Window | 3–16 August 2026 (14 days) |
| Sites | 10 across 5 cities |
| Random seed | 7 (fully reproducible) |
| Generator | `generate_app_events.py` |
| Blank cells | 0 |

---

## 1. What this dataset represents

Every row is one interaction with the operator's driver-facing mobile app.
Rows are grouped into **journeys** — a single user's attempt to find and use a
charger, from the first search to either charging or giving up.

This is the *demand* side of the network. It records what drivers were looking
for and what they saw, which is what makes it possible to explain why sessions
did or did not happen, rather than only counting the ones that did.

---

## 2. Core structure

### 2.1 The journey funnel

Every journey begins with a `SEARCH` and drops out at some point along:

```
SEARCH -> VIEW_STATION -> NAVIGATE -> RESERVE (optional) -> START_CHARGE
                                   \
                                    -> ABANDON
```

| Field | Meaning |
|---|---|
| `journey_id` | Groups all events belonging to one user attempt |
| `journey_step` | Position within that journey, starting at 1 |
| `action` | Which stage this row represents |

**Rule:** `journey_step` is strictly sequential within a journey, starting at 1
with no gaps. A journey has exactly one `SEARCH` and at most one of each
subsequent action.

### 2.2 Journey-constant fields

These are decided once when a journey starts and repeat unchanged on every row
of that journey:

`user_id`, `user_tier`, `device_platform`, `app_version`, `network_type`,
`city`, `search_radius_km`, `filter_connector`, `filter_fast_only`,
`results_returned`, `nearest_site_id`, `nearest_distance_km`,
`vehicle_soc_pct`, `range_left_km`, `is_weekend`

**Rule:** a driver does not change their car's battery level or their search
filters midway through one journey, so these must not vary within a
`journey_id`.

### 2.3 Step-varying fields

These are re-evaluated at each step, because the world changes while the user
deliberates:

`event_time`, `journey_step`, `action`, `selected_site_id`, `site_category`,
`available_bays`, `total_bays`, `price_per_kwh`, `estimated_wait_min`,
`reservation_id`, `reservation_window_min`, `converted_to_charge`,
`response_time_ms`, `day_part`

---

## 3. Data dictionary

### Identity and journey

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 1 | `event_id` | string | `APP_0000001`+ | Unique per row |
| 2 | `event_time` | datetime | 2026-08-03 to 2026-08-16 | `YYYY-MM-DD HH:MM:SS` |
| 3 | `journey_id` | string | `JNY_########` | Groups one user attempt |
| 4 | `journey_step` | int | 1–5 | Sequential within journey |
| 5 | `action` | enum | SEARCH, VIEW_STATION, NAVIGATE, RESERVE, START_CHARGE, ABANDON | |

### User

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 6 | `user_id` | string | `USR_#####` | Pseudonymous |
| 7 | `user_tier` | enum | FREE, SUBSCRIBER | ~31% subscribers |
| 8 | `device_platform` | enum | ANDROID, IOS | 78/22 split |
| 9 | `app_version` | string | 3.9.8 – 4.2.1 | Newer versions dominate |
| 10 | `network_type` | enum | 5G, 4G, WIFI | 38/49/13 |

### Search

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 11 | `city` | enum | Delhi, Bengaluru, Gurugram, Mumbai, Jaipur | |
| 12 | `search_lat` | float | 12.9–28.8 | Jittered around the nearest site |
| 13 | `search_lon` | float | 72.8–77.6 | |
| 14 | `search_radius_km` | float | 3, 5, 10, 15, 25 | User-selected |
| 15 | `filter_connector` | enum | CCS2, TYPE2_AC, CHAdeMO, ANY | |
| 16 | `filter_fast_only` | bool | TRUE, FALSE | ~42% TRUE |
| 17 | `results_returned` | int | 0–5 | 0 means a coverage gap |
| 18 | `nearest_site_id` | string | site id, or `NONE` | `NONE` only when 0 results |
| 19 | `nearest_distance_km` | float | 0.4 – search_radius | 0.0 when 0 results |

### Site in context

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 20 | `selected_site_id` | string | site id, or `NONE` | `NONE` on SEARCH rows |
| 21 | `site_category` | enum | MALL, HIGHWAY, OFFICE, RESIDENTIAL, NONE | |
| 22 | `available_bays` | int | 0–5 | Free bays right now |
| 23 | `total_bays` | int | 4–5 | Bays at that site |
| 24 | `price_per_kwh` | float | 12.0–21.0 | INR per kWh |
| 25 | `estimated_wait_min` | int | 0–75 | Minutes until a bay frees |

**A bay** is one parking space with one charger, where one car plugs in.
Elsewhere in the industry this is called a port, stall, charge point or EVSE.

### Vehicle

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 26 | `vehicle_soc_pct` | float | 4.0–88.0 | Battery percentage |
| 27 | `range_left_km` | float | 11–370 | Derived from SoC |

### Reservation and outcome

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 28 | `reservation_id` | string | `RSV_#######` or `NONE` | |
| 29 | `reservation_window_min` | int | 0, 15, 20, 30, 45 | 0 when no reservation |
| 30 | `converted_to_charge` | bool | TRUE, FALSE | TRUE only on START_CHARGE |

### Context and labels

| # | Column | Type | Values / Range | Notes |
|---|---|---|---|---|
| 31 | `response_time_ms` | int | 60–4000 | App API latency |
| 32 | `day_part` | enum | MORNING, AFTERNOON, EVENING, NIGHT | |
| 33 | `is_weekend` | bool | TRUE, FALSE | Saturday and Sunday |
| 34 | `is_anomaly` | bool | TRUE, FALSE | Ground-truth label |

---

## 4. Density rules

**No blank cells anywhere.** Where a value does not apply, a sentinel is used
rather than leaving the field empty:

| Situation | Field | Sentinel |
|---|---|---|
| SEARCH row (nothing selected yet) | `selected_site_id`, `site_category` | `NONE` |
| No reservation made | `reservation_id` | `NONE` |
| No reservation made | `reservation_window_min` | `0` |
| Zero search results | `nearest_site_id` | `NONE` |
| Zero search results | `nearest_distance_km` | `0.0` |
| Zero search results | bay and price fields | `0` |

Sentinels are used because the file must load cleanly into MongoDB, Kafka and
pandas without null handling at every step. **Filter on `results_returned > 0`
before averaging any bay or price column**, or the zero-result rows will drag
the averages down.

---

## 5. Behavioural rules

These are what make the funnel worth analysing rather than just counting.

### 5.1 Urgency from battery level

```
soc > 40%          urgency = 1.00
20% < soc <= 40%   urgency = 1.25
soc <= 20%         urgency = 1.55
```

Urgency multiplies the probability of advancing at every stage. A driver with
15% battery is far less likely to abandon than one with 70%.

**Observed effect:** conversion is 58% for drivers under 20% battery, against
28% for those above 60%.

### 5.2 Progression probabilities

| Transition | Base probability | Modifiers |
|---|---|---|
| SEARCH → VIEW_STATION | 0.74 | × urgency^0.4; skipped entirely if 0 results |
| VIEW_STATION → NAVIGATE | 0.70 | × urgency; × 0.32 if no free bays; × 0.55 if wait > 25 min; × 0.74 if wait 11–25; × 0.90 if wait 1–10; × 0.88 if price > 20 |
| NAVIGATE → RESERVE | 0.34 subscriber / 0.09 free | × 1.6 if ≤ 1 bay free |
| NAVIGATE → START_CHARGE | 0.80 reserved / 0.63 not | × 0.30 if no free bays and unreserved; × urgency^0.5 |
| NAVIGATE → ABANDON | — | 40% of non-converting journeys where a bay *was* free |

All probabilities are capped below 1.0 so no transition is guaranteed.

### 5.3 Availability model

Free bays are drawn from a binomial distribution whose occupancy rate follows
the hour of day, modified by site category:

| Category | Modifier |
|---|---|
| OFFICE | × 1.35 during 09:00–18:00, × 0.35 otherwise |
| RESIDENTIAL | × 1.5 during 19:00–07:00, × 0.3 otherwise |
| HIGHWAY | × 1.2 at all hours |
| MALL | no modifier |

### 5.4 Wait time model

`estimated_wait_min` follows from scarcity, because a free bay can be claimed
before the driver arrives:

| Free bays | Wait |
|---|---|
| 0 | Exponential(18) + 6, capped 5–75 min |
| 1 | Exponential(6) + 2, capped 2–30 min |
| 2 | 0 with probability 0.55, else 2–11 min |
| 3+ | 0 with probability 0.82, else 1–5 min |

**Observed:** 50.6% of rows carry a positive wait; mean of the positives is
9.9 minutes.

### 5.5 Demand timing

Hourly arrival weights, peaking at 18:00 and troughing at 03:00:

```
hour   0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20 21 22 23
weight 1  1  1  1  2  4  7 10 12 11 10 10 11 11 10 10 12 15 18 16 11  7  4  2
```

Weekends are scaled by 0.78. City shares: Delhi 28%, Mumbai 22%, Gurugram 18%,
Jaipur 18%, Bengaluru 14%.

### 5.6 Coverage gaps

4.5% of searches return zero results, representing areas outside the network's
coverage. These journeys terminate immediately and are labelled
`is_anomaly = TRUE`.

---

## 6. Invariants

Assert these before trusting the file.

| # | Rule |
|---|---|
| 1 | No nulls and no empty strings in any cell |
| 2 | `event_id` is unique across all rows |
| 3 | `journey_step` starts at 1 and increments by 1 within each `journey_id`, with no gaps |
| 4 | Every journey has exactly one `SEARCH`, and it is step 1 |
| 5 | Journey-constant fields (Section 2.2) never vary within a `journey_id` |
| 6 | `event_time` increases monotonically within a journey |
| 7 | `available_bays` ≤ `total_bays` always |
| 8 | `converted_to_charge` is TRUE only on `START_CHARGE` rows |
| 9 | `reservation_id` is non-`NONE` only on `RESERVE` and later rows of the same journey |
| 10 | `reservation_window_min` > 0 if and only if `reservation_id` ≠ `NONE` |
| 11 | `selected_site_id` is `NONE` on `SEARCH` rows and a real site on all others |
| 12 | `results_returned` = 0 implies `nearest_site_id` = `NONE` and journey length 1 |
| 13 | `nearest_distance_km` ≤ `search_radius_km` when results exist |
| 14 | `estimated_wait_min` = 0 whenever `available_bays` ≥ 3, except a 18% minority |
| 15 | `site_category` matches the category of `selected_site_id` |
| 16 | `is_weekend` matches the weekday of the journey's **first** event (see note below) |
| 17 | `day_part` matches the hour of `event_time` |
| 18 | Every `ABANDON` row is preceded by a `NAVIGATE` in the same journey |

**Note on invariant 16.** `is_weekend` is a journey-level attribute, fixed when
the search begins, so it describes when the user started looking rather than
when each individual step occurred. Six rows (0.019%) belong to journeys that
began before midnight and finished after it, so their `is_weekend` disagrees
with their own `event_time`. This is intended and is required for consistency
with invariant 5. Derive the weekday from `event_time` directly if you need
per-row accuracy.

All 18 invariants were verified against the shipped file; see
`validate_app_events.py`.

### Expected distributions

| Metric | Expected |
|---|---|
| SEARCH → VIEW_STATION | 74–78% |
| SEARCH → NAVIGATE | 52–57% |
| SEARCH → START_CHARGE | 37–41% |
| Reservation rate among navigators | 18–20% |
| Positive `estimated_wait_min` | 48–53% |
| Zero-result searches | 4–5% |
| Conversion when 0 bays free | 33–38% |
| Conversion when wait > 25 min | 45–50% |
| Conversion when SoC < 20% | 56–60% |
| Conversion when reserved | 84–88% |

---

## 7. Kafka configuration

| Setting | Value |
|---|---|
| Topic | `app-events` |
| Partitions | 3 |
| Replication factor | 1 |
| Retention | 72 hours |
| Value format | JSON |
| Default key | `city` |
| Alternative key | `journey_id` |

**Key choice matters.** `city` gives 5 distinct keys, which is convenient for
city-level aggregation but distributes unevenly across 3 partitions.

`journey_id` gives 11,000 keys, spreads evenly, and keeps every step of one
journey in the same partition and in order — which a funnel consumer needs, or
it must join across partitions to reconstruct a journey.

**Type casting.** The producer casts numeric and boolean columns before
sending, so consumers receive `4` rather than `"4"` and `false` rather than
`"FALSE"`. Without this, a comparison like `wait > 25` silently fails on
strings.

---

## 8. Known limitations

State these rather than letting an examiner find them.

1. **Fully synthetic.** No real app telemetry was used. Parameters come from
   published EV charging industry figures, not from an actual operator.
2. **Availability is sampled, not simulated.** Free-bay counts are drawn from a
   distribution per event rather than tracked as a continuous state, so two
   events seconds apart at the same site can disagree slightly.
3. **Not joined to the session dataset.** `user_id` values here do not
   correspond to those in `ev_charging_dataset.csv`. The two are separate
   simulations.
4. **Only five cities and ten sites**, chosen to keep the data legible.
5. **No returning users.** Each journey draws a fresh `user_id`, so repeat-usage
   and loyalty analysis is not possible.
6. **`is_anomaly` marks only zero-result searches and abandonments**, not a
   general anomaly label.
7. **Coordinates are approximate**, jittered around city centres, and should not
   be used for real routing.

---

## 9. Reproducing the file

```bash
pip install numpy pandas scipy
python generate_app_events.py
```

Seed is fixed at 7, so the output is identical on every run. To vary the data,
change `SEED` at the top. To change the size, edit `JOURNEYS` (default 11,000)
or `DAYS` (default 14).
