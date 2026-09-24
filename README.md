# EV Charging Network — Real-Time Streaming & Risk Analytics

Real-time streaming pipeline for an EV charging network across 5 Indian cities.
Simulates driver app journeys, charger sessions, and station telemetry, streams
them through Kafka, scores charging risk live, and persists results to MySQL for
a Grafana dashboard.

## Architecture

```
generate_*.py / *_data_generator.py
        │  (CSV datasets)
        ▼
app_producer.py ──► Kafka: app-events ──┐
charging_producer.py ─► Kafka: charging-telemetry ─┼─► stream_processor.py ─► Kafka: system-alerts
station_producer.py ──► Kafka: station-telemetry ──┘        │
                                                    risk_engine.py   MySQL (EV-Stations) ─► Grafana
```

| Component | Consumes | Produces / Writes |
|---|---|---|
| `app_producer.py` | `app_events.csv` | Kafka `app-events` (key: `city` or `journey_id`) |
| `charging_producer.py` | `charging_sessions.csv` | Kafka `charging-telemetry` (key: `selected_site_id`) |
| `station_producer.py` | `station_telemetry.csv` | Kafka `station-telemetry` (key: `selected_site_id`) |
| `stream_processor.py` | all 3 topics | Kafka `system-alerts` + MySQL tables |

## Datasets

| File | Rows | Description | Generator |
|---|---|---|---|
| `app_events.csv` | 31,264 (11,000 journeys) | Mobile-app funnel: `SEARCH → VIEW_STATION → NAVIGATE → RESERVE → START_CHARGE / ABANDON` | `generate_app_events.py` (seed 7) |
| `charging_sessions.csv` | 4,265 | One session per `START_CHARGE`; energy, power, expected vs actual duration, delay ratio | `charging_data_generator.py` (seed 42) |
| `station_telemetry.csv` | 40,270 | 5-min station snapshots: bays, queue, wait, utilization, incidents | `station_data_generator.py` (seed 42, `--interval 5`) |

Window: 3–16 Aug 2026 (14 days). Sites: 10 across Delhi, Mumbai, Gurugram, Jaipur, Bengaluru.
Full schema, funnel rules, and the 18 data invariants are documented in
[`app_events_rules.md`](app_events_rules.md). Validate with `validate_app_events.py`.

## Risk Engine (`risk_engine.py`)

Multiplicative score — operational problems compound each other:

```
risk = delay_score × congestion_factor × wait_surge_factor × urgency_factor
```

- **Delay:** `actual / expected` duration → discrete score 0–4
- **Congestion:** free/total bays → factor 1.0–2.5
- **Wait surge:** current wait / rolling baseline wait (per station × day-part × weekend) → factor 1.0–2.5
- **Urgency:** low SoC (<20%) + range-vs-distance pressure → factor 1.0–2.0

Levels: `NORMAL (≤2)` · `WATCH (≤5)` · `WARNING (≤9)` · `CRITICAL (>9)`.
An alert fires only on genuine delay (`ratio > 1.25`) **plus** supporting context
(low availability, wait surge, or low SoC) to avoid alert spam.

## Quickstart

### 1. Install

```bash
pip install -r requirements.txt
pip install numpy pandas scipy   # for generators / validation
```

You also need a Kafka broker at `localhost:9092`:

```bash
docker compose up -d   # or your own broker
```

### 2. (Optional) Regenerate data

CSVs are committed, so this is optional. Seeds are fixed → reproducible.

```bash
python generate_app_events.py        # → app_events.csv
python charging_data_generator.py    # → charging_sessions.csv
python station_data_generator.py     # → station_telemetry.csv (use --interval 5)
python validate_app_events.py        # checks all 18 invariants + distributions
```

### 3. Create Kafka topics

```bash
python app_producer.py --create-topic
python charging_producer.py --create-topic
python station_producer.py --create-topic
```

Each topic: 3 partitions, replication factor 1, 72 h retention.

### 4. Stream data (each in its own terminal)

```bash
python app_producer.py --speed 200
python charging_producer.py --speed 200
python station_producer.py --speed 200
```

Useful flags (all three producers): `--dry-run` (no Kafka needed),
`--burst` (no pacing), `--limit N` (smoke test), `--loop` (replay forever),
`--quiet`, `--speed 5000` (≈14 days in ~4 min).
`app_producer.py` additionally supports `--key journey_id` (keeps a whole
journey in one partition — required for funnel consumers) and `--json`.

### 5. Run the stream processor

```bash
python stream_processor.py --mysql-host localhost --mysql-user root --mysql-password root --mysql-database EV-Stations
```

Writes `charging_sessions`, `station_telemetry`, `charging_risk_events` to
MySQL and publishes every risk evaluation to `system-alerts`.

> **Note:** `stream_processor.py` imports `mysql_writer` and
> `import_dashboard.py` references `grafana/dashboard_ev_risk.json` —
> both are expected alongside this repo for the MySQL/Grafana stage.

### 6. Grafana dashboard

```bash
python import_dashboard.py   # pushes dashboard to http://localhost:3000 (admin:admin)
```

## Kafka Topics

| Topic | Key | Value |
|---|---|---|
| `app-events` | `city` (default) or `journey_id` | JSON, typed (ints/floats/bools, not strings) |
| `charging-telemetry` | `selected_site_id` | JSON |
| `station-telemetry` | `selected_site_id` | JSON |
| `system-alerts` | `selected_site_id` | Risk result JSON |

## Repository Structure

```
├── generate_app_events.py       # app funnel generator (seed 7)
├── charging_data_generator.py   # charging session generator
├── station_data_generator.py    # station telemetry generator
├── app_events.csv / charging_sessions.csv / station_telemetry.csv
├── app_events_rules.md          # full data dictionary + rules
├── app_producer.py / charging_producer.py / station_producer.py
├── stream_processor.py          # Kafka consumer → risk + MySQL + alerts
├── risk_engine.py               # delay × congestion × surge × urgency scoring
├── validate_app_events.py       # 18 invariants + distribution checks
├── import_dashboard.py          # Grafana dashboard importer
└── requirements.txt             # pandas, kafka-python
```

## Known Limitations

- Fully synthetic — no real telemetry; parameters from published industry figures.
- Station availability is sampled per event, not tracked as continuous state.
- App `user_id`s do not join to charging sessions; each journey draws a fresh user.
- 5 cities / 10 sites only; coordinates are jittered approximations.
- `is_anomaly` flags only zero-result searches and abandonments.

## Requirements

- Python 3.9+
- `pandas`, `kafka-python` (+ `numpy`, `scipy` for generators)
- Kafka broker on `localhost:9092`, MySQL + Grafana for the full pipeline
