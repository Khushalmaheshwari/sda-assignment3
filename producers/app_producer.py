"""
Producer — streams app_events.csv into the Kafka topic `app-events`.

Setup
-----
    pip install kafka-python
    docker compose up -d                     # or your own Kafka broker
    python app_events_producer.py --create-topic

Run
---
    python app_events_producer.py --dry-run          # no Kafka needed, checks everything
    python app_events_producer.py                    # 200x speed
    python app_events_producer.py --speed 5000       # 14 days in ~4 minutes
    python app_events_producer.py --burst            # no pacing, load as fast as possible
    python app_events_producer.py --limit 500        # short smoke test
    python app_events_producer.py --key journey_id   # key by journey instead of city
    python app_events_producer.py --loop             # replay continuously

Partition key
-------------
Default is `city`. Kafka preserves ordering only within a partition and routes
by key, so this keeps one city's events together and in order.

Use `--key journey_id` when the consumer does funnel analysis: every step of a
user's journey (SEARCH -> VIEW_STATION -> NAVIGATE -> START_CHARGE) then lands
in the same partition, so the consumer can follow a journey without joining
across partitions. City-level aggregation then spans partitions instead.
"""
import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# --------------------------------------------------------------------------
# Version-tolerant imports.
# kafka-python moved several names between 2.x and 3.x — NoBrokersAvailable no
# longer exists in 3.x, for example — so resolve them defensively rather than
# importing names that may be absent.
# --------------------------------------------------------------------------
KAFKA_READY = True
KAFKA_IMPORT_ERROR = None
try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka import errors as kerr
except Exception as exc:                     # ImportError, or a broken install
    KAFKA_READY = False
    KAFKA_IMPORT_ERROR = exc

if KAFKA_READY:
    TopicExists = getattr(kerr, "TopicAlreadyExistsError", None)
    _candidates = ("NoBrokersAvailable", "KafkaConnectionError",
                   "MetadataEmptyBrokerList", "KafkaTimeoutError")
    CONNECT_ERRORS = tuple(
        c for c in (getattr(kerr, n, None) for n in _candidates)
        if isinstance(c, type) and issubclass(c, BaseException)
    ) or (Exception,)
else:
    TopicExists, CONNECT_ERRORS = None, (Exception,)


# --------------------------------------------------------------------------
# Locate the CSV. Checks the producers/ folder, then the project data/
# folder, then the working directory, so it works whether you run it from
# the repo root or from inside producers/.
# --------------------------------------------------------------------------
CSV_NAME = "app_events.csv"
TOPIC = "app-events"
BOOTSTRAP = "localhost:9092"
PARTITIONS = 3
RETENTION_HOURS = 72


def find_csv():
    try:
        here = Path(__file__).resolve().parent
    except NameError:                        # notebook / REPL
        here = Path.cwd()
    project_root = here.parent
    candidates = (
        here / CSV_NAME,
        project_root / "data" / CSV_NAME,
        Path.cwd() / CSV_NAME,
        Path.cwd() / "data" / CSV_NAME,
    )
    for path in candidates:
        if path.exists():
            return path
    sys.exit(
        f"Could not find {CSV_NAME}.\n"
        f"  looked in: {here}\n"
        f"             {project_root / 'data'}\n"
        f"             {Path.cwd()}\n"
        f"Run from the repo root, or keep the CSV in data/."
    )


INT_COLS = {"journey_step", "results_returned", "available_bays", "total_bays",
            "estimated_wait_min", "reservation_window_min", "response_time_ms"}
FLOAT_COLS = {"search_lat", "search_lon", "search_radius_km",
              "nearest_distance_km", "price_per_kwh", "vehicle_soc_pct",
              "range_left_km"}
BOOL_COLS = {"filter_fast_only", "converted_to_charge", "is_weekend",
             "is_anomaly"}


def typed(row, line_no):
    """CSV yields strings; cast to real JSON types so consumers need no casting."""
    out = {}
    for k, v in row.items():
        if k is None:
            continue
        v = (v or "").strip()
        try:
            if k in INT_COLS:
                out[k] = int(float(v))
            elif k in FLOAT_COLS:
                out[k] = float(v)
            elif k in BOOL_COLS:
                out[k] = v.upper() in ("TRUE", "1", "YES")
            else:
                out[k] = v
        except ValueError:
            sys.exit(f"Bad value on CSV line {line_no}: {k}={v!r}")
    return out


def load(key_field, limit=None):
    path = find_csv()
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            sys.exit(f"{path} is empty")
        if key_field not in reader.fieldnames:
            sys.exit(f"--key '{key_field}' is not a column.\n"
                     f"Available: {', '.join(reader.fieldnames)}")
        rows = [typed(r, i) for i, r in enumerate(reader, start=2)]
    if not rows:
        sys.exit(f"{path} has a header but no rows")
    rows.sort(key=lambda r: r["event_time"])
    return (rows[:limit] if limit else rows), path


def fmt(r):
    """One readable line per message, so the stream is visible as it flows."""
    site = r["selected_site_id"] if r["selected_site_id"] != "NONE" \
        else r["nearest_site_id"]
    bays = f'{r["available_bays"]}/{r["total_bays"]}'
    wait = f'{r["estimated_wait_min"]}m' if r["estimated_wait_min"] else "-"
    if r["converted_to_charge"]:
        tag = "  <- CHARGED"
    elif r["action"] == "ABANDON":
        tag = "  <- abandoned"
    elif r["results_returned"] == 0:
        tag = "  <- no results"
    else:
        tag = ""
    return (f'{r["event_time"][11:]:<10}{r["city"]:<11}{r["action"]:<14}'
            f'{r["user_id"]:<12}{site:<15}{bays:>5}{wait:>7}{tag}')


HEADER = (f'{"time":<10}{"city":<11}{"action":<14}{"user":<12}'
          f'{"site":<15}{"bays":>5}{"wait":>7}')


def _timeouts(cls, seconds=8):
    """
    Fail fast on a dead broker instead of retrying for minutes.

    Config names differ between kafka-python 2.x and 3.x, so only pass the ones
    the installed version actually accepts.
    """
    wanted = {
        "bootstrap_timeout_ms": seconds * 1000,        # 3.x
        "api_version_auto_timeout_ms": seconds * 1000,  # 2.x
        "request_timeout_ms": seconds * 1000,
        "max_block_ms": seconds * 1000,
    }
    allowed = getattr(cls, "DEFAULT_CONFIG", {})
    return {k: v for k, v in wanted.items() if k in allowed}


def require_kafka():
    if not KAFKA_READY:
        sys.exit(
            f"Could not import kafka-python.\n"
            f"  underlying error: {type(KAFKA_IMPORT_ERROR).__name__}: "
            f"{KAFKA_IMPORT_ERROR}\n"
            f"  fix: pip install --upgrade kafka-python\n"
            f"  (or run with --dry-run to test without Kafka)"
        )


def create_topic():
    require_kafka()
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP,
                                 **_timeouts(KafkaAdminClient))
    except Exception as exc:
        sys.exit(f"Could not connect to Kafka at {BOOTSTRAP}\n"
                 f"  {type(exc).__name__}: {exc}\n"
                 f"  start a broker with:  docker compose up -d")
    try:
        admin.create_topics([NewTopic(
            name=TOPIC,
            num_partitions=PARTITIONS,
            replication_factor=1,
            topic_configs={"retention.ms": str(RETENTION_HOURS * 3600 * 1000)},
        )])
        print(f"created  {TOPIC}  partitions={PARTITIONS}  "
              f"retention={RETENTION_HOURS}h")
    except Exception as exc:
        if TopicExists and isinstance(exc, TopicExists):
            print(f"exists   {TOPIC}")
        elif "TopicAlreadyExists" in type(exc).__name__:
            print(f"exists   {TOPIC}")
        else:
            raise
    finally:
        admin.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--create-topic", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and pace without sending; needs no Kafka")
    ap.add_argument("--speed", type=float, default=200.0,
                    help="simulated seconds per real second (default 200)")
    ap.add_argument("--key", default="city", help="partition key column")
    ap.add_argument("--burst", action="store_true", help="send with no pacing")
    ap.add_argument("--limit", type=int, help="stop after N messages")
    ap.add_argument("--loop", action="store_true", help="replay continuously")
    ap.add_argument("--quiet", action="store_true",
                    help="show a running count instead of each message")
    ap.add_argument("--json", action="store_true",
                    help="print the raw JSON payload of each message")
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    args = ap.parse_args()

    if args.create_topic:
        create_topic()
        return

    rows, path = load(args.key, args.limit)
    span = (datetime.fromisoformat(rows[-1]["event_time"])
            - datetime.fromisoformat(rows[0]["event_time"])).total_seconds()

    print(f"source    : {path}")
    print(f"topic     : {TOPIC}")
    print(f"key       : {args.key}  "
          f"({len({str(r[args.key]) for r in rows})} distinct)")
    print(f"messages  : {len(rows):,}")
    print(f"window    : {rows[0]['event_time']}  ->  {rows[-1]['event_time']}")
    print("pacing    : " + ("burst (no delay)" if args.burst else
                            f"{args.speed:g}x  (~{span/args.speed/60:.1f} min per pass)"))
    if args.dry_run:
        print("mode      : DRY RUN — nothing will be sent")
    print()
    if not args.quiet and not args.json:
        print(HEADER)
        print("-" * 74)

    producer = None
    if not args.dry_run:
        require_kafka()
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap,
                acks="all",
                linger_ms=20,
                retries=3,
                **_timeouts(KafkaProducer),
            )
        except Exception as exc:
            sys.exit(f"Could not connect to Kafka at {args.bootstrap}\n"
                     f"  {type(exc).__name__}: {exc}\n"
                     f"  start a broker with:  docker compose up -d\n"
                     f"  or test this script without Kafka:  --dry-run")

    sent = errors = 0
    by_action = {}
    wall = time.time()

    def on_error(exc):
        nonlocal errors
        errors += 1
        if errors <= 3:
            print(f"  send failed: {exc}")

    try:
        while True:
            t0 = datetime.fromisoformat(rows[0]["event_time"])
            pass_start = time.time()

            for r in rows:
                if not args.burst:
                    sim = (datetime.fromisoformat(r["event_time"]) - t0).total_seconds()
                    drift = sim / args.speed - (time.time() - pass_start)
                    if drift > 0:
                        time.sleep(drift)

                if producer is not None:
                    producer.send(
                        TOPIC,
                        key=str(r[args.key]).encode(),
                        value=json.dumps(r).encode(),
                    ).add_errback(on_error)
                sent += 1
                by_action[r["action"]] = by_action.get(r["action"], 0) + 1

                if args.quiet:
                    if sent % 1000 == 0:
                        rate = sent / max(time.time() - wall, 1e-6)
                        print(f"  {sent:7,} sent   {r['event_time']}   "
                              f"{rate:6.0f} msg/s", flush=True)
                elif args.json:
                    print(json.dumps(r), flush=True)
                else:
                    print(fmt(r), flush=True)

            if not args.loop:
                break
            print("  --- pass complete, looping ---")

    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        secs = max(time.time() - wall, 1e-6)
        print(f"\n{'parsed' if args.dry_run else 'sent'} {sent:,} messages "
              f"in {secs:.1f}s ({sent/secs:.0f}/s), {errors} errors")
        for a in sorted(by_action, key=by_action.get, reverse=True):
            print(f"  {a:14s} {by_action[a]:7,}")


if __name__ == "__main__":
    main()
