#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import random
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, List


DEFAULT_TYPES = ("traffic", "weather", "alerts", "roads")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_partition_dir(output_root: Path, domain: str, source: str, event_time: dt.datetime) -> Path:
    ingest_date = event_time.strftime("%Y-%m-%d")
    hour = event_time.strftime("%H")
    return (
        output_root
        / "bronze"
        / f"domain={domain}"
        / f"source={source}"
        / f"ingest_date={ingest_date}"
        / f"hour={hour}"
    )


def make_record(domain: str, source: str, payload: Dict, event_time: dt.datetime) -> Dict:
    return {
        "ingest_ts": iso_z(utc_now()),
        "source": source,
        "domain": domain,
        "schema_v": 1,
        "event_id": f"{domain}-{uuid.uuid4()}",
        "payload": {
            **payload,
            "event_time": iso_z(event_time),
        },
    }


def build_traffic_records(event_time: dt.datetime, batch_size: int) -> List[Dict]:
    roads = ("M01", "M03", "M05", "P46")
    base_lat = 50.4501
    base_lon = 30.5234
    records: List[Dict] = []
    for _ in range(batch_size):
        payload = {
            "road_id": random.choice(roads),
            "speed_kmh": round(random.uniform(8.0, 75.0), 1),
            "congestion_level": random.choice(("low", "medium", "high")),
            "confidence": round(random.uniform(0.75, 0.99), 2),
            "lat": round(base_lat + random.uniform(-0.03, 0.03), 6),
            "lon": round(base_lon + random.uniform(-0.03, 0.03), 6),
        }
        records.append(make_record("traffic", "local_traffic_simulator", payload, event_time))
    return records


def build_weather_records(event_time: dt.datetime, batch_size: int) -> List[Dict]:
    cities = ("Kyiv", "Lviv", "Odesa", "Dnipro")
    coords = {
        "Kyiv": (50.4501, 30.5234),
        "Lviv": (49.8397, 24.0297),
        "Odesa": (46.4825, 30.7233),
        "Dnipro": (48.4647, 35.0462),
    }
    records: List[Dict] = []
    for _ in range(batch_size):
        city = random.choice(cities)
        lat, lon = coords[city]
        payload = {
            "city": city,
            "temp_c": round(random.uniform(-10.0, 32.0), 1),
            "feels_like_c": round(random.uniform(-15.0, 35.0), 1),
            "humidity_pct": random.randint(35, 98),
            "wind_mps": round(random.uniform(0.0, 18.0), 1),
            "precip_mm": round(random.uniform(0.0, 12.0), 1),
            "lat": lat,
            "lon": lon,
        }
        records.append(make_record("weather", "local_weather_simulator", payload, event_time))
    return records


def build_alert_records(event_time: dt.datetime, batch_size: int) -> List[Dict]:
    alert_types = ("accident", "road_work", "closure", "ice_warning")
    severities = ("low", "medium", "high")
    roads = ("M01", "M03", "M05", "P46")
    records: List[Dict] = []
    for _ in range(batch_size):
        alert_type = random.choice(alert_types)
        road_id = random.choice(roads)
        payload = {
            "type": alert_type,
            "severity": random.choice(severities),
            "road_id": road_id,
            "message": f"{alert_type} reported on road {road_id}",
        }
        records.append(make_record("alerts", "local_alerts_simulator", payload, event_time))
    return records


def build_road_records(event_time: dt.datetime, batch_size: int) -> List[Dict]:
    roads = (
        {"road_id": "M01", "name": "Kyiv-Chernihiv", "lanes": 4, "surface": "asphalt"},
        {"road_id": "M03", "name": "Kyiv-Kharkiv", "lanes": 4, "surface": "asphalt"},
        {"road_id": "M05", "name": "Kyiv-Odesa", "lanes": 4, "surface": "asphalt"},
        {"road_id": "P46", "name": "Kharkiv-Okhtyrka", "lanes": 2, "surface": "mixed"},
    )
    records: List[Dict] = []
    for index in range(batch_size):
        road = roads[index % len(roads)]
        payload = {
            **road,
            "status": random.choice(("open", "restricted", "maintenance")),
        }
        records.append(make_record("roads", "local_roads_snapshot", payload, event_time))
    return records


def build_records(data_type: str, event_time: dt.datetime, batch_size: int) -> List[Dict]:
    builders = {
        "traffic": build_traffic_records,
        "weather": build_weather_records,
        "alerts": build_alert_records,
        "roads": build_road_records,
    }
    if data_type not in builders:
        raise ValueError(f"Unsupported data type: {data_type}")
    return builders[data_type](event_time, batch_size)


def source_for_type(data_type: str) -> str:
    sources = {
        "traffic": "local_traffic_simulator",
        "weather": "local_weather_simulator",
        "alerts": "local_alerts_simulator",
        "roads": "local_roads_snapshot",
    }
    return sources[data_type]


def write_jsonl(path: Path, records: Iterable[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_file_name(batch_no: int, event_time: dt.datetime) -> str:
    stamp = event_time.strftime("%Y%m%dT%H%M%SZ")
    return f"part-{stamp}-{batch_no:05d}.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate local bronze-layer data partitions")
    parser.add_argument("--output-root", default="data_lake", help="Root directory for local bronze data")
    parser.add_argument("--data-types", nargs="+", default=list(DEFAULT_TYPES), choices=list(DEFAULT_TYPES))
    parser.add_argument("--batch-size", type=int, default=5, help="Records per type per iteration")
    parser.add_argument("--interval-seconds", type=int, default=300, help="Pause between iterations")
    parser.add_argument("--duration-hours", type=float, default=2.0, help="Total runtime for local ingest")
    parser.add_argument("--iterations", type=int, default=0, help="Optional fixed number of iterations")
    parser.add_argument("--start-datetime", help="UTC datetime in ISO format, defaults to now")
    return parser.parse_args()


def parse_start_datetime(raw_value: str | None) -> dt.datetime:
    if not raw_value:
        return utc_now()
    normalized = raw_value.replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def run_ingest(args: argparse.Namespace) -> None:
    output_root = Path(args.output_root)
    started_at = time.monotonic()
    max_seconds = max(args.duration_hours, 0) * 3600
    batch_no = 1
    event_time = parse_start_datetime(args.start_datetime)

    while True:
        if args.iterations and batch_no > args.iterations:
            break
        if not args.iterations and batch_no > 1 and max_seconds and (time.monotonic() - started_at) >= max_seconds:
            break

        for data_type in args.data_types:
            source = source_for_type(data_type)
            records = build_records(data_type, event_time, args.batch_size)
            partition_dir = build_partition_dir(output_root, data_type, source, event_time)
            output_file = partition_dir / build_file_name(batch_no, event_time)
            write_jsonl(output_file, records)
            print(f"WRITTEN: {output_file} ({len(records)} records)")

        batch_no += 1
        event_time += dt.timedelta(seconds=args.interval_seconds)
        if args.iterations and batch_no > args.iterations:
            break
        if args.interval_seconds > 0:
            time.sleep(args.interval_seconds)


def main() -> None:
    args = parse_args()
    run_ingest(args)


if __name__ == "__main__":
    main()
