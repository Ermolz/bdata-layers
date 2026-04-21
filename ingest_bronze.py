#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import io
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, Tuple


AUTHOR_SURNAME = "Yermolovych"
DEFAULT_TYPES = ("traffic", "weather", "alerts", "roads")
STATIC_TYPES = {"alerts", "roads"}
KYIV_BBOX = (50.213273, 30.239440, 50.590798, 30.825941)
TOP_10_CITIES = (
    {"city": "Kyiv", "latitude": 50.4501, "longitude": 30.5234},
    {"city": "Kharkiv", "latitude": 49.9935, "longitude": 36.2304},
    {"city": "Odesa", "latitude": 46.4825, "longitude": 30.7233},
    {"city": "Dnipro", "latitude": 48.4647, "longitude": 35.0462},
    {"city": "Donetsk", "latitude": 48.0159, "longitude": 37.8029},
    {"city": "Zaporizhzhia", "latitude": 47.8388, "longitude": 35.1396},
    {"city": "Lviv", "latitude": 49.8397, "longitude": 24.0297},
    {"city": "Kryvyi Rih", "latitude": 47.9105, "longitude": 33.3918},
    {"city": "Mykolaiv", "latitude": 46.9750, "longitude": 31.9946},
    {"city": "Mariupol", "latitude": 47.0971, "longitude": 37.5434},
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stamp(event_time: dt.datetime) -> str:
    return event_time.strftime("%Y%m%dT%H%M%SZ")


def build_partition_dir(output_root: Path, domain: str, source: str, event_time: dt.datetime) -> Path:
    return (
        output_root
        / "bronze"
        / f"domain={domain}"
        / f"source={source}"
        / f"ingest_date={event_time.strftime('%Y-%m-%d')}"
        / f"hour={event_time.strftime('%H')}"
    )


def write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def request_url(url: str, timeout: int = 60) -> Tuple[bytes, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "local-bronze-ingest/1.0",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        return body, content_type


def load_json_from_url(url: str, timeout: int = 60) -> Dict:
    body, _ = request_url(url, timeout=timeout)
    return json.loads(body.decode("utf-8"))


def request_overpass(query: str, timeout: int = 120) -> Tuple[bytes, str]:
    body = urllib.parse.urlencode({"data": query}).encode("utf-8")
    request = urllib.request.Request(
        "https://overpass-api.de/api/interpreter",
        data=body,
        headers={
            "User-Agent": "local-bronze-ingest/1.0",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = response.read()
        content_type = response.headers.get("Content-Type", "application/json")
        return data, content_type


def build_weather_url() -> str:
    latitudes = ",".join(str(item["latitude"]) for item in TOP_10_CITIES)
    longitudes = ",".join(str(item["longitude"]) for item in TOP_10_CITIES)
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "Europe/Kyiv",
    }
    return "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)


def build_weather_archive_url(start_date: str, end_date: str) -> str:
    latitudes = ",".join(str(item["latitude"]) for item in TOP_10_CITIES)
    longitudes = ",".join(str(item["longitude"]) for item in TOP_10_CITIES)
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "Europe/Kyiv",
    }
    return "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)


def build_roads_query() -> str:
    south, west, north, east = KYIV_BBOX
    return f"""
[out:json][timeout:90];
(
  way["highway"]({south},{west},{north},{east});
);
out body;
>;
out skel qt;
""".strip()


def load_alerts_csv_as_jsonl(csv_bytes: bytes, fetched_at: dt.datetime) -> str:
    decoded = csv_bytes.decode("utf-8")
    reader = csv.DictReader(io.StringIO(decoded))
    lines = []
    for row in reader:
        lines.append(
            json.dumps(
                {
                    "ingest_ts": iso_z(fetched_at),
                    "source": "vadimkin_air_raid_dataset",
                    "domain": "alerts",
                    "metadata": {
                        "author_surname": AUTHOR_SURNAME,
                    },
                    "payload": row,
                },
                ensure_ascii=False,
            )
        )
    return "\n".join(lines) + ("\n" if lines else "")


def resolve_alerts_csv_url() -> str:
    tree_url = "https://api.github.com/repos/Vadimkin/ukrainian-air-raid-sirens-dataset/git/trees/main?recursive=1"
    payload = load_json_from_url(tree_url)
    paths = [item["path"] for item in payload.get("tree", []) if item.get("type") == "blob"]

    preferred_suffixes = (
        "official_data_en.csv",
        "official_en.csv",
    )
    for suffix in preferred_suffixes:
        for path in paths:
            if path.endswith(suffix):
                return f"https://raw.githubusercontent.com/Vadimkin/ukrainian-air-raid-sirens-dataset/main/{path}"

    for path in paths:
        if path.startswith("datasets/") and "official" in path and path.endswith(".csv"):
            return f"https://raw.githubusercontent.com/Vadimkin/ukrainian-air-raid-sirens-dataset/main/{path}"

    raise RuntimeError("Could not resolve alerts CSV URL from GitHub repository tree")


def wrap_json_payload(raw_bytes: bytes, domain: str, source: str, fetched_at: dt.datetime) -> str:
    payload = json.loads(raw_bytes.decode("utf-8"))
    record = {
        "ingest_ts": iso_z(fetched_at),
        "source": source,
        "domain": domain,
        "metadata": {
            "author_surname": AUTHOR_SURNAME,
        },
        "payload": payload,
    }
    return json.dumps(record, ensure_ascii=False, indent=2) + "\n"


def wrap_weather_payload(raw_bytes: bytes, fetched_at: dt.datetime) -> str:
    payload = json.loads(raw_bytes.decode("utf-8"))
    payload_items = payload if isinstance(payload, list) else [payload]
    records = []
    for index, city in enumerate(TOP_10_CITIES):
        item = payload_items[index] if index < len(payload_items) else {}
        current_block = item.get("current", {})
        current_units = item.get("current_units", {})
        records.append(
            {
                "ingest_ts": iso_z(fetched_at),
                "source": "open_meteo_api",
                "domain": "weather",
                "metadata": {
                    "author_surname": AUTHOR_SURNAME,
                },
                "payload": {
                    "city": city["city"],
                    "latitude": item.get("latitude", city["latitude"]),
                    "longitude": item.get("longitude", city["longitude"]),
                    "elevation": item.get("elevation"),
                    "timezone": item.get("timezone"),
                    "timezone_abbreviation": item.get("timezone_abbreviation"),
                    "temperature_2m": current_block.get("temperature_2m"),
                    "relative_humidity_2m": current_block.get("relative_humidity_2m"),
                    "precipitation": current_block.get("precipitation"),
                    "wind_speed_10m": current_block.get("wind_speed_10m"),
                    "event_time": current_block.get("time"),
                    "units": current_units,
                },
            }
        )
    return "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"


def wrap_weather_archive_payload(raw_bytes: bytes, fetched_at: dt.datetime) -> str:
    payload = json.loads(raw_bytes.decode("utf-8"))
    payload_items = payload if isinstance(payload, list) else [payload]
    lines = []
    for index, city in enumerate(TOP_10_CITIES):
        item = payload_items[index] if index < len(payload_items) else {}
        hourly = item.get("hourly", {})
        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidity_values = hourly.get("relative_humidity_2m", [])
        precipitation_values = hourly.get("precipitation", [])
        wind_values = hourly.get("wind_speed_10m", [])
        for hour_index, event_time in enumerate(times):
            lines.append(
                json.dumps(
                    {
                        "ingest_ts": iso_z(fetched_at),
                        "source": "open_meteo_archive_api",
                        "domain": "weather",
                        "metadata": {
                            "author_surname": AUTHOR_SURNAME,
                        },
                        "payload": {
                            "city": city["city"],
                            "latitude": item.get("latitude", city["latitude"]),
                            "longitude": item.get("longitude", city["longitude"]),
                            "elevation": item.get("elevation"),
                            "timezone": item.get("timezone"),
                            "timezone_abbreviation": item.get("timezone_abbreviation"),
                            "temperature_2m": temperatures[hour_index] if hour_index < len(temperatures) else None,
                            "relative_humidity_2m": humidity_values[hour_index] if hour_index < len(humidity_values) else None,
                            "precipitation": precipitation_values[hour_index] if hour_index < len(precipitation_values) else None,
                            "wind_speed_10m": wind_values[hour_index] if hour_index < len(wind_values) else None,
                            "event_time": event_time,
                        },
                    },
                    ensure_ascii=False,
                )
            )
    return "\n".join(lines) + ("\n" if lines else "")


def write_metadata(path: Path, domain: str, source: str, url: str, fetched_at: dt.datetime, content_type: str) -> None:
    metadata = {
        "author_surname": AUTHOR_SURNAME,
        "domain": domain,
        "source": source,
        "source_url": url,
        "fetched_at": iso_z(fetched_at),
        "content_type": content_type,
        "stored_file": path.name,
    }
    write_text(path.with_suffix(path.suffix + ".meta.json"), json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")


def fetch_weather(output_root: Path, event_time: dt.datetime) -> Path:
    source = "open_meteo_api"
    url = build_weather_url()
    body, content_type = request_url(url)
    target_dir = build_partition_dir(output_root, "weather", source, event_time)
    output_file = target_dir / f"part-{stamp(event_time)}.jsonl"
    write_text(output_file, wrap_weather_payload(body, event_time))
    write_metadata(output_file, "weather", source, url, event_time, content_type)
    return output_file


def fetch_weather_archive(output_root: Path, event_time: dt.datetime, days: int) -> Path:
    source = "open_meteo_archive_api"
    end_date = event_time.date()
    start_date = end_date - dt.timedelta(days=max(days - 1, 0))
    url = build_weather_archive_url(start_date.isoformat(), end_date.isoformat())
    body, content_type = request_url(url, timeout=180)
    target_dir = build_partition_dir(output_root, "weather", source, event_time)
    output_file = target_dir / f"part-{stamp(event_time)}-history-{days}d.jsonl"
    write_text(output_file, wrap_weather_archive_payload(body, event_time))
    write_metadata(output_file, "weather", source, url, event_time, content_type)
    return output_file


def fetch_alerts(output_root: Path, event_time: dt.datetime) -> Path:
    source = "vadimkin_air_raid_dataset"
    url = resolve_alerts_csv_url()
    body, content_type = request_url(url)
    target_dir = build_partition_dir(output_root, "alerts", source, event_time)
    output_file = target_dir / f"part-{stamp(event_time)}.jsonl"
    write_text(output_file, load_alerts_csv_as_jsonl(body, event_time))
    write_metadata(output_file, "alerts", source, url, event_time, content_type)
    return output_file


def fetch_roads(output_root: Path, event_time: dt.datetime) -> Path:
    source = "overpass_osm_kyiv"
    query = build_roads_query()
    body, content_type = request_overpass(query)
    target_dir = build_partition_dir(output_root, "roads", source, event_time)
    output_file = target_dir / f"part-{stamp(event_time)}.json"
    write_text(output_file, wrap_json_payload(body, "roads", source, event_time))
    write_metadata(output_file, "roads", source, "https://overpass-api.de/api/interpreter", event_time, content_type)
    return output_file


def fetch_traffic(output_root: Path, event_time: dt.datetime) -> Path:
    source = "lviv_gtfs_rt_vehicle_positions"
    url = "https://track.ua-gis.com/gtfs/lviv/vehicle_position"
    body, content_type = request_url(url)
    target_dir = build_partition_dir(output_root, "traffic", source, event_time)
    output_file = target_dir / f"part-{stamp(event_time)}.pb"
    write_bytes(output_file, body)
    write_metadata(output_file, "traffic", source, url, event_time, content_type)
    return output_file


def fetch_one(domain: str, output_root: Path, event_time: dt.datetime) -> Path:
    handlers = {
        "traffic": fetch_traffic,
        "weather": fetch_weather,
        "alerts": fetch_alerts,
        "roads": fetch_roads,
    }
    return handlers[domain](output_root, event_time)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download local bronze data from public internet sources")
    parser.add_argument("--output-root", default="data", help="Root directory for local data")
    parser.add_argument("--data-types", nargs="+", default=list(DEFAULT_TYPES), choices=list(DEFAULT_TYPES))
    parser.add_argument("--interval-seconds", type=int, default=300, help="Pause between realtime fetches")
    parser.add_argument("--duration-hours", type=float, default=2.0, help="Total runtime")
    parser.add_argument("--iterations", type=int, default=0, help="Fixed number of realtime iterations")
    parser.add_argument("--include-static-every-iteration", action="store_true", help="Re-fetch alerts and roads every iteration")
    parser.add_argument("--weather-history-days", type=int, default=0, help="Download bulk hourly weather history for the last N days")
    parser.add_argument("--bulk-only", action="store_true", help="Run only bulk downloads and skip the realtime loop")
    return parser.parse_args()


def should_fetch(domain: str, iteration_no: int, include_static_every_iteration: bool) -> bool:
    if domain not in STATIC_TYPES:
        return True
    return include_static_every_iteration or iteration_no == 1


def run_ingest(args: argparse.Namespace) -> None:
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    if args.weather_history_days > 0:
        event_time = utc_now()
        output_file = fetch_weather_archive(output_root, event_time, args.weather_history_days)
        print(f"DOWNLOADED BULK: weather -> {output_file}")
        if args.bulk_only:
            return

    started_at = time.monotonic()
    max_seconds = max(args.duration_hours, 0) * 3600
    iteration_no = 1

    while True:
        if args.iterations and iteration_no > args.iterations:
            break
        if not args.iterations and iteration_no > 1 and max_seconds and (time.monotonic() - started_at) >= max_seconds:
            break

        event_time = utc_now()
        for domain in args.data_types:
            if not should_fetch(domain, iteration_no, args.include_static_every_iteration):
                continue
            try:
                output_file = fetch_one(domain, output_root, event_time)
                print(f"DOWNLOADED: {domain} -> {output_file}")
            except (TimeoutError, urllib.error.URLError, urllib.error.HTTPError) as exc:
                print(f"FAILED: {domain} -> {exc}")

        iteration_no += 1
        if args.iterations and iteration_no > args.iterations:
            break
        if args.interval_seconds > 0:
            time.sleep(args.interval_seconds)


def main() -> None:
    run_ingest(parse_args())


if __name__ == "__main__":
    main()
