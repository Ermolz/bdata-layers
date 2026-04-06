# Local Bronze Ingest

Simple local ingest script for generating demo data for multiple domains and storing it only in the `bronze` layer with date/hour partitioning.

## What it does

- Generates demo data for:
  - `traffic`
  - `weather`
  - `alerts`
  - `roads`
- Writes data only to local `bronze`
- Partitions output by:
  - `ingest_date=YYYY-MM-DD`
  - `hour=HH`
- Can run locally for two hours and gradually write part of the data during that time

## Repository structure

```text
.
|-- ingest_bronze.py
|-- requirements.txt
|-- .gitignore
`-- README.md
```

## Output structure

By default, files are written to `data_lake/bronze/...`

Example:

```text
data_lake/
`-- bronze/
    `-- domain=traffic/
        `-- source=local_traffic_simulator/
            `-- ingest_date=2026-04-06/
                `-- hour=14/
                    `-- part-20260406T140000Z-00001.jsonl
```

## Requirements

- Python 3.10+

No third-party packages are required.

## Run

Default run:

```bash
python ingest_bronze.py
```

This will:

- write data into `data_lake`
- generate all supported data types
- run for `2` hours
- create a new batch every `5` minutes

## Useful commands

Run a single iteration:

```bash
python ingest_bronze.py --iterations 1
```

Write to a custom directory:

```bash
python ingest_bronze.py --output-root storage
```

Run only selected data types:

```bash
python ingest_bronze.py --data-types traffic weather
```

Change interval and batch size:

```bash
python ingest_bronze.py --interval-seconds 60 --batch-size 10
```

Start from a fixed UTC datetime:

```bash
python ingest_bronze.py --start-datetime 2026-04-06T12:00:00Z
```

## Data format

Each output file is written as JSON Lines (`.jsonl`), one record per line.

Example record:

```json
{
  "ingest_ts": "2026-04-06T12:00:00Z",
  "source": "local_traffic_simulator",
  "domain": "traffic",
  "schema_v": 1,
  "event_id": "traffic-uuid",
  "payload": {
    "road_id": "M01",
    "speed_kmh": 42.3,
    "congestion_level": "medium",
    "confidence": 0.91,
    "lat": 50.45123,
    "lon": 30.52345,
    "event_time": "2026-04-06T12:00:00Z"
  }
}
```

## Notes

- The script currently generates demo data locally.
- It does not upload data to cloud storage.
- Generated output is ignored by Git via `.gitignore`.
