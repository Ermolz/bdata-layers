# Local Bronze Ingest

Local ingest project that downloads real data from public internet sources and stores it only in the `bronze` layer under the local `data` folder.

## Sources

- `traffic`: Lviv public transport GTFS-Realtime vehicle positions feed from `track.ua-gis.com`
- `weather`: Open-Meteo API
- `alerts`: `official_data_en.csv` from the `Vadimkin/ukrainian-air-raid-sirens-dataset` GitHub repository
- `roads`: OpenStreetMap road data from Overpass API for Kyiv bounding box

## What the script does

- downloads data locally
- writes data only to `data/bronze`
- partitions by `ingest_date=YYYY-MM-DD/hour=HH`
- adds metadata with surname `Yermolovych`
- can run for two hours and keep fetching realtime sources during that time

## Repository structure

```text
.
|-- data/
|-- ingest_bronze.py
|-- requirements.txt
|-- .gitignore
`-- README.md
```

## Output structure

Example layout:

```text
data/
`-- bronze/
    |-- domain=traffic/
    |   `-- source=lviv_gtfs_rt_vehicle_positions/
    |       `-- ingest_date=2026-04-06/
    |           `-- hour=13/
    |               |-- part-20260406T132215Z.pb
    |               `-- part-20260406T132215Z.pb.meta.json
    |-- domain=weather/
    |-- domain=alerts/
    `-- domain=roads/
```

## File formats

- `traffic`: raw GTFS-Realtime protobuf file (`.pb`) plus metadata sidecar
- `weather`: wrapped JSON file (`.json`) plus metadata sidecar
- `alerts`: JSON Lines file (`.jsonl`) plus metadata sidecar
- `roads`: wrapped JSON file (`.json`) plus metadata sidecar

## Run

One quick iteration:

```bash
py ingest_bronze.py --iterations 1
```

Default run for two hours:

```bash
py ingest_bronze.py
```

Write to another root directory:

```bash
py ingest_bronze.py --output-root my_data
```

Fetch only some domains:

```bash
py ingest_bronze.py --data-types weather roads
```

Re-fetch static sources on every iteration too:

```bash
py ingest_bronze.py --include-static-every-iteration
```

## Notes

- `traffic` here is public transport realtime movement data, not car congestion speed API.
- `alerts` is pulled from a public GitHub dataset snapshot, because commonly used alert APIs usually require an API key.
- Generated files under `data/` are ignored by Git.
