#!/usr/bin/env python3
import argparse
import json
import uuid
from pathlib import Path

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        input_file_name,
        lit,
        regexp_extract,
        to_timestamp,
        to_utc_timestamp,
    )
except ModuleNotFoundError as exc:
    raise SystemExit(
        "PySpark is required for the Silver Layer pipeline. "
        "Install dependencies with: py -m pip install -r requirements.txt"
    ) from exc


AUTHOR_SURNAME = "Yermolovych"
DOMAIN = "weather"
SCHEMA_VERSION = 1
DEFAULT_SOURCES = ("open_meteo_api", "open_meteo_archive_api")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("local-bronze-to-silver-weather")
        .master("local[*]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def bronze_glob(project_root: Path, source: str) -> str:
    return str(
        project_root
        / "data"
        / "bronze"
        / f"domain={DOMAIN}"
        / f"source={source}"
        / "ingest_date=*"
        / "hour=*"
        / "*.jsonl"
    )


def discover_bronze_files(project_root: Path, source: str) -> list[str]:
    bronze_root = (
        project_root
        / "data"
        / "bronze"
        / f"domain={DOMAIN}"
        / f"source={source}"
    )
    files = sorted(bronze_root.glob("ingest_date=*/hour=*/*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No bronze JSONL files found under {bronze_root}")
    return ["file:///" + path.resolve().as_posix() for path in files]


def silver_root(project_root: Path, source: str, batch_id: str) -> Path:
    return (
        project_root
        / "data"
        / "processed"
        / "silver"
        / f"domain={DOMAIN}"
        / f"source={source}"
        / f"schema_v={SCHEMA_VERSION}"
        / f"batch_id={batch_id}"
    )


def spark_path(path: Path) -> str:
    return "file:///" + path.resolve().as_posix()


def read_bronze(spark: SparkSession, project_root: Path, source: str) -> DataFrame:
    bronze_files = discover_bronze_files(project_root, source)
    return (
        spark.read.option("mode", "PERMISSIVE")
        .json(bronze_files)
        .withColumn("file_path", input_file_name())
        .withColumn("ingest_date", regexp_extract(col("file_path"), r"ingest_date=([0-9\-]+)", 1))
        .withColumn("hour", regexp_extract(col("file_path"), r"hour=([0-9]{2})", 1))
    )


def normalize_weather(bronze_df: DataFrame, source: str, batch_id: str) -> DataFrame:
    event_ts_local = to_timestamp(col("payload.event_time"), "yyyy-MM-dd'T'HH:mm")

    return (
        bronze_df.select(
            col("metadata.author_surname").alias("author_surname"),
            col("domain").alias("domain"),
            col("source").alias("source"),
            to_timestamp(col("ingest_ts")).alias("ingest_ts"),
            to_utc_timestamp(event_ts_local, "Europe/Kyiv").alias("event_ts"),
            col("payload.city").cast("string").alias("city"),
            col("payload.latitude").cast("double").alias("latitude"),
            col("payload.longitude").cast("double").alias("longitude"),
            col("payload.elevation").cast("double").alias("elevation"),
            col("payload.timezone").cast("string").alias("timezone"),
            col("payload.timezone_abbreviation").cast("string").alias("timezone_abbreviation"),
            col("payload.temperature_2m").cast("double").alias("temperature_2m"),
            col("payload.relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
            col("payload.precipitation").cast("double").alias("precipitation"),
            col("payload.wind_speed_10m").cast("double").alias("wind_speed_10m"),
            col("ingest_date"),
            col("hour"),
            col("file_path"),
        )
        .withColumn("schema_v", lit(SCHEMA_VERSION))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("processed_author_surname", lit(AUTHOR_SURNAME))
        .withColumn("processed_source", lit(source))
    )


def valid_weather(df: DataFrame) -> DataFrame:
    return df.filter(
        col("author_surname").isNotNull()
        & col("domain").isNotNull()
        & col("source").isNotNull()
        & col("ingest_ts").isNotNull()
        & col("event_ts").isNotNull()
        & col("city").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("temperature_2m").isNotNull()
        & col("relative_humidity_2m").isNotNull()
        & col("precipitation").isNotNull()
        & col("wind_speed_10m").isNotNull()
        & col("ingest_date").isNotNull()
        & col("hour").isNotNull()
        & (col("author_surname") == AUTHOR_SURNAME)
        & (col("domain") == DOMAIN)
        & (col("source") == col("processed_source"))
        & (col("latitude").between(-90.0, 90.0))
        & (col("longitude").between(-180.0, 180.0))
        & (col("temperature_2m").between(-100.0, 100.0))
        & (col("relative_humidity_2m").between(0.0, 100.0))
        & (col("precipitation") >= 0.0)
        & (col("wind_speed_10m") >= 0.0)
        & (col("event_ts") <= current_timestamp())
    )


def process_source(spark: SparkSession, project_root: Path, source: str, output_format: str) -> None:
    batch_id = str(uuid.uuid4())
    print(f"Processing source={source}, batch_id={batch_id}")

    bronze_df = read_bronze(spark, project_root, source)
    normalized_df = normalize_weather(bronze_df, source, batch_id)
    valid_df = valid_weather(normalized_df)

    total_count = normalized_df.count()
    valid_count = valid_df.count()
    rejected_count = total_count - valid_count

    print(f"Rows total: {total_count}")
    print(f"Rows valid: {valid_count}")
    print(f"Rows rejected: {rejected_count}")

    if valid_count == 0:
        print(f"SKIPPED: source={source} has no valid rows")
        return

    output_path = silver_root(project_root, source, batch_id)
    repartitioned_df = valid_df.repartition(8)
    if output_format == "parquet":
        (
            repartitioned_df.write.mode("overwrite")
            .partitionBy("ingest_date", "hour")
            .parquet(spark_path(output_path))
        )
    else:
        write_jsonl_locally(repartitioned_df, output_path, batch_id)
    print(f"WRITTEN: {output_path}")


def write_jsonl_locally(df: DataFrame, output_path: Path, batch_id: str) -> None:
    writers = {}
    try:
        for row in df.toLocalIterator():
            record = row.asDict(recursive=True)
            ingest_date = record.pop("ingest_date")
            hour = record.pop("hour")
            partition_dir = output_path / f"ingest_date={ingest_date}" / f"hour={hour}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            file_path = partition_dir / f"part-{batch_id}.jsonl"
            if file_path not in writers:
                writers[file_path] = file_path.open("a", encoding="utf-8")
            writers[file_path].write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    finally:
        for writer in writers.values():
            writer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local bronze-to-silver pipeline for weather data")
    parser.add_argument("--project-root", default=".", help="Project root with data/bronze and data/processed")
    parser.add_argument("--sources", nargs="+", default=list(DEFAULT_SOURCES), choices=list(DEFAULT_SOURCES))
    parser.add_argument("--output-format", choices=("jsonl", "parquet"), default="jsonl")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    spark = build_spark()
    try:
        for source in args.sources:
            process_source(spark, project_root, source, args.output_format)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
