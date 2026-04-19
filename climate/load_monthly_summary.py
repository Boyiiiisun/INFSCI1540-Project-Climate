#!/usr/bin/env python3
"""
Consume monthly NORMAL_DLY summaries from Kafka and upsert them into MySQL DW.

Default Kafka topic:
  NORMAL_DLY_MONTHLY_SUMMARY

This loader is intentionally flexible:
- it can read key fields from either the Kafka key or the value
- it accepts upper/lower-case field names
- it supports either AS_VALUE(...) output columns or plain grouped-key output

Environment variables:
  KAFKA_BROKER
  KAFKA_TOPIC
  DW_HOST
  DW_PORT
  DW_USER
  DW_PASSWORD
  DW_DATABASE
"""

import json
import os
import sys
from decimal import Decimal
from typing import Any, Iterable, Optional

import mysql.connector
from kafka import KafkaConsumer
from mysql.connector import Error

BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "NORMAL_DLY_MONTHLY_SUMMARY")

DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT = int(os.getenv("DW_PORT", "23306"))
DW_USER = os.getenv("DW_USER", "deuser")
DW_PASSWORD = os.getenv("DW_PASSWORD", "depassword")
DW_DATABASE = os.getenv("DW_DATABASE", "dw")


def json_or_none(raw: Optional[bytes]) -> Any:
    if raw is None:
        return None
    text = raw.decode("utf-8").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def pick(source: Any, *names: Iterable[str]) -> Any:
    if not isinstance(source, dict):
        return None
    for name in names:
        if name in source:
            return source[name]
    return None


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=json_or_none,
        value_deserializer=json_or_none,
    )

    conn = None
    cursor = None

    upsert_sql = """
    INSERT INTO monthly_climate_normal_summary (
        station_id,
        station_name,
        norm_month,
        year_num,
        month_num,
        num_days,
        avg_dly_tmin_normal,
        avg_dly_tmax_normal,
        month_end_prcp_normal
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        station_name = VALUES(station_name),
        year_num = VALUES(year_num),
        month_num = VALUES(month_num),
        num_days = VALUES(num_days),
        avg_dly_tmin_normal = VALUES(avg_dly_tmin_normal),
        avg_dly_tmax_normal = VALUES(avg_dly_tmax_normal),
        month_end_prcp_normal = VALUES(month_end_prcp_normal)
    """

    try:
        conn = mysql.connector.connect(
            host=DW_HOST,
            port=DW_PORT,
            database=DW_DATABASE,
            user=DW_USER,
            password=DW_PASSWORD,
        )
        cursor = conn.cursor()

        print(f"Waiting for monthly summary messages on topic '{TOPIC}'...")

        for message in consumer:
            key_data = message.key if isinstance(message.key, dict) else {}
            value_data = message.value if isinstance(message.value, dict) else {}

            # Skip tombstones
            if message.value is None:
                continue

            station_id = (
                pick(value_data, "STATION_ID_V", "station_id_v", "STATION_ID", "station_id")
                or pick(key_data, "STATION_ID", "station_id")
            )
            station_name = (
                pick(value_data, "STATION_NAME_V", "station_name_v", "STATION_NAME", "station_name")
                or pick(key_data, "STATION_NAME", "station_name")
            )
            norm_month = (
                pick(value_data, "NORM_MONTH_V", "norm_month_v", "NORM_MONTH", "norm_month",
                     "OBS_MONTH_V", "obs_month_v", "OBS_MONTH", "obs_month")
                or pick(key_data, "NORM_MONTH", "norm_month", "OBS_MONTH", "obs_month")
            )
            year_num = (
                pick(value_data, "YEAR_NUM_V", "year_num_v", "YEAR_NUM", "year_num")
                or pick(key_data, "YEAR_NUM", "year_num")
            )
            month_num = (
                pick(value_data, "MONTH_NUM_V", "month_num_v", "MONTH_NUM", "month_num")
                or pick(key_data, "MONTH_NUM", "month_num")
            )

            num_days = pick(value_data, "NUM_DAYS", "num_days", "COUNT", "count")
            avg_dly_tmin_normal = pick(
                value_data,
                "AVG_DLY_TMIN_NORMAL", "avg_dly_tmin_normal",
                "AVG_TMIN_NORMAL", "avg_tmin_normal",
                "AVG_TMIN", "avg_tmin"
            )
            avg_dly_tmax_normal = pick(
                value_data,
                "AVG_DLY_TMAX_NORMAL", "avg_dly_tmax_normal",
                "AVG_TMAX_NORMAL", "avg_tmax_normal",
                "AVG_TMAX", "avg_tmax"
            )
            month_end_prcp_normal = pick(
                value_data,
                "MONTH_END_PRCP_NORMAL", "month_end_prcp_normal",
                "MAX_MTD_PRCP_NORMAL", "max_mtd_prcp_normal",
                "MAX_PRCP_NORMAL", "max_prcp_normal",
                "MTD_PRCP_NORMAL", "mtd_prcp_normal"
            )

            required = [station_id, station_name, norm_month, year_num, month_num]
            if any(x is None for x in required):
                print(f"Skipped malformed message: key={key_data}, value={value_data}")
                continue

            values = (
                str(station_id),
                str(station_name),
                str(norm_month),
                int(year_num),
                int(month_num),
                int(num_days) if num_days is not None else None,
                to_decimal(avg_dly_tmin_normal),
                to_decimal(avg_dly_tmax_normal),
                to_decimal(month_end_prcp_normal),
            )

            cursor.execute(upsert_sql, values)
            conn.commit()
            print(f"Upserted monthly summary: {values}")

    except Error as e:
        print(f"MySQL error while loading monthly summary: {e}", file=sys.stderr)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
