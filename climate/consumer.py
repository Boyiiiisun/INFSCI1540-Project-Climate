#!/usr/bin/env python3
"""
Consume NORMAL_DLY raw Kafka records and upsert them into ODB.

Kafka topic:
  NORMAL_DLY_RAW

Target MySQL table:
  odb.climate_normal_daily_raw

Environment variables:
  KAFKA_BROKER
  KAFKA_TOPIC
  ODB_HOST
  ODB_PORT
  ODB_USER
  ODB_PASSWORD
  ODB_DATABASE
"""

import json
import os
import sys
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import mysql.connector
from kafka import KafkaConsumer
from mysql.connector import Error

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "NORMAL_DLY_RAW")

ODB_HOST = os.getenv("ODB_HOST", "localhost")
ODB_PORT = int(os.getenv("ODB_PORT", "13306"))
ODB_USER = os.getenv("ODB_USER", "deuser")
ODB_PASSWORD = os.getenv("ODB_PASSWORD", "depassword")
ODB_DATABASE = os.getenv("ODB_DATABASE", "odb")


def json_or_none(raw: Optional[bytes]) -> Any:
    if raw is None:
        return None
    text = raw.decode("utf-8").strip()
    if not text:
        return None
    return json.loads(text)


def parse_date_yyyymmdd(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def main() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=json_or_none,
    )

    conn = None
    cursor = None

    upsert_sql = """
    INSERT INTO climate_normal_daily_raw (
        station_id,
        station_name,
        elevation,
        latitude,
        longitude,
        norm_date,
        dly_tmin_normal,
        dly_tmax_normal,
        mtd_prcp_normal
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        station_name = VALUES(station_name),
        elevation = VALUES(elevation),
        latitude = VALUES(latitude),
        longitude = VALUES(longitude),
        dly_tmin_normal = VALUES(dly_tmin_normal),
        dly_tmax_normal = VALUES(dly_tmax_normal),
        mtd_prcp_normal = VALUES(mtd_prcp_normal)
    """

    try:
        conn = mysql.connector.connect(
            host=ODB_HOST,
            port=ODB_PORT,
            database=ODB_DATABASE,
            user=ODB_USER,
            password=ODB_PASSWORD,
        )
        cursor = conn.cursor()

        print(f"Waiting for raw NORMAL_DLY messages on topic '{KAFKA_TOPIC}'...")

        for message in consumer:
            data = message.value
            if not isinstance(data, dict):
                print(f"Skipped malformed message: {data}")
                continue

            station_id = data.get("station_id")
            station_name = data.get("station_name")
            elevation = to_decimal(data.get("elevation"))
            latitude = to_decimal(data.get("latitude"))
            longitude = to_decimal(data.get("longitude"))
            norm_date = parse_date_yyyymmdd(data.get("norm_date"))
            dly_tmin_normal = to_decimal(data.get("dly_tmin_normal"))
            dly_tmax_normal = to_decimal(data.get("dly_tmax_normal"))
            mtd_prcp_normal = to_decimal(data.get("mtd_prcp_normal"))

            required = [station_id, station_name, norm_date]
            if any(x is None for x in required):
                print(f"Skipped malformed message: {data}")
                continue

            values = (
                str(station_id),
                str(station_name),
                elevation,
                latitude,
                longitude,
                norm_date,
                dly_tmin_normal,
                dly_tmax_normal,
                mtd_prcp_normal,
            )

            cursor.execute(upsert_sql, values)
            conn.commit()
            print(f"Upserted ODB row: {values}")

    except Error as e:
        print(f"MySQL error while loading ODB raw data: {e}", file=sys.stderr)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
