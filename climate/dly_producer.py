#!/usr/bin/env python3
"""
Publish NORMAL_DLY CSV records to Kafka as JSON.

Defaults:
  file  = NORMAL_DLY_sample_csv.csv
  topic = NORMAL_DLY_RAW
  broker= localhost:29092

Environment variables:
  INPUT_CSV
  KAFKA_TOPIC
  KAFKA_BROKER
  SLEEP_SECONDS
"""

import csv
import json
import os
import time
from typing import Optional

from kafka import KafkaProducer

INPUT_CSV = os.getenv("INPUT_CSV", "NORMAL_DLY_sample_csv.csv")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "NORMAL_DLY_RAW")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0"))


def to_float(value: str) -> Optional[float]:
    value = (value or "").strip()
    if value == "":
        return None
    return float(value)


def to_int(value: str) -> Optional[int]:
    value = (value or "").strip()
    if value == "":
        return None
    return int(float(value))


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    count = 0
    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            record = {
                "station_id": row.get("STATION"),
                "station_name": row.get("STATION_NAME"),
                "elevation": to_float(row.get("ELEVATION", "")),
                "latitude": to_float(row.get("LATITUDE", "")),
                "longitude": to_float(row.get("LONGITUDE", "")),
                "norm_date": str(row.get("DATE", "")).strip(),
                "dly_tmin_normal": to_int(row.get("DLY-TMIN-NORMAL", "")),
                "dly_tmax_normal": to_int(row.get("DLY-TMAX-NORMAL", "")),
                "mtd_prcp_normal": to_int(row.get("MTD-PRCP-NORMAL", "")),
            }

            producer.send(KAFKA_TOPIC, value=record)
            count += 1
            print(f"Produced record {count}: {record}")

            if SLEEP_SECONDS > 0:
                time.sleep(SLEEP_SECONDS)

    producer.flush()
    print(f"Done. Sent {count} records to topic '{KAFKA_TOPIC}'.")


if __name__ == "__main__":
    main()
