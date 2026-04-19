#!/usr/bin/env python3
"""
Load DW dimensions and fact table from ODB raw NORMAL_DLY data.

Source:
  odb.climate_normal_daily_raw

Targets:
  dw.dim_date
  dw.dim_station
  dw.fact_daily_climate_normal

This script can be rerun safely. It uses INSERT IGNORE and
ON DUPLICATE KEY UPDATE where appropriate.

Environment variables:
  ODB_HOST, ODB_PORT, ODB_USER, ODB_PASSWORD, ODB_DATABASE
  DW_HOST,  DW_PORT,  DW_USER,  DW_PASSWORD,  DW_DATABASE
"""

import os
import sys
from collections import OrderedDict
from decimal import Decimal
from typing import Any

import mysql.connector
from mysql.connector import Error

ODB_HOST = os.getenv("ODB_HOST", "localhost")
ODB_PORT = int(os.getenv("ODB_PORT", "13306"))
ODB_USER = os.getenv("ODB_USER", "root")
ODB_PASSWORD = os.getenv("ODB_PASSWORD", "secret")
ODB_DATABASE = os.getenv("ODB_DATABASE", "odb")

DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT = int(os.getenv("DW_PORT", "23306"))
DW_USER = os.getenv("DW_USER", "root")
DW_PASSWORD = os.getenv("DW_PASSWORD", "secret")
DW_DATABASE = os.getenv("DW_DATABASE", "dw")


def normalize_decimal(value: Any):
    if value is None:
        return None
    return Decimal(str(value))


def main() -> None:
    odb_conn = None
    dw_conn = None
    odb_cursor = None
    dw_cursor = None

    try:
        odb_conn = mysql.connector.connect(
            host=ODB_HOST,
            port=ODB_PORT,
            database=ODB_DATABASE,
            user=ODB_USER,
            password=ODB_PASSWORD,
        )
        dw_conn = mysql.connector.connect(
            host=DW_HOST,
            port=DW_PORT,
            database=DW_DATABASE,
            user=DW_USER,
            password=DW_PASSWORD,
        )

        odb_cursor = odb_conn.cursor(dictionary=True)
        dw_cursor = dw_conn.cursor()

        odb_cursor.execute("""
            SELECT
                station_id,
                station_name,
                elevation,
                latitude,
                longitude,
                norm_date,
                dly_tmin_normal,
                dly_tmax_normal,
                mtd_prcp_normal
            FROM climate_normal_daily_raw
            ORDER BY station_id, norm_date
        """)
        rows = odb_cursor.fetchall()
        print(f"Read {len(rows)} rows from ODB.")

        if not rows:
            print("ODB table is empty. Nothing to load into DW.")
            return

        # Load dim_station
        station_map = OrderedDict()
        for r in rows:
            sid = r["station_id"]
            if sid not in station_map:
                station_map[sid] = (
                    r["station_id"],
                    r["station_name"],
                    normalize_decimal(r["elevation"]),
                    normalize_decimal(r["latitude"]),
                    normalize_decimal(r["longitude"]),
                )

        station_sql = """
        INSERT INTO dim_station (
            station_id, station_name, elevation, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            station_name = VALUES(station_name),
            elevation = VALUES(elevation),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude)
        """
        for values in station_map.values():
            dw_cursor.execute(station_sql, values)
        dw_conn.commit()
        print(f"Upserted {len(station_map)} station rows into dim_station.")

        # Load dim_date
        date_map = OrderedDict()
        for r in rows:
            d = r["norm_date"]
            date_key = int(d.strftime("%Y%m%d"))
            if date_key not in date_map:
                date_map[date_key] = (
                    date_key,
                    d,
                    d.day,
                    d.month,
                    d.strftime("%B"),
                    ((d.month - 1) // 3) + 1,
                    d.year,
                )

        date_sql = """
        INSERT IGNORE INTO dim_date (
            date_key, full_date, day_num, month_num, month_name, quarter_num, year_num
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for values in date_map.values():
            dw_cursor.execute(date_sql, values)
        dw_conn.commit()
        print(f"Inserted/verified {len(date_map)} date rows into dim_date.")

        # Build station lookup
        dw_cursor.execute("SELECT station_key, station_id FROM dim_station")
        station_lookup = {sid: skey for skey, sid in dw_cursor.fetchall()}

        # Load fact table
        fact_sql = """
        INSERT INTO fact_daily_climate_normal (
            date_key, station_key, dly_tmin_normal, dly_tmax_normal, mtd_prcp_normal
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            dly_tmin_normal = VALUES(dly_tmin_normal),
            dly_tmax_normal = VALUES(dly_tmax_normal),
            mtd_prcp_normal = VALUES(mtd_prcp_normal)
        """

        fact_count = 0
        for r in rows:
            d = r["norm_date"]
            date_key = int(d.strftime("%Y%m%d"))
            station_key = station_lookup[r["station_id"]]
            values = (
                date_key,
                station_key,
                normalize_decimal(r["dly_tmin_normal"]),
                normalize_decimal(r["dly_tmax_normal"]),
                normalize_decimal(r["mtd_prcp_normal"]),
            )
            dw_cursor.execute(fact_sql, values)
            fact_count += 1

        dw_conn.commit()
        print(f"Upserted {fact_count} rows into fact_daily_climate_normal.")

    except Error as e:
        print(f"MySQL error while loading DW from ODB: {e}", file=sys.stderr)
        raise
    finally:
        if odb_cursor is not None:
            odb_cursor.close()
        if dw_cursor is not None:
            dw_cursor.close()
        if odb_conn is not None and odb_conn.is_connected():
            odb_conn.close()
        if dw_conn is not None and dw_conn.is_connected():
            dw_conn.close()


if __name__ == "__main__":
    main()

