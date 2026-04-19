#!/usr/bin/env python3
"""
Prepare the DW database for the NOAA NORMAL_DLY pipeline.

Default connection:
  host=localhost
  port=23306
  user=deuser
  password=depassword

You can override with environment variables:
  DW_HOST, DW_PORT, DW_USER, DW_PASSWORD, DW_DATABASE
"""

import os
import sys
import mysql.connector
from mysql.connector import Error

HOST = os.getenv("DW_HOST", "localhost")
PORT = int(os.getenv("DW_PORT", "23306"))
USER = os.getenv("DW_USER", "deuser")
PASSWORD = os.getenv("DW_PASSWORD", "depassword")
DATABASE = os.getenv("DW_DATABASE", "dw")

CREATE_DATABASE_SQL = f"CREATE DATABASE IF NOT EXISTS `{DATABASE}`"

CREATE_DIM_DATE_SQL = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT NOT NULL PRIMARY KEY,
    full_date DATE NOT NULL,
    day_num TINYINT NOT NULL,
    month_num TINYINT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter_num TINYINT NOT NULL,
    year_num INT NOT NULL,
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date)
)
"""

CREATE_DIM_STATION_SQL = """
CREATE TABLE IF NOT EXISTS dim_station (
    station_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(30) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    elevation DECIMAL(8,2) NULL,
    latitude DECIMAL(10,6) NULL,
    longitude DECIMAL(10,6) NULL,
    CONSTRAINT uq_dim_station_station_id UNIQUE (station_id)
)
"""

CREATE_FACT_SQL = """
CREATE TABLE IF NOT EXISTS fact_daily_climate_normal (
    fact_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    station_key INT NOT NULL,
    dly_tmin_normal DECIMAL(7,2) NULL,
    dly_tmax_normal DECIMAL(7,2) NULL,
    mtd_prcp_normal DECIMAL(10,2) NULL,
    CONSTRAINT uq_fact_station_date UNIQUE (station_key, date_key),
    CONSTRAINT fk_fact_daily_normal_date
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_daily_normal_station
        FOREIGN KEY (station_key) REFERENCES dim_station(station_key)
)
"""

CREATE_MONTHLY_SUMMARY_SQL = """
CREATE TABLE IF NOT EXISTS monthly_climate_normal_summary (
    summary_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(30) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    norm_month CHAR(6) NOT NULL,
    year_num INT NOT NULL,
    month_num INT NOT NULL,
    num_days INT NULL,
    avg_dly_tmin_normal DECIMAL(7,2) NULL,
    avg_dly_tmax_normal DECIMAL(7,2) NULL,
    month_end_prcp_normal DECIMAL(10,2) NULL,
    CONSTRAINT uq_monthly_normal_station_month UNIQUE (station_id, norm_month)
)
"""

def prepare_dw() -> None:
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
        )
        cursor = conn.cursor()

        print(f"Connected to MySQL DW server at {HOST}:{PORT} as {USER}")
        cursor.execute(CREATE_DATABASE_SQL)
        cursor.execute(f"USE `{DATABASE}`")
        cursor.execute(CREATE_DIM_DATE_SQL)
        cursor.execute(CREATE_DIM_STATION_SQL)
        cursor.execute(CREATE_FACT_SQL)
        cursor.execute(CREATE_MONTHLY_SUMMARY_SQL)
        conn.commit()

        print(f"DW database '{DATABASE}' is ready.")
        print("Tables created/verified:")
        print("  - dim_date")
        print("  - dim_station")
        print("  - fact_daily_climate_normal")
        print("  - monthly_climate_normal_summary")

    except Error as e:
        print(f"MySQL error while preparing DW: {e}", file=sys.stderr)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    prepare_dw()
