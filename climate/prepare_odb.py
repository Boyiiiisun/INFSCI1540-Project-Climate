#!/usr/bin/env python3
"""
Prepare the ODB database for the NOAA NORMAL_DLY pipeline.

Default connection:
  host=localhost
  port=13306
  user=deuser
  password=depassword

You can override with environment variables:
  ODB_HOST, ODB_PORT, ODB_USER, ODB_PASSWORD, ODB_DATABASE
"""

import os
import sys
import mysql.connector
from mysql.connector import Error

HOST = os.getenv("ODB_HOST", "localhost")
PORT = int(os.getenv("ODB_PORT", "13306"))
USER = os.getenv("ODB_USER", "root")
PASSWORD = os.getenv("ODB_PASSWORD", "secret")
DATABASE = os.getenv("ODB_DATABASE", "odb")

CREATE_DATABASE_SQL = f"CREATE DATABASE IF NOT EXISTS `{DATABASE}`"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS climate_normal_daily_raw (
    norm_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(30) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    elevation DECIMAL(8,2) NULL,
    latitude DECIMAL(10,6) NULL,
    longitude DECIMAL(10,6) NULL,
    norm_date DATE NOT NULL,
    dly_tmin_normal DECIMAL(7,2) NULL,
    dly_tmax_normal DECIMAL(7,2) NULL,
    mtd_prcp_normal DECIMAL(10,2) NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_station_normdate UNIQUE (station_id, norm_date),
    INDEX idx_norm_date (norm_date)
)
"""

def prepare_odb() -> None:
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

        print(f"Connected to MySQL ODB server at {HOST}:{PORT} as {USER}")
        cursor.execute(CREATE_DATABASE_SQL)
        cursor.execute(f"USE `{DATABASE}`")
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()

        print(f"ODB database '{DATABASE}' is ready.")
        print("Table created/verified: climate_normal_daily_raw")

    except Error as e:
        print(f"MySQL error while preparing ODB: {e}", file=sys.stderr)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    prepare_odb()
