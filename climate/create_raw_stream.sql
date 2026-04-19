SET 'auto.offset.reset' = 'earliest';

CREATE STREAM normal_dly_raw (
  station_id VARCHAR,
  station_name VARCHAR,
  elevation DOUBLE,
  latitude DOUBLE,
  longitude DOUBLE,
  norm_date VARCHAR,
  dly_tmin_normal INTEGER,
  dly_tmax_normal INTEGER,
  mtd_prcp_normal INTEGER
) WITH (
  KAFKA_TOPIC = 'NORMAL_DLY_RAW',
  VALUE_FORMAT = 'JSON',
  PARTITIONS = 1,
  REPLICAS = 1
);
