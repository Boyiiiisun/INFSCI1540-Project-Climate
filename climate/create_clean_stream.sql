CREATE STREAM normal_dly_clean AS
SELECT
  station_id,
  station_name,
  elevation,
  latitude,
  longitude,
  norm_date,
  CAST(SUBSTRING(norm_date, 1, 4) AS INTEGER) AS year_num,
  CAST(SUBSTRING(norm_date, 5, 2) AS INTEGER) AS month_num,
  SUBSTRING(norm_date, 1, 6) AS norm_month,
  CAST(dly_tmin_normal AS DOUBLE) AS dly_tmin_normal,
  CAST(dly_tmax_normal AS DOUBLE) AS dly_tmax_normal,
  CAST(mtd_prcp_normal AS DOUBLE) AS mtd_prcp_normal
FROM normal_dly_raw
EMIT CHANGES;
