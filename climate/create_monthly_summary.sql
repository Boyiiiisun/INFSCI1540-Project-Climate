CREATE TABLE normal_dly_monthly_summary
WITH (
  KEY_FORMAT = 'JSON',
  VALUE_FORMAT = 'JSON'
) AS
SELECT
  station_id,
  AS_VALUE(station_id) AS station_id_v,
  station_name,
  AS_VALUE(station_name) AS station_name_v,
  norm_month,
  AS_VALUE(norm_month) AS norm_month_v,
  year_num,
  AS_VALUE(year_num) AS year_num_v,
  month_num,
  AS_VALUE(month_num) AS month_num_v,
  COUNT(*) AS num_days,
  AVG(dly_tmin_normal) AS avg_dly_tmin_normal,
  AVG(dly_tmax_normal) AS avg_dly_tmax_normal,
  MAX(mtd_prcp_normal) AS month_end_prcp_normal
FROM normal_dly_clean
GROUP BY station_id, station_name, norm_month, year_num, month_num
EMIT CHANGES;
