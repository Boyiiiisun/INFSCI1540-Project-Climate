USE dw;

-- Convert raw NOAA-style units once:
--   temperature: stored in tenths of degrees Fahrenheit
--   precipitation: stored in hundredths of inches
CREATE OR REPLACE VIEW vw_daily_climate_imperial AS
SELECT
    ds.station_id,
    ds.station_name,
    ds.latitude,
    ds.longitude,
    dd.year_num,
    dd.month_num,
    dd.month_name,
    dd.quarter_num,
    dd.full_date,
    ROUND(f.dly_tmin_normal / 10.0, 2) AS dly_tmin_normal_f,
    ROUND(f.dly_tmax_normal / 10.0, 2) AS dly_tmax_normal_f,
    ROUND(f.mtd_prcp_normal / 100.0, 2) AS mtd_prcp_normal_in
FROM fact_daily_climate_normal f
JOIN dim_date dd
    ON f.date_key = dd.date_key
JOIN dim_station ds
    ON f.station_key = ds.station_key
WHERE
    f.dly_tmin_normal IS NOT NULL
    AND f.dly_tmax_normal IS NOT NULL
    AND f.mtd_prcp_normal IS NOT NULL;

CREATE OR REPLACE VIEW vw_monthly_climate_imperial AS
SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    year_num,
    month_num,
    month_name,
    COUNT(*) AS num_days,
    ROUND(AVG((dly_tmin_normal_f + dly_tmax_normal_f) / 2), 2)
        AS monthly_avg_temperature_f,
    ROUND(MAX(dly_tmax_normal_f), 2) AS monthly_max_temperature_f,
    ROUND(MIN(dly_tmin_normal_f), 2) AS monthly_min_temperature_f,
    ROUND(MAX(mtd_prcp_normal_in), 2) AS monthly_total_precipitation_in
FROM vw_daily_climate_imperial
GROUP BY
    station_id,
    station_name,
    latitude,
    longitude,
    year_num,
    month_num,
    month_name;

-- OLAP Query 1:
-- Monthly station-level climate summary using converted units.
SELECT
    station_id,
    station_name,
    year_num,
    month_num,
    month_name,
    num_days,
    monthly_avg_temperature_f AS monthly_avg_temperature_normal,
    monthly_max_temperature_f AS monthly_max_temperature_normal,
    monthly_min_temperature_f AS monthly_min_temperature_normal,
    monthly_total_precipitation_in AS monthly_total_precipitation_normal
FROM vw_monthly_climate_imperial
ORDER BY
    station_name,
    year_num,
    month_num;

-- OLAP Query 2:
-- Compare similar-latitude coastal and inland stations:
-- Seattle Urban Site, WA vs Petersburg 2 N, ND.
WITH comparison_months AS (
    SELECT
        year_num,
        month_num,
        month_name,
        MAX(CASE
            WHEN station_id = 'GHCND:USW00024281'
            THEN monthly_avg_temperature_f
        END) AS seattle_avg_temperature,
        MAX(CASE
            WHEN station_id = 'GHCND:USC00327027'
            THEN monthly_avg_temperature_f
        END) AS petersburg_avg_temperature,
        MAX(CASE
            WHEN station_id = 'GHCND:USW00024281'
            THEN monthly_total_precipitation_in
        END) AS seattle_total_precipitation,
        MAX(CASE
            WHEN station_id = 'GHCND:USC00327027'
            THEN monthly_total_precipitation_in
        END) AS petersburg_total_precipitation
    FROM vw_monthly_climate_imperial
    WHERE station_id IN ('GHCND:USW00024281', 'GHCND:USC00327027')
    GROUP BY
        year_num,
        month_num,
        month_name
)
SELECT
    year_num,
    month_num,
    month_name,
    seattle_avg_temperature,
    petersburg_avg_temperature,
    ROUND(
        seattle_avg_temperature - petersburg_avg_temperature,
        2
    ) AS seattle_minus_petersburg_avg_temperature,
    seattle_total_precipitation,
    petersburg_total_precipitation,
    ROUND(
        seattle_total_precipitation - petersburg_total_precipitation,
        2
    ) AS seattle_minus_petersburg_precipitation
FROM comparison_months
ORDER BY
    year_num,
    month_num;

-- OLAP Query 3:
-- Compare San Francisco, CA vs Pittsburgh, PA by season.
WITH seasonal_station AS (
    SELECT
        station_id,
        station_name,
        year_num,
        CASE
            WHEN month_num IN (12, 1, 2) THEN 'Winter'
            WHEN month_num IN (3, 4, 5) THEN 'Spring'
            WHEN month_num IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season_name,
        CASE
            WHEN month_num IN (12, 1, 2) THEN 1
            WHEN month_num IN (3, 4, 5) THEN 2
            WHEN month_num IN (6, 7, 8) THEN 3
            ELSE 4
        END AS season_order,
        ROUND(AVG(monthly_avg_temperature_f), 2) AS seasonal_avg_temperature,
        ROUND(MAX(monthly_max_temperature_f), 2) AS seasonal_max_temperature,
        ROUND(MIN(monthly_min_temperature_f), 2) AS seasonal_min_temperature,
        ROUND(SUM(monthly_total_precipitation_in), 2)
            AS seasonal_total_precipitation
    FROM vw_monthly_climate_imperial
    WHERE station_id IN ('GHCND:USC00047767', 'GHCND:USW00094823')
    GROUP BY
        station_id,
        station_name,
        year_num,
        season_name,
        season_order
)
SELECT
    year_num,
    season_name,
    station_name,
    seasonal_avg_temperature,
    seasonal_max_temperature,
    seasonal_min_temperature,
    ROUND(seasonal_max_temperature - seasonal_min_temperature, 2)
        AS seasonal_temperature_range,
    seasonal_total_precipitation
FROM seasonal_station
ORDER BY
    year_num,
    season_order,
    station_name;

-- OLAP Query 4:
-- Compare Seattle, WA vs San Francisco, CA and rank monthly temperature gaps.
WITH paired_months AS (
    SELECT
        year_num,
        month_num,
        month_name,
        MAX(CASE
            WHEN station_id = 'GHCND:USW00024281'
            THEN latitude
        END) AS seattle_latitude,
        MAX(CASE
            WHEN station_id = 'GHCND:USC00047767'
            THEN latitude
        END) AS san_francisco_latitude,
        MAX(CASE
            WHEN station_id = 'GHCND:USW00024281'
            THEN monthly_avg_temperature_f
        END) AS seattle_avg_temperature,
        MAX(CASE
            WHEN station_id = 'GHCND:USC00047767'
            THEN monthly_avg_temperature_f
        END) AS san_francisco_avg_temperature,
        MAX(CASE
            WHEN station_id = 'GHCND:USW00024281'
            THEN monthly_total_precipitation_in
        END) AS seattle_total_precipitation,
        MAX(CASE
            WHEN station_id = 'GHCND:USC00047767'
            THEN monthly_total_precipitation_in
        END) AS san_francisco_total_precipitation
    FROM vw_monthly_climate_imperial
    WHERE station_id IN ('GHCND:USW00024281', 'GHCND:USC00047767')
    GROUP BY
        year_num,
        month_num,
        month_name
)
SELECT
    year_num,
    month_num,
    month_name,
    seattle_latitude,
    san_francisco_latitude,
    seattle_avg_temperature,
    san_francisco_avg_temperature,
    ROUND(
        san_francisco_avg_temperature - seattle_avg_temperature,
        2
    ) AS san_francisco_minus_seattle_temperature,
    seattle_total_precipitation,
    san_francisco_total_precipitation,
    ROUND(
        san_francisco_total_precipitation - seattle_total_precipitation,
        2
    ) AS san_francisco_minus_seattle_precipitation,
    RANK() OVER (s
        ORDER BY ABS(
            san_francisco_avg_temperature - seattle_avg_temperature
        ) DESC
    ) AS temperature_difference_rank
FROM paired_months
ORDER BY
    temperature_difference_rank,
    year_num,
    month_num;

-- OLAP Query 5:
-- Annual climate profile by station.
WITH annual_profile AS (
    SELECT
        station_id,
        station_name,
        year_num,
        ROUND(AVG(monthly_avg_temperature_f), 2) AS annual_avg_temperature,
        ROUND(MAX(monthly_max_temperature_f), 2) AS annual_max_temperature,
        ROUND(MIN(monthly_min_temperature_f), 2) AS annual_min_temperature,
        ROUND(
            MAX(monthly_max_temperature_f) - MIN(monthly_min_temperature_f),
            2
        ) AS annual_temperature_range,
        ROUND(SUM(monthly_total_precipitation_in), 2)
            AS annual_total_precipitation
    FROM vw_monthly_climate_imperial
    GROUP BY
        station_id,
        station_name,
        year_num
),
wettest_month AS (
    SELECT
        station_id,
        year_num,
        month_name AS wettest_month_name,
        monthly_total_precipitation_in AS wettest_month_precipitation,
        ROW_NUMBER() OVER (
            PARTITION BY station_id, year_num
            ORDER BY monthly_total_precipitation_in DESC, month_num
        ) AS precipitation_rank
    FROM vw_monthly_climate_imperial
)
SELECT
    ap.station_id,
    ap.station_name,
    ap.year_num,
    ap.annual_avg_temperature,
    ap.annual_max_temperature,
    ap.annual_min_temperature,
    ap.annual_temperature_range,
    ap.annual_total_precipitation,
    wm.wettest_month_name,
    wm.wettest_month_precipitation,
    RANK() OVER (
        PARTITION BY ap.year_num
        ORDER BY ap.annual_temperature_range DESC
    ) AS annual_temperature_range_rank
FROM annual_profile ap
JOIN wettest_month wm
    ON ap.station_id = wm.station_id
    AND ap.year_num = wm.year_num
WHERE wm.precipitation_rank = 1
ORDER BY
    ap.year_num,
    annual_temperature_range_rank,
    ap.station_name;
