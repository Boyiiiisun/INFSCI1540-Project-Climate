USE dw;

-- OLAP Query 1:
-- Monthly average temperature, monthly max temperature,
-- monthly min temperature, and monthly total precipitation by station.
SELECT
    ds.station_id,
    ds.station_name,
    dd.year_num,
    dd.month_num,
    dd.month_name,
    COUNT(*) AS num_days,
    ROUND(AVG((f.dly_tmin_normal + f.dly_tmax_normal) / 2), 2)
        AS monthly_avg_temperature_normal,
    ROUND(MAX(f.dly_tmax_normal), 2)
        AS monthly_max_temperature_normal,
    ROUND(MIN(f.dly_tmin_normal), 2)
        AS monthly_min_temperature_normal,
    ROUND(MAX(f.mtd_prcp_normal), 2)
        AS monthly_total_precipitation_normal
FROM fact_daily_climate_normal f
JOIN dim_date dd
    ON f.date_key = dd.date_key
JOIN dim_station ds
    ON f.station_key = ds.station_key
WHERE
    f.dly_tmin_normal IS NOT NULL
    AND f.dly_tmax_normal IS NOT NULL
    AND f.mtd_prcp_normal IS NOT NULL
GROUP BY
    ds.station_id,
    ds.station_name,
    dd.year_num,
    dd.month_num,
    dd.month_name
ORDER BY
    ds.station_name,
    dd.year_num,
    dd.month_num;

-- OLAP Query 2:
-- Compare similar-latitude coastal and inland stations:
-- Seattle Urban Site, WA vs Petersburg 2 N, ND.
-- This query pivots the two stations into the same monthly row so their
-- temperature and precipitation differences are easy to compare.
WITH monthly_station AS (
    SELECT
        ds.station_id,
        ds.station_name,
        CASE
            WHEN ds.station_id = 'GHCND:USW00024281' THEN 'Coastal Seattle'
            WHEN ds.station_id = 'GHCND:USC00327027' THEN 'Inland Petersburg'
        END AS comparison_group,
        dd.year_num,
        dd.month_num,
        dd.month_name,
        ROUND(AVG((f.dly_tmin_normal + f.dly_tmax_normal) / 2), 2)
            AS monthly_avg_temperature_normal,
        ROUND(MAX(f.mtd_prcp_normal), 2)
            AS monthly_total_precipitation_normal
    FROM fact_daily_climate_normal f
    JOIN dim_date dd
        ON f.date_key = dd.date_key
    JOIN dim_station ds
        ON f.station_key = ds.station_key
    WHERE ds.station_id IN ('GHCND:USW00024281', 'GHCND:USC00327027')
    GROUP BY
        ds.station_id,
        ds.station_name,
        comparison_group,
        dd.year_num,
        dd.month_num,
        dd.month_name
)
SELECT
    year_num,
    month_num,
    month_name,
    MAX(CASE WHEN comparison_group = 'Coastal Seattle'
        THEN monthly_avg_temperature_normal END) AS seattle_avg_temperature,
    MAX(CASE WHEN comparison_group = 'Inland Petersburg'
        THEN monthly_avg_temperature_normal END) AS petersburg_avg_temperature,
    ROUND(
        MAX(CASE WHEN comparison_group = 'Coastal Seattle'
            THEN monthly_avg_temperature_normal END)
        - MAX(CASE WHEN comparison_group = 'Inland Petersburg'
            THEN monthly_avg_temperature_normal END),
        2
    ) AS seattle_minus_petersburg_avg_temperature,
    MAX(CASE WHEN comparison_group = 'Coastal Seattle'
        THEN monthly_total_precipitation_normal END) AS seattle_total_precipitation,
    MAX(CASE WHEN comparison_group = 'Inland Petersburg'
        THEN monthly_total_precipitation_normal END) AS petersburg_total_precipitation,
    ROUND(
        MAX(CASE WHEN comparison_group = 'Coastal Seattle'
            THEN monthly_total_precipitation_normal END)
        - MAX(CASE WHEN comparison_group = 'Inland Petersburg'
            THEN monthly_total_precipitation_normal END),
        2
    ) AS seattle_minus_petersburg_precipitation
FROM monthly_station
GROUP BY
    year_num,
    month_num,
    month_name
ORDER BY
    year_num,
    month_num;

-- OLAP Query 3:
-- Compare a west-coast major city station and an east/inland major city station:
-- San Francisco, CA vs Pittsburgh, PA.
-- This query rolls months into seasons to show broader regional differences.
WITH monthly_station AS (
    SELECT
        ds.station_id,
        ds.station_name,
        dd.year_num,
        dd.month_num,
        ROUND(AVG((f.dly_tmin_normal + f.dly_tmax_normal) / 2), 2)
            AS monthly_avg_temperature_normal,
        ROUND(MAX(f.dly_tmax_normal), 2)
            AS monthly_max_temperature_normal,
        ROUND(MIN(f.dly_tmin_normal), 2)
            AS monthly_min_temperature_normal,
        ROUND(MAX(f.mtd_prcp_normal), 2)
            AS monthly_total_precipitation_normal
    FROM fact_daily_climate_normal f
    JOIN dim_date dd
        ON f.date_key = dd.date_key
    JOIN dim_station ds
        ON f.station_key = ds.station_key
    WHERE ds.station_id IN ('GHCND:USC00047767', 'GHCND:USW00094823')
    GROUP BY
        ds.station_id,
        ds.station_name,
        dd.year_num,
        dd.month_num
),
seasonal_station AS (
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
        ROUND(AVG(monthly_avg_temperature_normal), 2)
            AS seasonal_avg_temperature,
        ROUND(MAX(monthly_max_temperature_normal), 2)
            AS seasonal_max_temperature,
        ROUND(MIN(monthly_min_temperature_normal), 2)
            AS seasonal_min_temperature,
        ROUND(SUM(monthly_total_precipitation_normal), 2)
            AS seasonal_total_precipitation
    FROM monthly_station
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
-- Compare two west-coast stations at different latitudes:
-- Seattle, WA vs San Francisco, CA.
-- This query ranks months by the absolute temperature difference between them.
WITH monthly_station AS (
    SELECT
        ds.station_id,
        ds.station_name,
        ds.latitude,
        ds.longitude,
        dd.year_num,
        dd.month_num,
        dd.month_name,
        ROUND(AVG((f.dly_tmin_normal + f.dly_tmax_normal) / 2), 2)
            AS monthly_avg_temperature_normal,
        ROUND(MAX(f.mtd_prcp_normal), 2)
            AS monthly_total_precipitation_normal
    FROM fact_daily_climate_normal f
    JOIN dim_date dd
        ON f.date_key = dd.date_key
    JOIN dim_station ds
        ON f.station_key = ds.station_key
    WHERE ds.station_id IN ('GHCND:USW00024281', 'GHCND:USC00047767')
    GROUP BY
        ds.station_id,
        ds.station_name,
        ds.latitude,
        ds.longitude,
        dd.year_num,
        dd.month_num,
        dd.month_name
),
paired_months AS (
    SELECT
        sea.year_num,
        sea.month_num,
        sea.month_name,
        sea.latitude AS seattle_latitude,
        sf.latitude AS san_francisco_latitude,
        sea.monthly_avg_temperature_normal AS seattle_avg_temperature,
        sf.monthly_avg_temperature_normal AS san_francisco_avg_temperature,
        ROUND(sf.monthly_avg_temperature_normal
            - sea.monthly_avg_temperature_normal, 2)
            AS san_francisco_minus_seattle_temperature,
        sea.monthly_total_precipitation_normal AS seattle_total_precipitation,
        sf.monthly_total_precipitation_normal AS san_francisco_total_precipitation,
        ROUND(sf.monthly_total_precipitation_normal
            - sea.monthly_total_precipitation_normal, 2)
            AS san_francisco_minus_seattle_precipitation
    FROM monthly_station sea
    JOIN monthly_station sf
        ON sea.year_num = sf.year_num
        AND sea.month_num = sf.month_num
    WHERE sea.station_id = 'GHCND:USW00024281'
        AND sf.station_id = 'GHCND:USC00047767'
)
SELECT
    year_num,
    month_num,
    month_name,
    seattle_latitude,
    san_francisco_latitude,
    seattle_avg_temperature,
    san_francisco_avg_temperature,
    san_francisco_minus_seattle_temperature,
    seattle_total_precipitation,
    san_francisco_total_precipitation,
    san_francisco_minus_seattle_precipitation,
    RANK() OVER (
        ORDER BY ABS(san_francisco_minus_seattle_temperature) DESC
    ) AS temperature_difference_rank
FROM paired_months
ORDER BY
    temperature_difference_rank,
    year_num,
    month_num;

-- OLAP Query 5:
-- Annual climate profile by station.
-- This query adds more OLAP variety by combining yearly averages,
-- annual temperature range, annual precipitation, and wettest month ranking.
WITH monthly_station AS (
    SELECT
        ds.station_id,
        ds.station_name,
        dd.year_num,
        dd.month_num,
        dd.month_name,
        ROUND(AVG((f.dly_tmin_normal + f.dly_tmax_normal) / 2), 2)
            AS monthly_avg_temperature_normal,
        ROUND(MAX(f.dly_tmax_normal), 2)
            AS monthly_max_temperature_normal,
        ROUND(MIN(f.dly_tmin_normal), 2)
            AS monthly_min_temperature_normal,
        ROUND(MAX(f.mtd_prcp_normal), 2)
            AS monthly_total_precipitation_normal
    FROM fact_daily_climate_normal f
    JOIN dim_date dd
        ON f.date_key = dd.date_key
    JOIN dim_station ds
        ON f.station_key = ds.station_key
    GROUP BY
        ds.station_id,
        ds.station_name,
        dd.year_num,
        dd.month_num,
        dd.month_name
),
annual_profile AS (
    SELECT
        station_id,
        station_name,
        year_num,
        ROUND(AVG(monthly_avg_temperature_normal), 2)
            AS annual_avg_temperature,
        ROUND(MAX(monthly_max_temperature_normal), 2)
            AS annual_max_temperature,
        ROUND(MIN(monthly_min_temperature_normal), 2)
            AS annual_min_temperature,
        ROUND(MAX(monthly_max_temperature_normal)
            - MIN(monthly_min_temperature_normal), 2)
            AS annual_temperature_range,
        ROUND(SUM(monthly_total_precipitation_normal), 2)
            AS annual_total_precipitation
    FROM monthly_station
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
        monthly_total_precipitation_normal AS wettest_month_precipitation,
        ROW_NUMBER() OVER (
            PARTITION BY station_id, year_num
            ORDER BY monthly_total_precipitation_normal DESC, month_num
        ) AS precipitation_rank
    FROM monthly_station
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
