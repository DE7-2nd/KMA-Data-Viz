USE ROLE ANALYTICS_AUTHOR;
USE DATABASE KMA_DB;
USE SCHEMA ANALYTICS;
USE ROLE ANALYTICS_AUTHOR;
USE DATABASE KMA_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW wind_quarter_daily_vw AS

-- 2022년 데이터
SELECT
    w.std_id,
    w.tm_id,
    w.wind_dir,
    w.wind_sp,
    w.wind_deg,
    w.wind_rad,
    w.u,
    w.v,
    w.year,
    loc.lon,
    loc.lat,
    loc.std_ko,
    loc.std_en
FROM KMA_DB.RAW_DATA.wind_2022_quarter_daily w
LEFT JOIN KMA_DB.RAW_DATA.observation_location loc
    ON w.std_id = loc.std_id

UNION ALL

-- 2023년 데이터
SELECT
    w.std_id,
    w.tm_id,
    w.wind_dir,
    w.wind_sp,
    w.wind_deg,
    w.wind_rad,
    w.u,
    w.v,
    w.year,
    loc.lon,
    loc.lat,
    loc.std_ko,
    loc.std_en
FROM KMA_DB.RAW_DATA.wind_2023_quarter_daily w
LEFT JOIN KMA_DB.RAW_DATA.observation_location loc
    ON w.std_id = loc.std_id

UNION ALL

-- 2024년 데이터
SELECT
    w.std_id,
    w.tm_id,
    w.wind_dir,
    w.wind_sp,
    w.wind_deg,
    w.wind_rad,
    w.u,
    w.v,
    w.year,
    loc.lon,
    loc.lat,
    loc.std_ko,
    loc.std_en
FROM KMA_DB.RAW_DATA.wind_2024_quarter_daily w
LEFT JOIN KMA_DB.RAW_DATA.observation_location loc
    ON w.std_id = loc.std_id;

DROP VIEW IF EXISTS wind_2022_quarter_daily_vw;
