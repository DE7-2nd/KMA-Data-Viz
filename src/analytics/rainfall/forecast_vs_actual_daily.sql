CREATE OR REPLACE VIEW analytics.forecast_date_hours AS
  SELECT
      f.region,
      f.region_code,
      TO_DATE(f.forecast_issue_time)                           AS forecast_date,
      DATEDIFF('hour', f.forecast_time, f.forecast_issue_time) AS lead_hours,
      f.rain_probability
  FROM raw_data.forecast_halfday f

CREATE OR REPLACE VIEW analytics.forecast_agg AS (
  SELECT
      region,
      region_code,
      forecast_date,
      lead_hours,
      AVG(rain_probability) AS avg_rain_prob
  FROM forecast
  GROUP BY region, region_code, forecast_date, lead_hours
)

CREATE OR REPLACE VIEW analytics.rainfall_actual AS (
  SELECT
      TO_DATE(r.date) AS obs_date,
      l.region,
      l.region_code,
      AVG(IFF(r.rain_yn, 1, 0)) AS actual_rain_rate,   -- 지역 내 지점 비율
      MAX(IFF(r.rain_yn, 1, 0)) AS actual_rain_yn_any  -- 지역 내 한 곳이라도 비가 오면 1
  FROM raw_data.rainfall_daily r
  JOIN analytics.observation_forecast_location_vw l
    ON r.std_id = l.std_id
  GROUP BY obs_date, l.region, l.region_code 
)

CREATE OR REPLACE VIEW analytics.forecast_vs_actual_daily AS
SELECT
    fa.forecast_date,
    fa.region,
    fa.region_code,
    fa.lead_hours,          -- 0,12,24,... 시간 단위 버킷
    fa.avg_rain_prob,          -- 예보 평균 강수확률(%)
    a.actual_rain_rate,        -- 실제 강수 비율(0~1)
    a.actual_rain_yn_any       -- 실제 강수 여부(0/1)
FROM analytics.forecast_agg fa
LEFT JOIN analytics.rainfall_actual a
  ON fa.region = a.region
 AND fa.region_code = a.region_code
 AND fa.forecast_date = a.obs_date
ORDER BY fa.forecast_date, fa.region, fa.lead_hours;