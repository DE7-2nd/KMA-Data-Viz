create table analytics.forecast_daily_avg_vw as
(
    WITH forecast AS (
      SELECT
          rw.region,
          rw.region_code,
          rw.forecast_issue_time,
          rw.forecast_time,
          rw.rain_probability,
          DATEDIFF(hour, rw.forecast_issue_time, rw.forecast_time) AS lead_time_hours,
          al.std_id AS matched_region,
          al.std_ko
      FROM raw_data.forecast_halfday rw
      JOIN analytics.obsercation_forecast_location_vw al
        ON rw.region_code = al.region_code
    )
    SELECT
        TO_DATE(forecast.forecast_issue_time) AS forecast_date,
        forecast.region,
        AVG(forecast.rain_probability) AS avg_rain_prob
    FROM forecast
    GROUP BY forecast_date, forecast.region
    ORDER BY forecast_date, forecast.region
)
    