-- 예보 데이터 분석 (지역별로 어떤 날씨가 제일 많이 예보되었는지 확인 2025년 1년간 데이터)
SELECT
    REGION,
    WEATHER_CONDITION,
    COUNT(*) AS cnt
FROM raw_data.forecast_halfday
GROUP BY REGION, WEATHER_CONDITION
HAVING REGION = '제주도'
ORDER BY cnt DESC;


/*
COUNT_IF(rain_yn = TRUE) → 해당 달에 비가 온 날 수
EXTRACT(MONTH FROM date) → 날짜에서 월만 추출
WHERE std_id = 108 → 특정 관측지점 필터
*/
SELECT 
    EXTRACT(MONTH FROM date) AS month,
    COUNT_IF(rain_yn = TRUE) AS rainy_days
FROM raw_data.rainfall_daily
WHERE std_id = 108
GROUP BY EXTRACT(MONTH FROM date)
ORDER BY month;


/*예보 데이터 분석 (예보가 예측한 시간과 예보가 발표된 시간의 차이를 계산)
DATEDIFF(day, forecast_time, forecast_issue_time) AS lead_time_hours
평균값으로 어떤날에 어떠한 rain probability 값을 가지는지 확인
*/
WITH forecast AS (
  SELECT
      region,
      region_code,
      forecast_issue_time,   -- 예보가 예측한 시간
      forecast_time,         -- 예보가 발표된 시간
      weather_condition,
      rain_probability,
      DATEDIFF(day, forecast_time, forecast_issue_time) AS lead_time_hours
  FROM raw_data.forecast_halfday
)
SELECT *
FROM forecast
ORDER BY forecast_issue_time, lead_time_hours;