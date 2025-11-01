-- USE ROLE ANALYTICS_AUTHOR;
-- USE WAREHOUSE COMPUTE_WH;
-- USE DATABASE kma_db;

-- CREATE OR REPLACE TABLE raw_data.forecast_halfday (
--     forecast_time         TIMESTAMP_NTZ NOT NULL COMMENT '예보 발표 시각',
--     region                VARCHAR(64)   NOT NULL COMMENT '지역명',
--     forecast_issue_time   TIMESTAMP_NTZ NOT NULL COMMENT '예보 대상 시각 (이 시점의 날씨를 예측)',
--     weather_condition     VARCHAR(64)   NOT NULL COMMENT '예보 상태 (맑음, 흐림 등)',
--     rain_probability      NUMBER        COMMENT '강수 확률 (%)',
--     region_code           VARCHAR(20)   NOT NULL COMMENT '지역코드',
--     CONSTRAINT pk_forecast PRIMARY KEY (
--         forecast_issue_time,
--         forecast_time,
--         region_code
--     )
-- );

-- COPY INTO raw_data.forecast_halfday
-- FROM 's3://kma-data-storage/rainfall/forecast_halfday.csv'
-- credentials=(AWS_KEY_ID='AWS_KEY_ID' AWS_SECRET_KEY='AWS_SECRET_KEY')
-- FILE_FORMAT = (type='CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

SELECT * from raw_data.forecast_halfday