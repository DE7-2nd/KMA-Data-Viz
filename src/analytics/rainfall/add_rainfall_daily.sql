-- USE ROLE ANALYTICS_AUTHOR;
-- USE WAREHOUSE COMPUTE_WH;
-- USE DATABASE kma_db;

-- CREATE OR REPLACE TABLE raw_data.rainfall_daily (
--     date        TIMESTAMP_NTZ   NOT NULL COMMENT '날짜 TM 관측시간(KST)',
--     std_id      INTEGER         NOT NULL COMMENT '관측지점 ID',
--     rain_dur    NUMBER          COMMENT '강수 계속 시간(hr) RN_DUR',
--     rain_d99    NUMBER          COMMENT '9-9 강수량(mm) RN_D99',
--     rain_yn     BOOLEAN         NOT NULL COMMENT '강수 유무 파생 필드',

--     -- Primary Key (날짜 + 지점 기준이 유일)
--     CONSTRAINT pk_rainfall_daily PRIMARY KEY (date, std_id),

--     -- Foreign Key → observation_location.std_id
--     CONSTRAINT fk_rainfall_daily_std_id
--         FOREIGN KEY (std_id)
--         REFERENCES raw_data.observation_location(std_id)
-- );


-- COPY INTO raw_data.rainfall_daily
-- FROM 's3://kma-data-storage/rainfall/rainfall_daily.csv'
-- credentials=(AWS_KEY_ID='AWS_KEY_ID' AWS_SECRET_KEY='AWS_SECRET_KEY')
-- FILE_FORMAT = (type='CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

SELECT * FROM raw_data.rainfall_daily;