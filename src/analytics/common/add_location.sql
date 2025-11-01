-- CREATE OR REPLACE TABLE raw_data.observation_location (
--     std_id     INTEGER      NOT NULL COMMENT '관측지점 ID',
--     lon        DOUBLE       NOT NULL COMMENT '경도 degree',
--     lat        DOUBLE       NOT NULL COMMENT '위도 degree',
--     std_ko     VARCHAR(64)  COMMENT '지점명 한글',
--     std_en     VARCHAR(32)  COMMENT '지점명 영문',
--     fct_id     VARCHAR(32)  COMMENT '예보구역 코드',
--     CONSTRAINT pk_observation_location PRIMARY KEY (std_id)
-- );

-- COPY INTO raw_data.observation_location
-- FROM 's3://kma-data-storage/common/observation_location.csv'
-- credentials=(AWS_KEY_ID='AWS_KEY_ID' AWS_SECRET_KEY='AWS_SECRET_KEY')
-- FILE_FORMAT = (type='CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

SELECT * FROM raw_data.observation_location;