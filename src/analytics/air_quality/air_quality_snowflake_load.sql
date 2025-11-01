- Use role and warehouse first
USE ROLE ANALYTICS_AUTHOR;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE KMA_DB;
USE SCHEMA RAW_DATA;

-- Create (or replace if it exists) a table to store air quality data
CREATE OR REPLACE TABLE air_quality_daily (
    date DATE,
    city STRING,
    pollutant STRING,
    value NUMBER
);

-- Load CSV data from S3 into the table
COPY INTO air_quality_daily
FROM 's3://kma-data-storage/air_quality/air_quality_daily.csv'
CREDENTIALS=(
    AWS_KEY_ID='KEY_ID'
    AWS_SECRET_KEY='KEY'
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    SKIP_HEADER = 1
);

-- Verify the table in Snowflake
SELECT *
FROM raw_data.air_quality_daily
LIMIT 10;
