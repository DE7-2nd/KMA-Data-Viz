-- SNOWFLAKE 실행환경 준비
USE ROLE ANALYTICS_AUTHOR;
USE WAREHOUSE COMPUTE_WH;

"""
관측지점 및 계절연도 별 계절 일 수 및 비율 산출 VIEW 생성
칼럼 : 관측지점 / 계절연도 / 봄 일 수 / 여름 일 수/ 가을 일 수/ 겨울 일 수/ 봄 비율 / 여름 비율 / 가을 비율 / 겨울 비율
"""
CREATE OR REPLACE VIEW KMA_DB.ANALYTICS.SEASON_DAYS_PIVOT AS
WITH first_dates AS (
    SELECT
        STN_KO,
        SEASON_YEAR,
        MAIN_SEASON,
        MIN(TM) AS FIRST_DATE
    FROM KMA_DB.RAW_DATA.SEASON_YEARLY
    WHERE MAIN_SEASON IN ('봄', '여름', '가을', '겨울')
    GROUP BY STN_KO, SEASON_YEAR, MAIN_SEASON
),

-- winter → spring 연결 고려 (계절 순서 중요)
ordered AS (
    SELECT
        STN_KO,
        SEASON_YEAR,
        MAIN_SEASON,
        FIRST_DATE,
        LEAD(FIRST_DATE) OVER (
            PARTITION BY STN_KO, SEASON_YEAR
            ORDER BY
                CASE MAIN_SEASON         -- ✅ 겨울 → 봄 → 여름 → 가을 순서
                    WHEN '겨울' THEN 1
                    WHEN '봄'   THEN 2
                    WHEN '여름' THEN 3
                    WHEN '가을' THEN 4
                END
        ) AS NEXT_FIRST_DATE
    FROM first_dates
),

-- 봄/여름/겨울만 first_date 차이로 DAYS 계산
season_days_raw AS (
    SELECT
        STN_KO,
        SEASON_YEAR,
        MAIN_SEASON,
        CASE
            WHEN MAIN_SEASON = '가을' THEN NULL
            WHEN NEXT_FIRST_DATE IS NULL THEN NULL
            ELSE DATEDIFF('day', FIRST_DATE, NEXT_FIRST_DATE) + 1
        END AS DAYS
    FROM ordered
),

-- 가을 = 365 - (봄 + 여름 + 겨울)
season_days_fixed AS (
    SELECT
        STN_KO,
        SEASON_YEAR,
        MAIN_SEASON,
        CASE
            WHEN MAIN_SEASON = '가을'
                THEN 365 - (
                        SUM(CASE WHEN MAIN_SEASON <> '가을' THEN DAYS ELSE 0 END)
                        OVER (PARTITION BY STN_KO, SEASON_YEAR)
                    )
            ELSE DAYS
        END AS DAYS
    FROM season_days_raw
)

SELECT
    STN_KO,
    SEASON_YEAR,

    MAX(CASE WHEN MAIN_SEASON = '봄'   THEN DAYS END) AS SPRING_DAYS,
    MAX(CASE WHEN MAIN_SEASON = '여름' THEN DAYS END) AS SUMMER_DAYS,
    MAX(CASE WHEN MAIN_SEASON = '가을' THEN DAYS END) AS AUTUMN_DAYS,
    MAX(CASE WHEN MAIN_SEASON = '겨울' THEN DAYS END) AS WINTER_DAYS,

    MAX(CASE WHEN MAIN_SEASON = '봄'   THEN ROUND(DAYS / 365, 4) END) AS SPRING_RATIO,
    MAX(CASE WHEN MAIN_SEASON = '여름' THEN ROUND(DAYS / 365, 4) END) AS SUMMER_RATIO,
    MAX(CASE WHEN MAIN_SEASON = '가을' THEN ROUND(DAYS / 365, 4) END) AS AUTUMN_RATIO,
    MAX(CASE WHEN MAIN_SEASON = '겨울' THEN ROUND(DAYS / 365, 4) END) AS WINTER_RATIO

FROM season_days_fixed
WHERE SEASON_YEAR >= 2010                  -- 연도 제한 (2009년 데이터에는 4계절이 다 존재하지 않음)
ORDER BY STN_KO, SEASON_YEAR;

'''
null 값 존재 컬럼 검증 
'''
SELECT *
FROM KMA_DB.ANALYTICS.SEASON_DAYS_PIVOT
WHERE SPRING_DAYS IS NULL
    OR SUMMER_DAYS IS NULL
    OR AUTUMN_DAYS IS NULL
    OR WINTER_DAYS IS NULL
ORDER BY STN_KO, SEASON_YEAR;