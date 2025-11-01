"""
계절비율 파이차트 베이스 쿼리
"""
SELECT
    STN_KO,
    SEASON_YEAR,
    CASE season_name
        WHEN 'SPRING_DAYS'  THEN '봄'
        WHEN 'SUMMER_DAYS'  THEN '여름'
        WHEN 'AUTUMN_DAYS'  THEN '가을'
        WHEN 'WINTER_DAYS'  THEN '겨울'
    END AS MAIN_SEASON,
    days AS DAYS
FROM KMA_DB.ANALYTICS.SEASON_DAYS_PIVOT
UNPIVOT (
    days FOR season_name IN (
        SPRING_DAYS,
        SUMMER_DAYS,
        AUTUMN_DAYS,
        WINTER_DAYS
    )
);
