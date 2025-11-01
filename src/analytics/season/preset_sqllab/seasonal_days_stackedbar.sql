"""
계절일 수 누적차트 생성 베이스 쿼리
"""
SELECT 
    STN_KO,
    SEASON_YEAR,
    '봄' as season,
    SUM(SPRING_DAYS) as days,
    1 as season_order
FROM season_days_pivot 
GROUP BY STN_KO, SEASON_YEAR

UNION ALL

SELECT
    STN_KO,
    SEASON_YEAR,
    '여름' as season,
    SUM(SUMMER_DAYS) as days,
    2 as season_order
FROM season_days_pivot
GROUP BY STN_KO, SEASON_YEAR

UNION ALL

SELECT
    STN_KO,
    SEASON_YEAR,
    '가을' as season,
    SUM(AUTUMN_DAYS) as days,
    3 as season_order
FROM season_days_pivot
GROUP BY STN_KO, SEASON_YEAR

UNION ALL

SELECT
    STN_KO,
    SEASON_YEAR,
    '겨울' as season,
    SUM(WINTER_DAYS) as days,
    4 as season_order
FROM season_days_pivot
GROUP BY STN_KO, SEASON_YEAR