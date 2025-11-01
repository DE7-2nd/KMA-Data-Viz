"""
SSN_ID & SSN_MD 조합 sunburest 차트 구현 위한 베이스 쿼리
"""
WITH combo_usage AS (
    SELECT
        MAIN_SEASON,
        CONCAT_WS(' - ', ID_NAME, MD_NAME) AS COMBO_NAME,
        COUNT(*) AS USED_COUNT
    FROM KMA_DB.RAW_DATA.SEASON_YEARLY
    GROUP BY MAIN_SEASON, COMBO_NAME
),
ranked AS (
    SELECT
        MAIN_SEASON,
        COMBO_NAME,
        USED_COUNT,
        ROW_NUMBER() OVER (
            PARTITION BY MAIN_SEASON
            ORDER BY USED_COUNT DESC
        ) AS RN
    FROM combo_usage
)
SELECT
    MAIN_SEASON,
    COMBO_NAME,
    USED_COUNT
FROM ranked
WHERE RN <= 5   -- ✅ Top 개수 조정 가능
ORDER BY
    CASE MAIN_SEASON
        WHEN '겨울' THEN 1
        WHEN '봄' THEN 2
        WHEN '여름' THEN 3
        WHEN '가을' THEN 4
        ELSE 5
    END,
    USED_COUNT DESC;
