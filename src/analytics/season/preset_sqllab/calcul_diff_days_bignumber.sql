"""
관측지점 및 계절년도별 계절 시작일 및 전년도 시작일 일 수 비교 값 생성
빅넘버차트 베이스 쿼리
"""
WITH current_data AS (
    SELECT 
        STN_KO,
        SEASON_YEAR,
        MAIN_SEASON,
        MIN(TM) as season_start
    FROM KMA_DB.RAW_DATA.SEASON_YEARLY
    GROUP BY STN_KO, SEASON_YEAR, MAIN_SEASON
),
comparison AS (
    SELECT
    c.STN_KO,
    c.SEASON_YEAR,
    c.MAIN_SEASON,
    c.season_start as current_start,
    p.season_start as prev_start,
    -- 연도를 같은 값으로 맞춰서 월-일만으로 차이 계산
    DATEDIFF(day, 
        TO_DATE(CONCAT('2000-', TO_CHAR(p.season_start, 'MM-DD')), 'YYYY-MM-DD'),
        TO_DATE(CONCAT('2000-', TO_CHAR(c.season_start, 'MM-DD')), 'YYYY-MM-DD')
    ) as day_diff
    FROM current_data c
    LEFT JOIN current_data p 
    ON c.STN_KO = p.STN_KO 
    AND c.MAIN_SEASON = p.MAIN_SEASON
    AND c.SEASON_YEAR = p.SEASON_YEAR + 1
    -- WHERE c.SEASON_YEAR = 2025
    --   AND c.STN_KO = '서울'
)
SELECT
    STN_KO,
    SEASON_YEAR,
    MAIN_SEASON,
    TO_CHAR(current_start, 'YYYY-MM-DD') as current_dt,
    TO_CHAR(prev_start, 'YYYY-MM-DD') as prev_dt,
    TO_CHAR(current_start, 'MM월 DD일') as current_display,
    TO_CHAR(prev_start, 'MM월 DD일') as prev_display,
    CASE 
        WHEN day_diff > 0 THEN CONCAT('(전년 대비 +', day_diff, '일)')
        WHEN day_diff < 0 THEN CONCAT('(전년 대비 ', day_diff, '일)')
        WHEN day_diff IS NULL THEN '(전년 데이터 없음)'
        ELSE '(동일)'
    END as comparison_text,
    CASE 
        -- 각 시즌연도 및 관측지점별로는 계절라벨이 없을 수 있기때문
        WHEN day_diff IS NULL THEN 0
        ELSE day_diff
    END as day_diff
FROM comparison
ORDER BY SEASON_YEAR DESC;