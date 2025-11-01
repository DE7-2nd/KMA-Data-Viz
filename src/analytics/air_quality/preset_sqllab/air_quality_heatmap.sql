"""
연도 및 월에 따른 대기오염물질 농도 히트맵 구성 위한 베이스 쿼리
"""
SELECT
    CASE CITY
        WHEN 'seoul'   THEN '서울'
        WHEN 'busan'   THEN '부산'
        WHEN 'gwangju' THEN '광주'
        WHEN 'incheon' THEN '인천'
        WHEN 'daegu'   THEN '대구'
        WHEN 'daejeon' THEN '대전'
        WHEN 'jeju'    THEN '제주'
        ELSE CITY
    END AS CITY_KO,

    POLLUTANT,

    YEAR(DATE) AS YEAR,
    LPAD(YEAR(DATE), 4, '0') AS YEAR_ORDER,     -- ✅ Y축 정렬용

    MONTH(DATE) AS MONTH,
    LPAD(MONTH(DATE), 2, '0') AS MONTH_ORDER,   

    AVG(VALUE) AS AVG_VALUE
FROM KMA_DB.RAW_DATA.AIR_QUALITY_DAILY
WHERE 1=1
{% if filter_values("pollutant") %}
    AND POLLUTANT = {{ filter_values("pollutant") | where_in }}
{% endif %}
{% if filter_values("city") %}
    AND CITY = {{ filter_values("city") | where_in }}
{% endif %}
GROUP BY CITY_KO, POLLUTANT, YEAR, YEAR_ORDER, MONTH, MONTH_ORDER
ORDER BY YEAR_ORDER DESC, MONTH_ORDER;
