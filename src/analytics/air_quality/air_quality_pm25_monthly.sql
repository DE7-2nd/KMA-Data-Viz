/* 6dcd92a04feb50f14bbcf07c661680ba */
SELECT
  DATE_TRUNC('MONTH', "DATE") AS "__timestamp",
  "CITY" AS "CITY",
  AVG("VALUE") AS "AVG(VALUE)"
FROM analytics.air_quality_daily_cleaned
JOIN (
  SELECT
    "CITY" AS "CITY__",
    AVG("VALUE") AS "mme_inner__"
  FROM analytics.air_quality_daily_cleaned
  WHERE
    "POLLUTANT" IN ('pm25')
    AND "DATE" >= CAST('2015-01-01' AS DATE)
    AND "DATE" < CAST('2025-10-23' AS DATE)
  GROUP BY
    "CITY"
  ORDER BY
    "mme_inner__" DESC
  LIMIT 7
) AS series_limit
  ON "CITY" = "CITY__"
WHERE
  "DATE" >= CAST('2015-01-01' AS DATE)
  AND "DATE" < CAST('2025-10-23' AS DATE)
  AND "POLLUTANT" IN ('pm25')
GROUP BY
  "CITY",
  DATE_TRUNC('MONTH', "DATE")
ORDER BY
  "AVG(VALUE)" DESC
LIMIT 50000 /* 6dcd92a04feb50f14bbcf07c661680ba; */