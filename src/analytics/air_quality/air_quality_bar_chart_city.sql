/* 6dcd92a04feb50f14bbcf07c661680ba */
SELECT
  "CITY" AS "CITY",
  "POLLUTANT" AS "POLLUTANT",
  AVG("VALUE") AS "AVG(VALUE)"
FROM analytics.air_quality_daily_cleaned
JOIN (
  SELECT
    "POLLUTANT" AS "POLLUTANT__",
    AVG("VALUE") AS "mme_inner__"
  FROM analytics.air_quality_daily_cleaned
  WHERE
    "DATE" >= CAST('2015-01-01' AS DATE) AND "DATE" < CAST('2025-09-23' AS DATE)
  GROUP BY
    "POLLUTANT"
  ORDER BY
    "mme_inner__" DESC
  LIMIT 7
) AS series_limit
  ON "POLLUTANT" = "POLLUTANT__"
WHERE
  "DATE" >= CAST('2015-01-01' AS DATE) AND "DATE" < CAST('2025-09-23' AS DATE)
GROUP BY
  "CITY",
  "POLLUTANT"
ORDER BY
  "AVG(VALUE)" DESC
LIMIT 10000 /* 6dcd92a04feb50f14bbcf07c661680ba; */