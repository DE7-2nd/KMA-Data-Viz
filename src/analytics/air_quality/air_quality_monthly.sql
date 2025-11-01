/* 6dcd92a04feb50f14bbcf07c661680ba */
SELECT
  DATE_TRUNC('MONTH', "DATE") AS "__timestamp",
  "POLLUTANT" AS "POLLUTANT",
  AVG("VALUE") AS "AVG(VALUE)"
FROM analytics.air_quality_daily_cleaned
JOIN (
  SELECT
    "POLLUTANT" AS "POLLUTANT__",
    AVG("VALUE") AS "mme_inner__"
  FROM analytics.air_quality_daily_cleaned
  WHERE
    "CITY" IN ('jeju', 'busan', 'gwangju', 'daejeon', 'daegu', 'seoul', 'incheon')
  GROUP BY
    "POLLUTANT"
  ORDER BY
    "mme_inner__" DESC
  LIMIT 6
) AS series_limit
  ON "POLLUTANT" = "POLLUTANT__"
WHERE
  "CITY" IN ('jeju', 'busan', 'gwangju', 'daejeon', 'daegu', 'seoul', 'incheon')
GROUP BY
  "POLLUTANT",
  DATE_TRUNC('MONTH', "DATE")
ORDER BY
  "AVG(VALUE)" DESC
LIMIT 50000 /* 6dcd92a04feb50f14bbcf07c661680ba; */