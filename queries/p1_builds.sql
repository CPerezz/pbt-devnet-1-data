-- P1. Builds per node, from the version string each process logged at start.
SELECT host, regexp_extract(build, '([0-9a-f]{7,})', 1) AS build, min(ts) AS first_start, max(ts) AS last_start, count(*) AS starts
FROM lifecycle WHERE kind = 'start' AND build IS NOT NULL
GROUP BY 1, 2 ORDER BY host, first_start
