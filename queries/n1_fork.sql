-- N1. Fork activation per node (binaryTrieTime 2026-10-29 09:00 UTC): newPayload latency of the last 300 pre-fork blocks vs
-- the first 10 / first 300 post-fork blocks (VALID answers only: a node catching up after a crash answers SYNCING in
-- microseconds), non-VALID answers in the first hour, and memory / datadir 24 h before vs 24 h after.
-- Empty until the data window passes the fork.
WITH f AS (SELECT TIMESTAMPTZ '2026-10-29 09:00:00+00' AS t),
     b AS (SELECT host, t, np_ms, status, row_number() OVER (PARTITION BY host, t >= (SELECT t FROM f) ORDER BY
                  CASE WHEN t >= (SELECT t FROM f) THEN epoch(t) ELSE -epoch(t) END) AS k
           FROM blocks_node WHERE t BETWEEN (SELECT t FROM f) - INTERVAL 2 HOUR AND (SELECT t FROM f) + INTERVAL 24 HOUR),
     lat AS (SELECT host,
                    quantile_cont(np_ms, 0.5) FILTER (WHERE t < (SELECT t FROM f) AND k <= 300 AND status = 'VALID') AS pre_p50_ms,
                    quantile_cont(np_ms, 0.5) FILTER (WHERE t >= (SELECT t FROM f) AND k <= 10 AND status = 'VALID') AS first10_p50_ms,
                    max(np_ms) FILTER (WHERE t >= (SELECT t FROM f) AND k <= 10 AND status = 'VALID') AS first10_max_ms,
                    quantile_cont(np_ms, 0.5) FILTER (WHERE t >= (SELECT t FROM f) AND k <= 300 AND status = 'VALID') AS post_p50_ms,
                    count(*) FILTER (WHERE t >= (SELECT t FROM f) AND t < (SELECT t FROM f) + INTERVAL 1 HOUR AND status <> 'VALID') AS non_valid_first_hour,
                    min(t) FILTER (WHERE t >= (SELECT t FROM f) AND status = 'VALID') AS first_valid_pbt_block_t
             FROM b GROUP BY host),
     res AS (SELECT host,
                    avg(mem_rss) FILTER (WHERE t BETWEEN (SELECT t FROM f) - INTERVAL 24 HOUR AND (SELECT t FROM f)) / 2^30 AS rss_pre_gib,
                    avg(mem_rss) FILTER (WHERE t BETWEEN (SELECT t FROM f) + INTERVAL 1 HOUR AND (SELECT t FROM f) + INTERVAL 25 HOUR) / 2^30 AS rss_post_gib,
                    arg_max(datadir, ts) FILTER (WHERE t < (SELECT t FROM f) AND datadir IS NOT NULL) / 2^30 AS datadir_pre_gib,
                    min(datadir) FILTER (WHERE t BETWEEN (SELECT t FROM f) AND (SELECT t FROM f) + INTERVAL 48 HOUR) / 2^30 AS datadir_post_min_gib
             FROM resources GROUP BY host)
SELECT lat.*, res.rss_pre_gib, res.rss_post_gib, res.datadir_pre_gib, res.datadir_post_min_gib,
       res.datadir_pre_gib - res.datadir_post_min_gib AS disk_released_gib
FROM lat JOIN res USING (host)
WHERE lat.first_valid_pbt_block_t IS NOT NULL
ORDER BY host
