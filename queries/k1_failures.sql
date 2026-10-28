-- K1. Failure ledger per node: cgroup OOM kills, JVM heap OOMs, SIGSEGVs, unclean restarts (start with no clean stop),
-- clean stop→start pairs (operator restarts / redeploys), attempts that restarted from scratch, builds used.
WITH f AS (SELECT host, count(*) FILTER (WHERE kind = 'oom_kill') AS oom_kills,
                  count(*) FILTER (WHERE kind = 'jvm_heap_oom') AS jvm_heap_oom,
                  count(*) FILTER (WHERE kind = 'segv') AS segv,
                  count(*) FILTER (WHERE kind = 'crash_restart') AS unclean_restarts,
                  count(*) FILTER (WHERE kind = 'replay_stall') AS replay_stalls,
                  sum(count) FILTER (WHERE kind = 'shadow_stopped') AS shadow_stop_lines
           FROM failures GROUP BY host),
     lc AS (SELECT host, count(*) FILTER (WHERE kind = 'start') AS starts, count(*) FILTER (WHERE kind = 'stop') AS clean_stops,
                   count(DISTINCT regexp_extract(build, '([0-9a-f]{7,})', 1)) FILTER (WHERE build IS NOT NULL) AS builds
            FROM lifecycle GROUP BY host),
     r AS (SELECT host, count(*) FILTER (WHERE status = 'failed') AS failed_attempts,
                  count(*) FILTER (WHERE status IN ('valid', 'reference', 'superseded')) AS completed_runs
           FROM runs GROUP BY host)
SELECT lc.host, coalesce(f.oom_kills, 0) AS oom_kills, coalesce(f.jvm_heap_oom, 0) AS jvm_heap_oom, coalesce(f.segv, 0) AS segv,
       coalesce(f.unclean_restarts, 0) AS unclean_restarts, lc.starts, lc.clean_stops, coalesce(r.failed_attempts, 0) AS failed_attempts,
       coalesce(r.completed_runs, 0) AS completed_runs, lc.builds, coalesce(f.replay_stalls, 0) AS replay_stalls,
       coalesce(f.shadow_stop_lines, 0) AS shadow_stop_lines
FROM lc LEFT JOIN f USING (host) LEFT JOIN r USING (host) ORDER BY host
