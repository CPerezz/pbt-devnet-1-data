-- A3. Effective time per node and method: first attempt start → first time at the head with a PBT,
-- and → the counted run's synced. Attempts = every run row for that node/method (failed, superseded, valid).
WITH g AS (SELECT host, client, CASE WHEN method = 'self' THEN 'self' ELSE 'import' END AS method,
                  count(*) AS attempts, min(start) AS first_start, min(synced) AS first_synced,
                  max(synced) FILTER (WHERE status = 'valid') AS valid_synced,
                  max(t_total_s) FILTER (WHERE status = 'valid') AS best_t_total_s
           FROM runs WHERE method IN ('import', 'self') GROUP BY 1, 2, 3)
SELECT g.*, epoch(first_synced) - epoch(first_start) AS effective_first_head_s,
       epoch(valid_synced) - epoch(first_start) AS effective_valid_s,
       (epoch(first_synced) - epoch(first_start)) / best_t_total_s AS effective_over_best
FROM g ORDER BY client, host, method
