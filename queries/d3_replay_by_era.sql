-- D3. Replay rate per run on synthetic (N+1 → genesis, ~5.4 k slot writes per block) vs live blocks (~665 before the load),
-- from consecutive replay cursor samples (watch for geth/Nethermind, every 128-block roll line for Besu), gaps ≤ 10 min.
WITH d AS (SELECT run_id, src, t, cursor,
                  (cursor - lag(cursor) OVER w) AS dn, epoch(t) - epoch(lag(t) OVER w) AS dt
           FROM replay WINDOW w AS (PARTITION BY run_id, src ORDER BY t))
SELECT run_id, CASE WHEN cursor < (SELECT first_live_block FROM boundaries) THEN 'synthetic' ELSE 'live' END AS era,
       count(*) AS intervals, sum(dn) / sum(dt) AS blk_s, sum(dt) AS seconds
FROM d WHERE dt BETWEEN 1 AND 600 AND dn >= 0
GROUP BY 1, 2 ORDER BY 1, 2
