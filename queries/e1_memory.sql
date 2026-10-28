-- E1. Memory per node and state: anonymous memory (rss) and working set as % of the container cap,
-- time above 85 % / 95 % of cap (30 s samples), the minimum headroom, and cgroup OOM kills (failure ledger).
WITH cap(host, cap) AS (VALUES ('geth-1', 16), ('geth-2', 32), ('geth-3', 16), ('nethermind-1', 16), ('nethermind-2', 32),
                               ('besu-1', 16), ('besu-2', 32), ('erigon-1', 32), ('erigon-2', 16)),
     oom AS (SELECT s.host, s.state, count(*) AS oom_kills
             FROM failures f JOIN c0_states s ON s.host = f.host AND f.ts >= s.t0 AND f.ts < s.t1
             WHERE f.kind = 'oom_kill' GROUP BY 1, 2),
     m AS (SELECT s.host, s.state, c.cap, count(*) AS samples,
                  max(r.mem_rss) / (2^30) AS rss_peak_gib,
                  max(r.mem_rss) / (c.cap * 2^30) AS rss_peak_frac,
                  quantile_cont(r.mem_rss, 0.95) / (c.cap * 2^30) AS rss_p95_frac,
                  avg(r.mem_rss) / (c.cap * 2^30) AS rss_mean_frac,
                  max(r.mem_ws) / (c.cap * 2^30) AS ws_peak_frac,
                  count(*) FILTER (WHERE r.mem_rss > 0.85 * c.cap * 2^30) * 30 AS above85_s,
                  count(*) FILTER (WHERE r.mem_rss > 0.95 * c.cap * 2^30) * 30 AS above95_s,
                  (c.cap * 2^30 - max(r.mem_rss)) / 2^30 AS min_headroom_gib,
                  min(s.t0) AS t0
           FROM resources r
           JOIN c0_states s ON s.host = r.host AND r.t >= s.t0 AND r.t < s.t1
           JOIN cap c ON c.host = r.host
           WHERE r.mem_rss IS NOT NULL
           GROUP BY s.host, s.state, c.cap)
SELECT m.*, coalesce(oom.oom_kills, 0) AS oom_kills FROM m LEFT JOIN oom USING (host, state) ORDER BY host, t0
