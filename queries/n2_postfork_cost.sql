-- N2. Block processing on one binary tree vs two trees (MPT + shadow), per node: median / p95 engine_newPayload ms on
-- VALID blocks > 150 Mgas. pre = 10-28 10:11 → 14:11 (two trees under the XEN load, every node up); post = the chain's
-- last four hours, 10-30 10:11 → 14:11 (binary tree only; the geths on their fixed builds); post_prefix = 10-29 13:00 →
-- 10-30 09:00 (binary tree only, the geths still on the pre-fix build #79). Nodes down in a window have fewer blocks.
WITH w AS (SELECT 'pre' AS win, TIMESTAMPTZ '2026-10-28 10:11:00+00' AS t0, TIMESTAMPTZ '2026-10-28 14:11:00+00' AS t1
           UNION ALL SELECT 'post', TIMESTAMPTZ '2026-10-30 10:11:00+00', TIMESTAMPTZ '2026-10-30 14:12:00+00'
           UNION ALL SELECT 'post_prefix', TIMESTAMPTZ '2026-10-29 13:00:00+00', TIMESTAMPTZ '2026-10-30 09:00:00+00'),
     b AS (SELECT w.win, n.host, n.np_ms, n.gas_used FROM blocks_node n JOIN w ON n.t >= w.t0 AND n.t < w.t1
           WHERE n.status = 'VALID' AND n.gas_used > 150e6)
SELECT host,
       count(*) FILTER (WHERE win = 'pre') AS pre_blocks,
       quantile_cont(np_ms, 0.5) FILTER (WHERE win = 'pre') AS pre_p50_ms,
       quantile_cont(np_ms, 0.95) FILTER (WHERE win = 'pre') AS pre_p95_ms,
       count(*) FILTER (WHERE win = 'post') AS post_blocks,
       quantile_cont(np_ms, 0.5) FILTER (WHERE win = 'post') AS post_p50_ms,
       quantile_cont(np_ms, 0.95) FILTER (WHERE win = 'post') AS post_p95_ms,
       count(*) FILTER (WHERE win = 'post_prefix') AS post_prefix_blocks,
       quantile_cont(np_ms, 0.5) FILTER (WHERE win = 'post_prefix') AS post_prefix_p50_ms,
       quantile_cont(np_ms, 0.5) FILTER (WHERE win = 'post') / quantile_cont(np_ms, 0.5) FILTER (WHERE win = 'pre') AS post_over_pre
FROM b GROUP BY host ORDER BY host
