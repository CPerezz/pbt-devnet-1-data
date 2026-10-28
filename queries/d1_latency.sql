-- D1. Steady-state block processing per node and state: engine_newPayload latency (xatu, first call per block)
-- and MGas/s = gas / processing time. VALID share shows when the EL answered SYNCING/ACCEPTED instead.
SELECT s.host, s.state, count(*) AS blocks,
       quantile_cont(b.np_ms, 0.5) AS p50_ms, quantile_cont(b.np_ms, 0.95) AS p95_ms, quantile_cont(b.np_ms, 0.99) AS p99_ms,
       avg(b.gas_used) / 1e6 AS mgas_per_block,
       sum(b.gas_used) FILTER (WHERE b.status = 'VALID') / 1e3 / nullif(sum(b.np_ms) FILTER (WHERE b.status = 'VALID'), 0) AS mgas_s,
       avg((b.status = 'VALID')::INT) AS valid_share,
       min(s.t0) AS t0, max(s.t1) AS t1
FROM blocks_node b JOIN c0_states s ON s.host = b.host AND b.t >= s.t0 AND b.t < s.t1
GROUP BY 1, 2 ORDER BY 1, min(s.t0)
