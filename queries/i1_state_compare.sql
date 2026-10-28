-- I1 / M1. Steady-state cost per node and state, for the two-tree cost (MPT only vs synced with both trees;
-- erigon hex vs hex+bin) and the XEN load (synced vs synced (load)):
-- latency per block and per MGas, memory, datadir growth, bytes written per block.
SELECT d.host, d.state, d.blocks, d.p50_ms, d.p95_ms, d.mgas_per_block,
       d.p50_ms / nullif(d.mgas_per_block, 0) AS p50_ms_per_mgas, d.mgas_s,
       e.rss_mean_frac, e.rss_mean_frac * e.cap AS rss_mean_gib, e.rss_peak_frac,
       f.growth_gib_day, f.written_gib * 2^30 / nullif(d.blocks, 0) / 2^20 AS written_mib_per_block,
       (epoch(d.t1) - epoch(d.t0)) / 3600 AS hours
FROM d1_latency d
LEFT JOIN e1_memory e USING (host, state)
LEFT JOIN f1_disk f USING (host, state)
WHERE d.state IN ('MPT only', 'MPT only (hex)', 'synced', 'synced (load)', 'dual (hex+bin)', 'dual (hex+bin) (load)')
ORDER BY d.host, d.t0
