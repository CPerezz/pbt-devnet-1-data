-- C1. Resources per A1 run over its core window [start, complete) and replay window [complete, synced).
-- Memory as a fraction of the container cap; disk GiB read/written on the root device; datadir growth (scratch).
WITH w AS (SELECT run_id, host, cap_gib, 'core' AS span, start AS t0, complete AS t1 FROM a1_runs
           UNION ALL SELECT run_id, host, cap_gib, 'replay', complete, synced FROM a1_runs WHERE synced IS NOT NULL)
SELECT w.run_id, w.span, w.t0, w.t1,
       max(r.mem_rss) / (w.cap_gib * 2^30) AS rss_peak_frac,
       quantile_cont(r.mem_rss, 0.95) / (w.cap_gib * 2^30) AS rss_p95_frac,
       avg(r.host_cpu_busy) AS host_cpu, quantile_cont(r.host_cpu_busy, 0.95) AS host_cpu_p95,
       avg(r.psi_io_wait) AS psi_io, avg(r.psi_mem_stall) AS psi_mem_full, avg(r.disk_util) AS disk_util,
       sum(r.disk_read_bps * 30) / 2^30 AS read_gib, sum(r.disk_write_bps * 30) / 2^30 AS written_gib,
       (max(r.datadir) - arg_min(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL)) / 2^30 AS datadir_scratch_gib,
       (arg_max(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL) - arg_min(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL)) / 2^30 AS datadir_delta_gib,
       min(r.fs_free) / 2^30 AS fs_free_min_gib,
       max(r.mig_input) / 2^30 AS input_gib,
       count(r.mem_rss) * 1.0 / nullif(count(*), 0) AS mem_coverage
FROM w JOIN resources r ON r.host = w.host AND r.t >= w.t0 AND r.t < w.t1
GROUP BY w.run_id, w.span, w.t0, w.t1, w.cap_gib
ORDER BY w.run_id, w.span
