-- F1. Disk per node and state: datadir size at the start/peak/end of the state (eth-docker volume usage,
-- sampled every ~30 min), growth per day, root-filesystem free space minimum, and bytes written.
SELECT s.host, s.state, min(s.t0) AS t0, max(s.t1) AS t1,
       arg_min(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL) / 2^30 AS datadir_start_gib,
       max(r.datadir) / 2^30 AS datadir_peak_gib,
       arg_max(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL) / 2^30 AS datadir_end_gib,
       (arg_max(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL) - arg_min(r.datadir, r.ts) FILTER (WHERE r.datadir IS NOT NULL))
         / 2^30 / nullif((max(r.ts) FILTER (WHERE r.datadir IS NOT NULL) - min(r.ts) FILTER (WHERE r.datadir IS NOT NULL)) / 86400, 0) AS growth_gib_day,
       min(r.fs_free) / 2^30 AS fs_free_min_gib,
       arg_min(r.t, r.fs_free) AS fs_free_min_t,
       sum(r.disk_write_bps * 30) / 2^30 AS written_gib,
       sum(r.disk_read_bps * 30) / 2^30 AS read_gib
FROM resources r JOIN c0_states s ON s.host = r.host AND r.t >= s.t0 AND r.t < s.t1
GROUP BY 1, 2 ORDER BY 1, min(s.t0)
