-- G1. Resources per run phase (A2 phases × 30 s resources) and a bottleneck label.
-- host_cpu = busy cores of 16 on the whole droplet (node-exporter; covers the hours before cadvisor arrived);
-- cpu_cores = the EL container only. PSI = fraction of time some task waited (cpu, io) or all tasks stalled (memory);
-- disk_util = fraction of time the device was busy.
WITH x AS (
    SELECT p.run_id, p.host, p.client, p.method, p.u, p.phase, p.class, p.dur_s,
           avg(r.host_cpu_busy) AS host_cpu, avg(r.cpu_cores) AS cpu_cores, quantile_cont(r.host_cpu_busy, 0.95) AS host_cpu_p95,
           avg(r.psi_cpu) AS psi_cpu, avg(r.psi_io_wait) AS psi_io, avg(r.psi_io_stall) AS psi_io_full,
           avg(r.psi_mem_wait) AS psi_mem, avg(r.psi_mem_stall) AS psi_mem_full,
           avg(r.disk_util) AS disk_util, avg(r.disk_read_bps) AS read_bps, avg(r.disk_write_bps) AS write_bps,
           avg(r.disk_read_iops + r.disk_write_iops) AS iops, avg(r.disk_qtime) AS queue_depth, avg(r.majfault_ps) AS majfault_ps,
           max(r.mem_rss) AS rss_peak, count(r.ts) AS samples
    FROM a2_phases p LEFT JOIN resources r ON r.host = p.host AND r.t >= p.start AND r.t < p."end"
    WHERE p.u <> 'U0' GROUP BY ALL)
SELECT *, CASE WHEN samples < 4 THEN 'short'
               WHEN psi_mem_full > 0.05 OR majfault_ps > 1000 THEN 'memory'
               WHEN disk_util > 0.8 OR psi_io > 0.4 THEN 'io'
               WHEN host_cpu > 12 OR psi_cpu > 0.3 THEN 'cpu'
               WHEN host_cpu < 3 AND disk_util < 0.5 THEN 'serial'
               ELSE 'mixed' END AS bound
FROM x ORDER BY client, host, run_id, u
