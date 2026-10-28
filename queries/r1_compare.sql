-- B1 / C / O1. One comparison row per counted run: time, throughput, memory, CPU, disk, failures.
-- Used for self vs import (B), client vs client (C) and 16 vs 32 GiB (O).
SELECT a.run_id, a.host, a.client, a.method, a.status, a.build, a.cap_gib,
       a.t_wait_s, a.t_core_s, a.t_replay_s, a.t_total_s, a.replay_blocks, a.replay_blk_s, a.leaves, a.leaves_per_s,
       v.verification_s, v.verification_s / a.t_core_s AS verification_share,
       c.rss_peak_frac AS core_rss_peak, c.rss_p95_frac AS core_rss_p95, c.host_cpu AS core_cpu, c.read_gib AS core_read_gib,
       c.written_gib AS core_written_gib, c.datadir_scratch_gib, c.input_gib, c.mem_coverage AS core_mem_coverage,
       rp.rss_peak_frac AS replay_rss_peak, rp.host_cpu AS replay_cpu,
       d.mgas_s AS replay_mgas_s, d.slot_writes_s AS replay_slot_writes_s,
       (SELECT count(*) FROM failures f WHERE f.host = a.host AND f.kind IN ('oom_kill', 'jvm_heap_oom')
          AND f.ts >= a.start AND f.ts < coalesce(a.synced, a.complete)) AS ooms_in_run
FROM a1_runs a
LEFT JOIN (SELECT run_id, sum(dur_s) FILTER (WHERE class = 'verification') AS verification_s FROM a2_phases GROUP BY 1) v USING (run_id)
LEFT JOIN c1_run_resources c ON c.run_id = a.run_id AND c.span = 'core'
LEFT JOIN c1_run_resources rp ON rp.run_id = a.run_id AND rp.span = 'replay'
LEFT JOIN d2_replay d ON d.run_id = a.run_id
ORDER BY a.client, a.host, a.start
