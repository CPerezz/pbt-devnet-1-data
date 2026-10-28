-- J2. Mainnet projection per import run (model, plan §8.B), against the same client's self-conversion:
--   T_total(import, anchor age A days) = T_core_i + (A·7200 + T_core_i/12) / (r − 1/12)
--   T_total(self)                      = T_core_s + (T_core_s/12) / (r − 1/12)
-- r_synthetic = replay rate measured on the synthetic window (~5.4 k slot writes per block: conservative);
-- r_mainnet = rate measured on live blocks (~665 writes) scaled to mainnet's ~833 writes per block (linear in writes).
-- breakeven_days = anchor age above which self-conversion reaches the head first (≤ 0: self always wins).
WITH er AS (SELECT run_id, max(blk_s) FILTER (WHERE era = 'synthetic') AS r_synthetic, max(blk_s) FILTER (WHERE era = 'live') AS r_live
            FROM d3_replay_by_era GROUP BY 1),
     sw AS (SELECT (SELECT avg(slot_writes) FROM workload_devnet WHERE era = 'live, before load') AS sw_live,
                   (SELECT avg(slots_written) FROM workload_mainnet WHERE "window" = 'recent') AS sw_main),
     slf AS (SELECT client, min(t_core_s) AS t_core_s FROM a1_runs WHERE method = 'self' AND status = 'valid' GROUP BY 1),
     x AS (SELECT a.run_id, a.client, a.cap_gib, a.t_core_s AS t_core_i, slf.t_core_s, er.r_synthetic, er.r_live,
                  er.r_live * sw.sw_live / sw.sw_main AS r_mainnet
           FROM a1_runs a JOIN er USING (run_id) JOIN slf USING (client) CROSS JOIN sw
           WHERE a.method = 'import' AND a.status = 'valid')
SELECT x.*,
       x.t_core_s + (x.t_core_s / 12) / (x.r_mainnet - 1 / 12) AS t_total_self_s,
       x.t_core_i + (0 * 7200 + x.t_core_i / 12) / (x.r_mainnet - 1 / 12) AS t_total_import_0d_s,
       x.t_core_i + (1 * 7200 + x.t_core_i / 12) / (x.r_mainnet - 1 / 12) AS t_total_import_1d_s,
       x.t_core_i + (7 * 7200 + x.t_core_i / 12) / (x.r_mainnet - 1 / 12) AS t_total_import_7d_s,
       ((x.t_core_s + (x.t_core_s / 12) / (x.r_mainnet - 1 / 12) - x.t_core_i) * (x.r_mainnet - 1 / 12) - x.t_core_i / 12) / 7200 AS breakeven_days,
       ((x.t_core_s + (x.t_core_s / 12) / (x.r_synthetic - 1 / 12) - x.t_core_i) * (x.r_synthetic - 1 / 12) - x.t_core_i / 12) / 7200 AS breakeven_days_conservative
FROM x ORDER BY client, cap_gib
