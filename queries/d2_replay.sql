-- D2. Replay (anchor → head) per run: blocks, time, blocks/s, MGas/s and BAL slot writes/s.
-- Gas per replayed block from the live chain (xatu new_payload) and the sampled headers/BALs before genesis;
-- slot writes use the stratified BAL sample mean of each era (bal_summary), weighted by the blocks replayed per era.
WITH r AS (SELECT run_id, host, client, method, status, anchor, complete, synced, t_replay_s, replay_blocks,
                  anchor + replay_blocks AS head FROM a1_runs WHERE synced IS NOT NULL),
     era AS (SELECT era, min(number) AS lo, max(number) AS hi, avg(gas_used) AS gas, avg(slot_writes) AS sw
             FROM workload_devnet GROUP BY era),
     w AS (SELECT r.run_id, e.era, greatest(0, least(r.head, e.hi) - greatest(r.anchor + 1, e.lo) + 1) AS n, e.gas, e.sw
           FROM r CROSS JOIN era e),
     agg AS (SELECT run_id, sum(n * gas) AS gas, sum(n * sw) AS sw, sum(n) AS n,
                    string_agg(era || ': ' || n, ', ' ORDER BY era) FILTER (WHERE n > 0) AS blocks_by_era
             FROM w GROUP BY run_id)
SELECT r.run_id, r.host, r.client, r.method, r.status, r.anchor, r.head, r.replay_blocks, r.t_replay_s,
       r.replay_blocks / r.t_replay_s AS blk_s,
       agg.gas / 1e6 / r.t_replay_s AS mgas_s,
       agg.sw / r.t_replay_s AS slot_writes_s,
       agg.gas / nullif(agg.n, 0) / 1e6 AS mgas_per_block,
       agg.sw / nullif(agg.n, 0) AS slot_writes_per_block,
       agg.blocks_by_era
FROM r JOIN agg USING (run_id)
ORDER BY r.client, r.host, r.run_id
