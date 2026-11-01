-- D4. How long the BAL replay needs per day of mainnet, per counted run: the run's storage writes per second, scaled by
-- mainnet's ~833 distinct slot writes per block (xatu, blocks 26.24–26.29 M) and 7,200 blocks per day; live-era and
-- synthetic-era block rates from the cursor samples. Imports replayed mostly synthetic blocks (~5 k writes each),
-- self-conversions only live ones (~1 k under the XEN load).
WITH mn AS (SELECT avg(slots_written) AS writes_per_block FROM workload_mainnet WHERE "window" = 'recent')
SELECT d.run_id, d.host, d.client, d.method, d.cap_gib, d.replay_blocks, d.t_replay_s, d.blk_s, d.slot_writes_s, d.slot_writes_per_block,
       (SELECT blk_s FROM d3_replay_by_era e WHERE e.run_id = d.run_id AND e.era = 'live') AS live_blk_s,
       (SELECT blk_s FROM d3_replay_by_era e WHERE e.run_id = d.run_id AND e.era = 'synthetic') AS synthetic_blk_s,
       d.slot_writes_s / mn.writes_per_block AS mainnet_blocks_per_s,
       7200 * mn.writes_per_block / d.slot_writes_s / 60 AS minutes_per_mainnet_day
FROM (SELECT d2.*, r.cap_gib FROM d2_replay d2 JOIN a1_runs r USING (run_id)) d, mn
WHERE d.status = 'valid' AND d.slot_writes_s > 0 ORDER BY d.run_id
