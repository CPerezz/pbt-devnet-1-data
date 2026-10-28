-- A1. Valid runs (plus the besu-2 old-build reference): T_wait, T_core, T_replay, T_total, leaves/s.
-- Leaves and anchors come from each run's completion log line (markers).
WITH leaves(run_id, anchor, leaves, root) AS (VALUES
    ('geth-1/import', 24453558, 3551663466, '0xd4597e…b14eab'),
    ('geth-2/import', 24453558, 3551663466, '0xd4597e…b14eab'),
    ('geth-3/self', 24503830, 3602991178, '0xbbda4f…527323'),
    ('nethermind-1/import', 24453558, 3551663466, '0xd4597e…b14eab'),
    ('nethermind-1/self', 24534182, 3609274444, '0xa477d0…b05924'),
    ('nethermind-2/import', 24453558, 3551663466, '0xd4597e…b14eab'),
    ('besu-1/import', 24453558, 3551663466, '0xd4597e…b14eab'),
    ('besu-1/self', 24535469, 3610963005, '0x8acd5a…744a7c'),
    ('besu-2/import', 24453558, 3551663466, '0xd4597e…b14eab'),
    ('besu-2/import-run1', 24453558, 3551663466, '0xd4597e…b14eab'))
SELECT r.run_id, r.host, r.client, r.method, r.build, r.status, r.note,
       CASE r.host WHEN 'geth-2' THEN 32 WHEN 'nethermind-2' THEN 32 WHEN 'besu-2' THEN 32 WHEN 'erigon-1' THEN 32 ELSE 16 END AS cap_gib,
       r.deploy, r.start, r.complete, r.synced, r.synced_source,
       r.t_wait_s, r.t_core_s, r.t_replay_s, r.t_total_s,
       l.anchor, l.leaves, l.root,
       r.replay_blocks, r.replay_blocks / nullif(r.t_replay_s, 0) AS replay_blk_s,
       l.leaves / nullif(r.t_core_s, 0) AS leaves_per_s
FROM runs r JOIN leaves l USING (run_id)
ORDER BY r.client, r.host, r.start
