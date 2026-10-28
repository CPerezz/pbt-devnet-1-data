-- H3. Missed slots attributed per node: each hour, a node that validated only a share v of the network's blocks
-- is charged slots × (1 − v) / 9 (its validators' expected share of proposals). Compare with the network's missed total.
WITH a AS (SELECT host, sum(slots * (1 - coalesce(valid_share, 0)) / 9) AS attributed_slots,
                  sum((1 - coalesce(valid_share, 0))) AS node_hours_down
           FROM h2_node_hourly GROUP BY host)
SELECT a.*, (SELECT sum(missed) FROM h1_network_hourly) AS network_missed, (SELECT sum(slots) FROM h1_network_hourly) AS network_slots
FROM a ORDER BY attributed_slots DESC
