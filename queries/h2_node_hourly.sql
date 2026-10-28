-- H2. Per node and hour: share of the network's blocks the node validated (engine_newPayload VALID, from xatu),
-- next to the network's missed-slot share. A node whose EL is down or answers SYNCING cannot propose either,
-- so its validators' slots go missing: expected missed ≈ Σ(nodes not validating) / 9.
WITH net AS (SELECT date_trunc('hour', slot_start) AS hour, count(*) AS slots, sum(missed::INT) AS missed,
                    count(*) FILTER (WHERE NOT missed) AS blocks
             FROM network WHERE slot_start >= TIMESTAMPTZ '2026-10-22 09:00:00+00' GROUP BY 1),
     nodes AS (SELECT DISTINCT host FROM blocks_node),
     v AS (SELECT host, date_trunc('hour', t) AS hour, count(*) FILTER (WHERE status = 'VALID') AS valid
           FROM blocks_node GROUP BY 1, 2)
SELECT nodes.host, net.hour, net.slots, net.missed, net.blocks, coalesce(v.valid, 0) AS valid,
       least(1.0, coalesce(v.valid, 0) / nullif(net.blocks, 0)) AS valid_share
FROM net CROSS JOIN nodes LEFT JOIN v ON v.host = nodes.host AND v.hour = net.hour
ORDER BY nodes.host, net.hour
