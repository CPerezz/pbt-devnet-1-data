-- H1. Network health per hour: slots, missed slots (no block seen by any sentry and none in beacon_api_slot),
-- blocks without a payload, finality lag (head epoch − last finalized epoch, from xatu finalized events), reorgs.
WITH s AS (SELECT date_trunc('hour', slot_start) AS hour, count(*) AS slots, sum(missed::INT) AS missed,
                  sum((NOT missed AND sentries_payload = 0)::INT) AS no_payload,
                  quantile_cont(first_seen_ms, 0.5) AS block_seen_p50_ms
           FROM network WHERE slot_start >= TIMESTAMPTZ '2026-10-22 09:00:00+00' GROUP BY 1),
     f AS (SELECT t, epoch FROM network_events WHERE kind = 'finalized'),
     lag AS (SELECT date_trunc('hour', n.slot_start) AS hour,
                    max(n.slot // 32 - (SELECT max(f.epoch) FROM f WHERE f.t <= n.slot_start)) AS max_finality_lag_epochs
             FROM network n WHERE n.slot % 32 = 0 AND n.slot_start >= TIMESTAMPTZ '2026-10-22 09:00:00+00' GROUP BY 1),
     ro AS (SELECT date_trunc('hour', t) AS hour, count(*) AS reorgs, max(depth) AS max_reorg_depth FROM network_events WHERE kind = 'reorg' GROUP BY 1)
SELECT s.*, lag.max_finality_lag_epochs, coalesce(ro.reorgs, 0) AS reorgs, ro.max_reorg_depth
FROM s LEFT JOIN lag USING (hour) LEFT JOIN ro USING (hour) ORDER BY hour
