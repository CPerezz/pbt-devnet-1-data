-- H4. Network-wide view per hour, free of single-sentry artifacts: finality distance from the most advanced sentry
-- (head epoch − max finalized epoch seen by any sentry), the longest stretch of the hour without a new finalized epoch,
-- participation as the watch's nodes reported it (median of the polls), how many nodes answered VALID for most of the
-- hour, and reorgs seen by a majority of sentries (a resyncing node's local reorg is not a network reorg).
WITH hrs AS (SELECT unnest(generate_series(TIMESTAMPTZ '2026-10-22 09:00:00+00', TIMESTAMPTZ '2026-10-30 14:00:00+00', INTERVAL 1 HOUR)) AS hour),
     f AS (SELECT epoch, min(t) AS t FROM network_events WHERE kind = 'finalized' GROUP BY epoch),
     fin AS (SELECT h.hour, max(n.slot // 32 - (SELECT max(f.epoch) FROM f WHERE f.t <= n.slot_start)) AS max_finality_lag_epochs
             FROM hrs h JOIN network n ON n.slot_start >= h.hour AND n.slot_start < h.hour + INTERVAL 1 HOUR AND n.slot % 32 = 0 GROUP BY 1),
     stall AS (SELECT h.hour, max(epoch(f2.t) - epoch(f1.t)) / 60.0 AS longest_gap_min
               FROM hrs h JOIN (SELECT t, lead(t) OVER (ORDER BY epoch) AS t_next FROM f) f1(t, tn) ON f1.t < h.hour + INTERVAL 1 HOUR AND f1.tn >= h.hour
               JOIN LATERAL (SELECT f1.tn AS t) f2 ON TRUE GROUP BY 1),
     part AS (SELECT date_trunc('hour', to_timestamp(t)) AS hour, quantile_cont(part, 0.5) AS participation_p50, min(part) AS participation_min
              FROM 'data/raw/watch/watch_samples.parquet' WHERE part IS NOT NULL AND part > 0 GROUP BY 1),
     val AS (SELECT hour, count(*) FILTER (WHERE valid_share >= 0.5) AS nodes_validating FROM h2_node_hourly GROUP BY 1),
     ro AS (SELECT date_trunc('hour', t) AS hour, count(*) FILTER (WHERE sentries >= 5) AS network_reorgs,
                   max(depth) FILTER (WHERE sentries >= 5) AS max_network_reorg_depth, count(*) AS sentry_reorg_events
            FROM network_events WHERE kind = 'reorg' GROUP BY 1)
SELECT h.hour, fin.max_finality_lag_epochs, stall.longest_gap_min, part.participation_p50, part.participation_min,
       val.nodes_validating, coalesce(ro.network_reorgs, 0) AS network_reorgs, ro.max_network_reorg_depth, coalesce(ro.sentry_reorg_events, 0) AS sentry_reorg_events
FROM hrs h LEFT JOIN fin USING (hour) LEFT JOIN stall USING (hour) LEFT JOIN part USING (hour) LEFT JOIN val USING (hour) LEFT JOIN ro USING (hour)
ORDER BY h.hour
