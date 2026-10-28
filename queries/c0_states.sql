-- C0. What each node was doing, as intervals [t0, t1): MPT only → migrating / failed attempt → replay → synced.
-- erigon states come from its own markers (hex+bin restarts, "binary commitment domain is stopped").
-- Every interval is split at the XEN load start (first block ≥ load_start_block): suffix " (load)".
WITH lt AS (SELECT min(t) AS t FROM workload_devnet WHERE number >= (SELECT load_start_block FROM boundaries) AND t IS NOT NULL),
     wend AS (SELECT to_timestamp(max(ts)) AS t FROM resources),
     ev AS (
        SELECT host, TIMESTAMPTZ '2026-10-22 09:00:00+00' AS t, CASE WHEN host LIKE 'erigon%' THEN 'MPT only (hex)' ELSE 'MPT only' END AS state, NULL AS run_id
        FROM (SELECT DISTINCT host FROM resources)
        UNION ALL SELECT host, start, CASE WHEN status = 'failed' THEN 'failed attempt' ELSE 'migrating' END, run_id
                  FROM runs WHERE method IN ('import', 'self') AND start IS NOT NULL
        UNION ALL SELECT host, complete, 'replay', run_id FROM runs WHERE complete IS NOT NULL AND status <> 'failed'
        UNION ALL SELECT host, synced, 'synced', run_id FROM runs WHERE synced IS NOT NULL
        UNION ALL SELECT host, t::TIMESTAMPTZ, state, run_id FROM (VALUES
            ('erigon-1', '2026-10-25 06:26:52+00', 'dual (hex+bin)', 'erigon-1/dual'),
            ('erigon-2', '2026-10-23 05:49:17+00', 'dual attempt', 'erigon-2/dual-A'),
            ('erigon-2', '2026-10-23 12:55:53+00', 'shadow stopped', 'erigon-2/dual-A'),
            ('erigon-2', '2026-10-23 13:59:50+00', 'dual attempt', 'erigon-2/dual-B'),
            ('erigon-2', '2026-10-24 06:16:37+00', 'shadow stopped', 'erigon-2/dual-B'),
            ('erigon-2', '2026-10-25 05:17:22+00', 'dual (hex+bin)', 'erigon-2/dual')) v(host, t, state, run_id)),
     iv AS (SELECT host, t AS t0, coalesce(lead(t) OVER (PARTITION BY host ORDER BY t), (SELECT t FROM wend)) AS t1, state, run_id FROM ev)
SELECT host, t0, least(t1, (SELECT t FROM lt)) AS t1, state, run_id FROM iv WHERE t0 < (SELECT t FROM lt) AND t1 > t0
UNION ALL
SELECT host, greatest(t0, (SELECT t FROM lt)), t1, state || ' (load)', run_id FROM iv WHERE t1 > (SELECT t FROM lt) AND t1 > t0
ORDER BY host, t0
