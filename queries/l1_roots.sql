-- L1. PBT root agreement per node (watch samples: every node's binary root at a shared sampled height).
-- comparisons = sampled heights where this node and ≥ 1 other node reported a root; divergent = heights with > 1 distinct root.
WITH h AS (SELECT t, height, nodes_with_root, distinct_roots, unnest(hosts) AS host FROM roots)
SELECT host, min(to_timestamp(t)) FILTER (WHERE nodes_with_root >= 2) AS first_agreement,
       count(*) FILTER (WHERE nodes_with_root >= 2) AS comparisons,
       count(*) FILTER (WHERE distinct_roots > 1) AS divergent,
       max(to_timestamp(t)) AS last_sample
FROM h GROUP BY host ORDER BY host
