-- I2. What the client says its memory is made of, mean per node and state (GiB; go_heap_objects is a count).
SELECT s.host, s.state, i.component, avg(i.value) / CASE WHEN i.component = 'go_heap_objects' THEN 1 ELSE 2^30 END AS mean_gib,
       max(i.value) / CASE WHEN i.component = 'go_heap_objects' THEN 1 ELSE 2^30 END AS max_gib
FROM internals i JOIN c0_states s ON s.host = i.host AND to_timestamp(i.ts) >= s.t0 AND to_timestamp(i.ts) < s.t1
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
