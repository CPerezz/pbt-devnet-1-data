-- K2. Time to recover after each crash (cgroup OOM kill, JVM heap OOM, SIGSEGV): the next process start (restart delay)
-- and the next VALID engine_newPayload answer from that node (back to validating blocks).
WITH f AS (SELECT host, ts, kind FROM failures WHERE kind IN ('oom_kill', 'jvm_heap_oom', 'segv'))
SELECT f.host, f.ts, f.kind,
       (SELECT min(l.ts) FROM lifecycle l WHERE l.host = f.host AND l.kind = 'start' AND l.ts >= f.ts - INTERVAL 1 MINUTE) AS restarted,
       (SELECT min(b.t) FROM blocks_node b WHERE b.host = f.host AND b.status = 'VALID' AND b.t > f.ts + INTERVAL 30 SECOND) AS valid_again,
       epoch((SELECT min(l.ts) FROM lifecycle l WHERE l.host = f.host AND l.kind = 'start' AND l.ts >= f.ts - INTERVAL 1 MINUTE)) - epoch(f.ts) AS restart_s,
       epoch((SELECT min(b.t) FROM blocks_node b WHERE b.host = f.host AND b.status = 'VALID' AND b.t > f.ts + INTERVAL 30 SECOND)) - epoch(f.ts) AS recover_s
FROM f ORDER BY f.host, f.ts
