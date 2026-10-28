-- A2. Phases of the A1 runs: duration, share of T_core, and class.
-- verification = work that only checks (preimage file, geth's code check, check 2, geth's read-back);
-- construction = keys/leaves/scan/build/write (check 1 is the root compare at the end of the build).
SELECT p.run_id, a.host, a.client, a.method, a.status, left(p.phase, 2) AS u, p.phase, p.start, p."end", p.dur_s,
       CASE WHEN left(p.phase, 2) IN ('U0', 'U7') THEN NULL ELSE p.dur_s / a.t_core_s END AS share_core,
       CASE WHEN left(p.phase, 2) = 'U0' THEN 'wait'
            WHEN left(p.phase, 2) = 'U7' THEN 'replay'
            WHEN p.phase LIKE 'U1-U3%' THEN 'construction'
            WHEN left(p.phase, 2) = 'U1' AND a.method = 'import' THEN 'verification'
            WHEN left(p.phase, 2) = 'U5' THEN 'verification'
            WHEN left(p.phase, 2) = 'U4' AND a.client = 'geth' THEN 'verification'
            WHEN left(p.phase, 2) = 'U6' AND a.client = 'geth' THEN 'verification'
            WHEN left(p.phase, 2) = 'U6' THEN 'commit'
            ELSE 'construction' END AS class
FROM phases p JOIN a1_runs a USING (run_id)
ORDER BY a.client, a.host, p.start
