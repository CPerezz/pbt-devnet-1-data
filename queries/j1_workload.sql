-- J1. Workload per block: devnet eras (headers/new_payload for gas and txs; stratified BAL sample for state access)
-- vs mainnet (xatu canonical diffs: slots written = distinct storage slots changed per block).
SELECT 'devnet' AS chain, era AS period, count(gas_used) AS blocks, count(slot_writes) AS bal_samples,
       avg(gas_used) / 1e6 AS mgas, avg(txs) AS txs,
       avg(accounts) AS accounts, avg(accounts_written) AS accounts_written,
       avg(slot_writes) AS slot_writes, quantile_cont(slot_writes, 0.5) AS slot_writes_p50, quantile_cont(slot_writes, 0.95) AS slot_writes_p95,
       avg(slot_reads) AS slot_reads, avg(code_changes) AS code_changes, NULL::DOUBLE AS slots_created
FROM workload_devnet GROUP BY era
UNION ALL
SELECT 'mainnet', "window", count(*), count(*), avg(gas_used) / 1e6, avg(txs),
       NULL, avg(accounts_written), avg(slots_written), quantile_cont(slots_written, 0.5), quantile_cont(slots_written, 0.95),
       NULL, avg(contracts), avg(slots_created)
FROM workload_mainnet GROUP BY "window"
ORDER BY chain DESC, period
