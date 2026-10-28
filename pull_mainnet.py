"""S8: mainnet per-block workload from xatu's default database (via panda).

Two 7-day windows of mainnet blocks:
- "recent": the last ~50,400 blocks xatu holds (mainnet today);
- "pre-fork": the 50,000 blocks right before the devnet left mainnet (24,350,001),
  i.e. the history the devnet's base state came from.
Per block: gas, txs, storage slots written (unique address+slot, the BAL-equivalent),
slots created / cleared, accounts written (any storage/balance/nonce diff), balance
and nonce diff counts, contracts created.
"""
import sys
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa

from common import RAW, ch, log, write_parquet

OUT = RAW / "mainnet"
ZERO = "match({c}, '^0x0*$')"
STEP = 2000


def chunk_sql(a: int, b: int) -> str:
    w = f"meta_network_name='mainnet' AND block_number BETWEEN {a} AND {b}"
    return f"""
SELECT blk.block_number AS block_number, toUnixTimestamp(blk.block_date_time) AS ts, blk.gas_used AS gas_used, blk.gas_limit AS gas_limit,
       t.txs AS txs, s.slot_events AS slot_events, s.slots_written AS slots_written, s.slots_created AS slots_created,
       s.slots_cleared AS slots_cleared, a.accounts_written AS accounts_written, bd.balance_diffs AS balance_diffs,
       nd.nonce_diffs AS nonce_diffs, c.contracts AS contracts
FROM (SELECT block_number, any(block_date_time) AS block_date_time, any(gas_used) AS gas_used, any(gas_limit) AS gas_limit FROM default.canonical_execution_block WHERE {w} GROUP BY block_number) AS blk
LEFT JOIN (SELECT block_number, uniqExact(transaction_hash) AS txs FROM default.canonical_execution_transaction WHERE {w} GROUP BY block_number) AS t USING block_number
LEFT JOIN (SELECT block_number, uniqExact(transaction_hash, internal_index) AS slot_events, uniqExact(address, slot) AS slots_written,
                  uniqExactIf((address, slot), {ZERO.format(c='from_value')} AND NOT {ZERO.format(c='to_value')}) AS slots_created,
                  uniqExactIf((address, slot), {ZERO.format(c='to_value')} AND NOT {ZERO.format(c='from_value')}) AS slots_cleared
           FROM default.canonical_execution_storage_diffs WHERE {w} GROUP BY block_number) AS s USING block_number
LEFT JOIN (SELECT block_number, uniqExact(address) AS accounts_written FROM (
             SELECT block_number, address FROM default.canonical_execution_storage_diffs WHERE {w}
             UNION ALL SELECT block_number, address FROM default.canonical_execution_balance_diffs WHERE {w}
             UNION ALL SELECT block_number, address FROM default.canonical_execution_nonce_diffs WHERE {w}) GROUP BY block_number) AS a USING block_number
LEFT JOIN (SELECT block_number, uniqExact(transaction_hash, internal_index) AS balance_diffs FROM default.canonical_execution_balance_diffs WHERE {w} GROUP BY block_number) AS bd USING block_number
LEFT JOIN (SELECT block_number, uniqExact(transaction_hash, internal_index) AS nonce_diffs FROM default.canonical_execution_nonce_diffs WHERE {w} GROUP BY block_number) AS nd USING block_number
LEFT JOIN (SELECT block_number, uniqExact(contract_address) AS contracts FROM default.canonical_execution_contracts WHERE {w} GROUP BY block_number) AS c USING block_number
ORDER BY block_number"""


def pull(window: str, a: int, b: int):
    path = OUT / window / f"{a}.parquet"
    if path.exists():
        return 0
    cols, rows = ch(chunk_sql(a, b), timeout=1200)
    data = {c: [int(r[i]) if r[i] not in (None, "") else None for r in rows] for i, c in enumerate(cols)}
    write_parquet(pa.table(data), path, {"source": "clickhouse-raw default.canonical_execution_* (mainnet)", "blocks": [a, b], "window": window})
    return len(rows)


def main():
    _, mx = ch("SELECT max(block_number) FROM default.canonical_execution_storage_diffs WHERE meta_network_name='mainnet' AND block_number > 26000000")
    recent_end = int(mx[0][0]) - 64
    windows = {"recent": (recent_end - 50_400 + 1, recent_end), "pre_fork": (24_300_001, 24_350_000)}
    work = [(w, a, min(a + STEP - 1, hi)) for w, (lo, hi) in windows.items() for a in range(lo, hi + 1, STEP)]
    with ThreadPoolExecutor(4) as ex:
        n = sum(ex.map(lambda j: pull(*j), work))
    log(f"mainnet: {len(work)} chunks, {n} new block rows; windows {windows}")


if __name__ == "__main__":
    sys.exit(main())
