"""S2: xatu tables of the glamsterdam-pbt-1 ClickHouse database (via panda).

Kept (discard list X3 drops the gossip tables):
- execution_engine_new_payload: every block on every node (rpc-snooper timing);
- beacon events: block, head, execution payload, payload available, finalized
  checkpoint, chain reorg; beacon_api_slot; beacon_api_eth_v2_beacon_block.
Pulled per day; each day's row count is checked against count().
"""
import sys
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa

from common import NET, RAW, WIN_START, ch, chunks, log, ts, win_end, write_parquet

OUT = RAW / "xatu"
DB = f"`{NET}`"
TIME_COL = {"execution_engine_new_payload": "requested_date_time", "beacon_api_slot": "slot_start_date_time"}
NO_FINAL = {"beacon_api_slot"}   # plain ReplicatedMergeTree: FINAL is illegal there
COLUMNS = {
    "execution_engine_new_payload": "meta_client_name, requested_date_time, event_date_time, duration_ms, block_number, "
                                    "block_hash, parent_hash, gas_used, gas_limit, tx_count, blob_count, status, "
                                    "validation_error, method_version, meta_execution_implementation, meta_execution_version",
    "beacon_api_eth_v1_events_block": "meta_client_name, event_date_time, slot, slot_start_date_time, propagation_slot_start_diff, block, block_hash, builder_index, execution_optimistic",
    "beacon_api_eth_v1_events_head": "meta_client_name, event_date_time, slot, slot_start_date_time, propagation_slot_start_diff, block, epoch_transition, execution_optimistic",
    "beacon_api_eth_v1_events_execution_payload": "meta_client_name, event_date_time, slot, slot_start_date_time, propagation_slot_start_diff, block_root, builder_index, block_hash, execution_optimistic",
    "beacon_api_eth_v1_events_execution_payload_available": "meta_client_name, event_date_time, slot, slot_start_date_time, propagation_slot_start_diff, block_root",
    "beacon_api_eth_v1_events_finalized_checkpoint": "meta_client_name, event_date_time, block, state, epoch, execution_optimistic",
    "beacon_api_eth_v1_events_chain_reorg": "meta_client_name, event_date_time, slot, slot_start_date_time, depth, old_head_block, new_head_block, epoch, execution_optimistic",
    "beacon_api_slot": "meta_client_name, slot, slot_start_date_time, epoch, sumMerge(blocks) AS blocks, sumMerge(attestations) AS attestations",
    "beacon_api_eth_v2_beacon_block": "meta_client_name, event_date_time, slot, slot_start_date_time, block_root, block_version, block_total_bytes, "
                                      "proposer_index, builder_index, payload_present, execution_payload_block_hash, execution_payload_block_number, "
                                      "execution_payload_gas_limit, execution_payload_gas_used, execution_payload_transactions_count, "
                                      "execution_payload_transactions_total_bytes, execution_payload_blob_gas_used, bid_value, execution_payment",
}
# Tables whose rows are partial aggregate states: merge them per key.
GROUP_BY = {"beacon_api_slot": "meta_client_name, slot, slot_start_date_time, epoch"}

def to_table(cols, rows) -> pa.Table:
    out = {}
    for i, c in enumerate(cols):
        vals = [r[i] for r in rows]
        if c.endswith("date_time"):
            out[c] = pa.array(np.array([v.replace(" ", "T") if v else "NaT" for v in vals], dtype="datetime64[ms]"),
                              pa.timestamp("ms", tz="UTC"))
        else:
            out[c] = pa.array(vals)
    return pa.table(out)


def pull_day(table: str, t0, t1):
    path = OUT / table / f"{t0:%Y%m%d}.parquet"
    tc = TIME_COL.get(table, "event_date_time")
    final = "" if table in NO_FINAL else " FINAL"
    where = f"meta_network_name='{NET}' AND {tc} >= '{ts(t0)}' AND {tc} < '{ts(t1)}'"
    group = f" GROUP BY {GROUP_BY[table]}" if table in GROUP_BY else ""
    inner = f"SELECT 1 FROM {DB}.{table}{final} WHERE {where}{group}"
    _, n = ch(f"SELECT count() FROM ({inner})")
    want = int(n[0][0])
    if path.exists():
        import pyarrow.parquet as pq
        if pq.read_metadata(path).num_rows == want:
            return table, t0, want, want
    sql = f"SELECT {COLUMNS[table]} FROM {DB}.{table}{final} WHERE {where}{group}"
    cols, rows = ch(sql)
    write_parquet(to_table(cols, rows), path, {"source": f"clickhouse-raw {NET}.{table}", "sql": sql, "t0": ts(t0), "t1": ts(t1)})
    return table, t0, len(rows), want


def main():
    end = win_end()
    tables = sys.argv[1:] or list(COLUMNS)
    jobs = [(t, a, b) for t in tables for a, b in chunks(WIN_START, end, 24)]
    with ThreadPoolExecutor(6) as ex:
        res = list(ex.map(lambda j: pull_day(*j), jobs))
    bad = [(t, f"{a:%m-%d}", n, w) for t, a, n, w in res if n != w]
    log(f"xatu: {len(res)} table-days, {sum(n for *_, n, _ in res)} rows, mismatches {bad}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
