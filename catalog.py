"""Build data/pbt-devnet-1.duckdb: one view per raw/derived dataset, then one view per
queries/*.sql (name = file stem; created in dependency order, retried until every view binds).

Paths are relative: open the catalog from the report directory.
    duckdb data/pbt-devnet-1.duckdb "SELECT * FROM a1_runs"
"""
import sys
from pathlib import Path

import duckdb

from common import ROOT

DB = ROOT / "data" / "pbt-devnet-1.duckdb"
Q = ROOT / "queries"

BASE = {
    # derived (data/derived)
    **{n: f"read_parquet('data/derived/{n}.parquet')" for n in (
        "runs", "phases", "failures", "lifecycle", "markers", "el_blocks", "resources", "internals", "blocks_node",
        "network", "network_events", "roots", "replay", "workload_devnet", "workload_mainnet")},
    # raw
    "watch_samples": "read_parquet('data/raw/watch/watch_samples.parquet')",
    "watch_events": "read_parquet('data/raw/watch/watch_events.parquet')",
    "bench_phases": "read_parquet('data/raw/bench/phases.parquet')",
    "rpc_headers": "read_parquet('data/raw/rpc/headers.parquet')",
    "bal_summary": "read_parquet('data/raw/rpc/bal_summary.parquet')",
    "boundaries": "read_json('data/raw/rpc/boundaries.json')",
    "logs_el": "(SELECT split_part(filename, '/', -2) AS host, * EXCLUDE (filename) FROM read_parquet('data/raw/logs/el/*/*.parquet', filename=true))",
    "logs_cl": "(SELECT split_part(filename, '/', -2) AS host, * EXCLUDE (filename) FROM read_parquet('data/raw/logs/cl/*/*.parquet', filename=true))",
    "logs_adhoc": "(SELECT split_part(filename, '/', -2) AS host, * EXCLUDE (filename) FROM read_parquet('data/raw/logs/adhoc/*/*.parquet', filename=true))",
    "el_noise_hourly": "read_parquet('data/raw/logs/el_noise_hourly.parquet')",
    "mainnet_blocks": "(SELECT split_part(filename, '/', -2) AS window, * EXCLUDE (filename) FROM read_parquet('data/raw/mainnet/*/*.parquet', filename=true))",
    **{f"prom_{j}": f"(SELECT split_part(filename, '/', -2) AS host, * EXCLUDE (filename) FROM read_parquet('data/raw/prom/{j}/*/*.parquet', filename=true))"
       for j in ("cadvisor", "node", "execution", "exporter", "consensus_node")},
    **{f"xatu_{t}": f"read_parquet('data/raw/xatu/{t}/*.parquet')" for t in (
        "execution_engine_new_payload", "beacon_api_eth_v1_events_block", "beacon_api_eth_v1_events_head",
        "beacon_api_eth_v1_events_execution_payload", "beacon_api_eth_v1_events_execution_payload_available",
        "beacon_api_eth_v1_events_finalized_checkpoint", "beacon_api_eth_v1_events_chain_reorg",
        "beacon_api_eth_v2_beacon_block", "beacon_api_slot")},
}


def connect(read_only=True):
    con = duckdb.connect(str(DB), read_only=read_only)
    con.sql("SET TimeZone='UTC'")
    return con


def main():
    import os
    os.chdir(ROOT)
    DB.unlink(missing_ok=True)
    con = connect(read_only=False)
    for name, src in BASE.items():
        con.sql(f"CREATE VIEW {name} AS SELECT * FROM {src}")
    pending, errors = sorted(Q.glob("*.sql")), {}
    while pending:
        left = []
        for f in pending:
            try:
                con.sql(f"CREATE VIEW {f.stem} AS {f.read_text().strip().rstrip(';')}")
            except duckdb.Error as e:
                left.append(f)
                errors[f.name] = e
        if len(left) == len(pending):
            sys.exit("\n".join(f"{f.name}: {errors[f.name]}" for f in left))
        pending = left
    names = [r[0] for r in con.sql("SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1").fetchall()]
    print(f"{DB.name}: {len(names)} views")
    if "--check" in sys.argv:
        for f in sorted(Q.glob("*.sql")):
            n = con.sql(f"SELECT count(*) FROM {f.stem}").fetchall()[0][0]
            print(f"  {f.stem}: {n} rows")


if __name__ == "__main__":
    main()
