"""P0: data only the live devnet can give (panda ethnode RPC).

- where the devnet's history leaves mainnet (bisection against xatu mainnet hashes);
- block headers sampled over the bloat region and the synthetic window N+1 → CL genesis;
- block access list summaries (Nethermind eth_getBlockAccessList on nethermind-2,
  which keeps every BAL) over the synthetic window, the live blocks and the load,
  with the full JSON kept for every 10th sampled block.
Re-running only fetches what is missing.
"""
import gzip
import json
import sys
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

from common import ANCHOR_N, RAW, ch, log, rpc, write_parquet

OUT = RAW / "rpc"
BAL_INSTANCE = "lighthouse-nethermind-2"
HDR_INSTANCE = "lighthouse-geth-2"
CL_GENESIS_BLOCK_APPROX = 24_504_000   # refined below from the first live block
LOAD_START_BLOCK = 24_530_322          # first ~190 Mgas block (project timeline)


def header(n: int) -> dict:
    b = rpc("eth_getBlockByNumber", [hex(n), False], HDR_INSTANCE)
    return {"number": n, "hash": b["hash"], "parent_hash": b["parentHash"], "timestamp": int(b["timestamp"], 16),
            "gas_used": int(b["gasUsed"], 16), "gas_limit": int(b["gasLimit"], 16), "tx_count": len(b["transactions"]),
            "size": int(b.get("size", "0x0"), 16), "miner": b["miner"], "state_root": b["stateRoot"],
            "base_fee": int(b.get("baseFeePerGas", "0x0"), 16), "extra_data": b.get("extraData", ""),
            "bal_hash": b.get("blockAccessListHash") or b.get("balHash") or ""}


def bal_summary(n: int, keep_full: bool) -> dict:
    bal = rpc("eth_getBlockAccessList", [hex(n)], BAL_INSTANCE)
    raw = json.dumps(bal)
    if keep_full:
        (OUT / "bal_full").mkdir(parents=True, exist_ok=True)
        with gzip.open(OUT / "bal_full" / f"{n}.json.gz", "wt") as f:
            f.write(raw)
    acc = bal if isinstance(bal, list) else []
    return {"number": n, "accounts": len(acc),
            "slot_writes": sum(len(a.get("storageChanges") or []) for a in acc),
            "slot_write_entries": sum(len(s.get("slotChanges") or s.get("changes") or [])
                                      for a in acc for s in (a.get("storageChanges") or []) if isinstance(s, dict)),
            "slot_reads": sum(len(a.get("storageReads") or []) for a in acc),
            "balance_changes": sum(len(a.get("balanceChanges") or []) for a in acc),
            "nonce_changes": sum(len(a.get("nonceChanges") or []) for a in acc),
            "code_changes": sum(len(a.get("codeChanges") or []) for a in acc),
            "accounts_written": sum(1 for a in acc if a.get("storageChanges") or a.get("balanceChanges")
                                    or a.get("nonceChanges") or a.get("codeChanges")),
            "json_bytes": len(raw)}


def existing(path, key="number") -> set:
    return set(pq.read_table(path, columns=[key]).column(key).to_pylist()) if path.exists() else set()


def fetch(path, numbers, fn, meta, workers=8):
    have = existing(path)
    todo = sorted(set(numbers) - have)
    log(f"{path.name}: {len(have)} cached, {len(todo)} to fetch")
    rows = []
    with ThreadPoolExecutor(workers) as ex:
        for i, r in enumerate(ex.map(fn, todo)):
            rows.append(r)
            if (i + 1) % 100 == 0:
                log(f"  {path.name}: {i + 1}/{len(todo)}")
    if not rows:
        return
    new = pa.Table.from_pylist(rows)
    if path.exists():
        new = pa.concat_tables([pq.read_table(path), new], promote_options="default")
    write_parquet(new.sort_by("number"), path, meta)


def fork_point() -> dict:
    """First block where the devnet's hash differs from mainnet's (bisection)."""
    def same(n):
        _, rows = ch(f"SELECT block_hash FROM default.canonical_execution_block WHERE meta_network_name='mainnet' AND block_number={n}")
        return bool(rows) and rows[0][0].lower() == header(n)["hash"].lower()
    lo, hi = 24_200_000, ANCHOR_N            # invariant: same(lo), not same(hi)
    assert same(lo) and not same(hi), "fork-point bracket is wrong"
    while hi - lo > 1:
        mid = (lo + hi) // 2
        lo, hi = (mid, hi) if same(mid) else (lo, mid)
        log(f"  fork bisection [{lo}, {hi}]")
    return {"last_shared_block": lo, "first_devnet_block": hi, "first_devnet_header": header(hi), "last_shared_header": header(lo)}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    fp = OUT / "fork_point.json"
    if not fp.exists():
        log("bisecting the mainnet fork point")
        fp.write_text(json.dumps(fork_point(), indent=1))
    fork = json.loads(fp.read_text())["first_devnet_block"]
    head = int(rpc("eth_blockNumber", [], HDR_INSTANCE), 16)
    log(f"fork point {fork}, head {head}")

    # The first live block: the first one built after the CL genesis (10-22 09:00:00 UTC).
    genesis_ts = 1_792_659_600
    lo, hi = ANCHOR_N, CL_GENESIS_BLOCK_APPROX + 500
    while hi - lo > 1:
        mid = (lo + hi) // 2
        lo, hi = (mid, hi) if header(mid)["timestamp"] < genesis_ts else (lo, mid)
    first_live = hi
    (OUT / "boundaries.json").write_text(json.dumps({"fork_point": fork, "anchor": ANCHOR_N, "first_live_block": first_live,
                                                     "load_start_block": LOAD_START_BLOCK, "head_at_pull": head}, indent=1))
    log(f"first live block {first_live}")

    hdr_numbers = (list(range(fork - 50, fork + 50)) + list(range(fork, ANCHOR_N, 500))
                   + list(range(ANCHOR_N - 600, ANCHOR_N + 2)) + list(range(ANCHOR_N, first_live + 1, 100))
                   + list(range(first_live - 5, first_live + 5)))
    fetch(OUT / "headers.parquet", hdr_numbers, header,
          {"source": f"eth_getBlockByNumber via panda ethnode {HDR_INSTANCE}", "what": "sampled headers fork→first live"})

    bal_numbers = (list(range(ANCHOR_N + 1, first_live, 100))
                   + list(range(first_live, LOAD_START_BLOCK, 50))
                   + list(range(LOAD_START_BLOCK, head - 64, 20)))
    full = set(bal_numbers[::10])
    fetch(OUT / "bal_summary.parquet", bal_numbers, lambda n: bal_summary(n, n in full),
          {"source": f"eth_getBlockAccessList via panda ethnode {BAL_INSTANCE}",
           "what": "BAL size per sampled block (stride 100 synthetic, 50 live, 20 load)"})
    log("done")


if __name__ == "__main__":
    sys.exit(main())
