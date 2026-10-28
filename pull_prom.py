"""S3: Prometheus series from the `devnets` datasource (via panda), 30 s steps.

The keep-lists below apply discard list X1/X6. One file per job × host × 12 h, in
long format (ts, metric, labels, value). Host-level CPU is stored as a per-mode sum.
"""
import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

from common import CLIENT, HOSTS, NET, RAW, WIN_START, chunks, covers, log, prom_range, ts, win_end, write_parquet

OUT = RAW / "prom"
KEEP = {
    ("execution", "geth"): r"system_(memory|cpu|disk)_.*|chain_.*|pathdb_.*|eth_db_(chaindata|state).*|state_(accounts|storages|contracts|trienodes|height).*|pbt_.*|txpool_(pending|queued|slots)|p2p_peers|rpc_duration_all(_count)?|engine_.*",
    ("execution", "nethermind"): r"nethermind_(pbt_.*|db_.*|bal_.*|block_access_list_.*|last_.*|blocks|mgas.*|gas_used|gas_limit|new_payload_execution_time|forkchoice_updated_execution_time|node_record_buffer_.*|.*snapshot.*|flat_.*|allocator_.*|arena_.*|blob_allocated_bytes|page_tracker_.*|carry_forward_.*|cached_nodes_count|loaded_from_.*|dirty_.*|memory_used_by_cache|persisted_.*|pruning_.*|processing_queue_size|account_writes|code_writes|storage_writes|main_thread_sstore_opcode|main_thread_sload_opcode|op_codes|creates|calls|trie_node_log_.*|importer_.*|commit_time|committed_nodes_count|reorganizations|bad_blocks|oldest_stored_.*|in_memory_slot_filter_memory)|dotnet_.*|jemalloc_bytes_.*|process_.*|ethereum_(blockchain_height|best_known_block_number|peer_count)",
    ("execution", "besu"): r"jvm_.*|process_.*|besu_(bal|block|blockchain|bonsai|synchronizer)_.*|ethereum_.*|besu_peers_peer_count",
    ("execution", "erigon"): r"commitment_.*|domain_.*|exec_.*|mem_.*|db_.*|sync|chain_.*|prune_.*|engine_.*|execution_.*|insert_.*|run_.*|validate_.*|update_.*|go_memstats_.*|go_gc_.*|process_.*|branch_.*|trie_.*",
    ("cadvisor", "*"): r"container_.*",
    ("exporter", "*"): r"eth_docker_.*|eth_exe_.*|eth_con_(sync_.*|beacon_finality_checkpoint_epochs|beacon_slot|peers|health_up)",
    ("node", "*"): r"node_pressure_.*|node_disk_.*|node_filesystem_(avail|size|free)_bytes|node_memory_(MemTotal|MemAvailable|MemFree|Cached|Buffers|Dirty|Writeback|AnonPages|Active_anon|Inactive_anon|Active_file|Inactive_file|Shmem|Slab|SReclaimable|SUnreclaim|PageTables|Committed_AS|Mapped)_bytes|node_vmstat_.*|node_load(1|5|15)|node_network_(receive|transmit)_bytes_total|node_procs_(running|blocked)|node_context_switches_total",
    ("consensus_node", "*"): r"beacon_head_state_(slot|finalized_epoch|current_justified_epoch)|beacon_block_delay_.*|execution_layer_.*|sync_eth2_synced|libp2p_peers",
}
EXTRA = {"cadvisor": ',name=~"execution|beacon"'}
DROP_LABELS = {"__name__", "network", "instance", "job", "ingress_user", "ip_address", "testnet", "tenant_tier",
               "vm_account_id", "vm_project_id", "consensus_client", "execution_client", "supernode", "Commit",
               "Instance", "Network", "PruningMode", "Runtime", "SourceDate", "SyncType", "Version", "NodeName",
               "service", "cluster"}
JOBS = ["execution", "cadvisor", "exporter", "node", "consensus_node"]


def selector(job: str, host: str) -> str:
    rx = KEEP.get((job, CLIENT[host])) or KEEP[(job, "*")]
    return f'{{__name__=~"{rx}",network="{NET}",job="{job}",instance="{NET}-lighthouse-{host}"{EXTRA.get(job, "")}}}'


def to_long(result, metric_override=None):
    t, m, l, v = [], [], [], []
    for s in result:
        name = metric_override or s["metric"].get("__name__", "")
        labels = json.dumps({k: val for k, val in sorted(s["metric"].items()) if k not in DROP_LABELS}, separators=(",", ":"))
        for ti, vi in s["values"]:
            fv = float(vi)
            if math.isnan(fv):
                continue
            t.append(int(float(ti))); m.append(name); l.append(labels); v.append(fv)
    return pa.table({"ts": pa.array(t, pa.int64()), "metric": pa.array(m).dictionary_encode(),
                     "labels": pa.array(l).dictionary_encode(), "value": pa.array(v, pa.float64())})


def pull(job, host, t0, t1):
    path = OUT / job / host / f"{t0:%Y%m%d%H}.parquet"
    if covers(path, t1):
        return pq.read_metadata(path).num_rows
    q = selector(job, host)
    tables = [to_long(prom_range(q, t0, t1))]
    if job == "node":
        qc = f'sum by (mode) (node_cpu_seconds_total{{network="{NET}",instance="{NET}-lighthouse-{host}"}})'
        tables.append(to_long(prom_range(qc, t0, t1), "node_cpu_seconds_total_by_mode"))
    table = pa.concat_tables([tb.cast(tables[0].schema) for tb in tables])
    write_parquet(table, path, {"source": "prometheus devnets", "query": q, "t0": ts(t0), "t1": ts(t1), "step": "30s"})
    return table.num_rows


def main():
    end = win_end()
    jobs = sys.argv[1:] or JOBS
    # The last 12 h chunk may be partial: never cache a chunk that ends within 2 h of the window end.
    work = [(j, h, a, b) for j in jobs for h in HOSTS for a, b in chunks(WIN_START, end, 12)]
    for j, h, a, b in work:
        p = OUT / j / h / f"{a:%Y%m%d%H}.parquet"
        if p.exists() and (end - b).total_seconds() < 7200:
            p.unlink()
    with ThreadPoolExecutor(4) as ex:
        rows = list(ex.map(lambda w: pull(*w), work))
    log(f"prom: {len(work)} chunks, {sum(rows):,} points")


if __name__ == "__main__":
    sys.exit(main())
