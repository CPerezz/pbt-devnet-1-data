"""Derived metric tables (data/derived).

resources.parquet     per host × 30 s: container memory (rss, working set, cache, limit,
                      failcnt), CPU cores, container IO, host disk IO/util, PSI, free
                      space, datadir size, major faults, host oom kills.
internals.parquet     per host × 30 s: what the memory is made of, per client.
blocks_node.parquet   per node × block: newPayload time and status (xatu) joined with the
                      client's own per-block log line.
network.parquet       per slot: block seen, payload seen, by how many sentries; plus
                      finality and reorgs in network_events.parquet.
roots.parquet         per sampled height: PBT roots per node, agreement.
workload_devnet.parquet / workload_mainnet.parquet  per block workload.
replay.parquet        per replay run: cursor over time.
"""
import sys

import duckdb

from common import ANCHOR_N, DER, RAW, log

C = duckdb.connect()
C.sql("SET TimeZone='UTC'")


def prom(job, metric_like, label_like=None):
    lab = f" AND labels LIKE '{label_like}'" if label_like else ""
    return (f"SELECT split_part(filename, '/', -2) AS host, ts, value FROM read_parquet('{RAW}/prom/{job}/*/*.parquet', filename=true) "
            f"WHERE metric = '{metric_like}'{lab}")


def rate(sql):
    """Per-second rate of a counter; resets, repeated timestamps and NaN samples give NULL."""
    return (f"SELECT host, ts, CASE WHEN d >= 0 AND dt > 0 AND isfinite(d) THEN d / dt END AS v FROM ("
            f"SELECT host, ts, value - lag(value) OVER w AS d, ts - lag(ts) OVER w AS dt FROM ({sql}) WINDOW w AS (PARTITION BY host ORDER BY ts))")


def resources():
    series = {
        "mem_rss": prom("cadvisor", "container_memory_rss", '%"name":"execution"%'),
        "mem_ws": prom("cadvisor", "container_memory_working_set_bytes", '%"name":"execution"%'),
        "mem_cache": prom("cadvisor", "container_memory_cache", '%"name":"execution"%'),
        "mem_usage": prom("cadvisor", "container_memory_usage_bytes", '%"name":"execution"%'),
        "mem_limit": prom("cadvisor", "container_spec_memory_limit_bytes", '%"name":"execution"%'),
        "cl_mem_rss": prom("cadvisor", "container_memory_rss", '%"name":"beacon"%'),
        "fs_free": prom("node", "node_filesystem_avail_bytes", '%"mountpoint":"/"%'),
        "fs_size": prom("node", "node_filesystem_size_bytes", '%"mountpoint":"/"%'),
        "host_mem_avail": prom("node", "node_memory_MemAvailable_bytes"),
        "datadir": prom("exporter", "eth_docker_volume_usage_bytes", '%"container_name":"execution","mount_path":"/data",%'),
        "mig_input": prom("exporter", "eth_docker_volume_usage_bytes", '%"mount_path":"/pbt-migration-input"%'),
        # busy cores of 16: rate per mode series first, then summed. Around node-exporter restarts the per-mode sums over
        # CPUs go partial for a few samples; a rate above the droplet's 16 vCPU is such an artefact and is dropped.
        "host_cpu_busy": (f"SELECT host, ts, sum(CASE WHEN d >= 0 AND dt > 0 AND d / dt <= 16.5 THEN d / dt END) AS value FROM ("
                          f"SELECT host, ts, value - lag(value) OVER w AS d, ts - lag(ts) OVER w AS dt FROM ("
                          f"SELECT split_part(filename, '/', -2) AS host, ts, labels, max(value) AS value "
                          f"FROM read_parquet('{RAW}/prom/node/*/*.parquet', filename=true) "
                          f"WHERE metric = 'node_cpu_seconds_total_by_mode' AND labels NOT LIKE '%idle%' GROUP BY 1, 2, 3) "
                          f"WINDOW w AS (PARTITION BY host, labels ORDER BY ts)) GROUP BY 1, 2"),
    }
    rates = {
        "cpu_cores": prom("cadvisor", "container_cpu_usage_seconds_total", '%"name":"execution"%'),
        "ctr_read_bps": prom("cadvisor", "container_fs_reads_bytes_total", '%"name":"execution"%'),
        "ctr_write_bps": prom("cadvisor", "container_fs_writes_bytes_total", '%"name":"execution"%'),
        "disk_read_bps": prom("node", "node_disk_read_bytes_total", '%"device":"vda"%'),
        "disk_write_bps": prom("node", "node_disk_written_bytes_total", '%"device":"vda"%'),
        "disk_read_iops": prom("node", "node_disk_reads_completed_total", '%"device":"vda"%'),
        "disk_write_iops": prom("node", "node_disk_writes_completed_total", '%"device":"vda"%'),
        "disk_util": prom("node", "node_disk_io_time_seconds_total", '%"device":"vda"%'),
        "disk_qtime": prom("node", "node_disk_io_time_weighted_seconds_total", '%"device":"vda"%'),
        "psi_cpu": prom("node", "node_pressure_cpu_waiting_seconds_total"),
        "psi_io_wait": prom("node", "node_pressure_io_waiting_seconds_total"),
        "psi_io_stall": prom("node", "node_pressure_io_stalled_seconds_total"),
        "psi_mem_wait": prom("node", "node_pressure_memory_waiting_seconds_total"),
        "psi_mem_stall": prom("node", "node_pressure_memory_stalled_seconds_total"),
        "majfault_ps": prom("exporter", "eth_docker_memory_major_page_faults_total", '%"container_name":"execution"%'),
        "oom_kills": prom("node", "node_vmstat_oom_kill"),
    }
    # node-exporter covers the whole window; cadvisor only arrived at 08:52 or 11:30 on 10-22 depending on the host.
    C.sql(f"CREATE TABLE grid AS SELECT DISTINCT split_part(filename, '/', -2) AS host, ts FROM read_parquet('{RAW}/prom/node/*/*.parquet', filename=true)")
    cols = ["host", "ts"]
    for name, sql in series.items():
        C.sql(f"CREATE TABLE s_{name} AS SELECT host, ts, max(value) AS v FROM ({sql}) GROUP BY 1, 2")
        cols.append(name)
    for name, sql in rates.items():
        C.sql(f"CREATE TABLE s_{name} AS SELECT host, ts, max(v) AS v FROM ({rate(sql)}) GROUP BY 1, 2")
        cols.append(name)
    joins = " ".join(f"LEFT JOIN s_{n} USING (host, ts)" for n in list(series) + list(rates))
    sel = ", ".join(f"s_{n}.v AS {n}" for n in list(series) + list(rates))
    C.sql(f"""COPY (SELECT host, to_timestamp(ts) AS t, ts, {sel} FROM grid {joins} ORDER BY host, ts)
              TO '{DER}/resources.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    log(f"resources: {C.sql(f'SELECT count(*) FROM read_parquet(\'{DER}/resources.parquet\')').fetchall()[0][0]:,} rows")


def internals():
    # (host pattern, name, metric, label filter) — what each client says its memory is made of.
    spec = [
        ("geth", "go_heap_objects", "system_memory_objects", None), ("geth", "go_used", "system_memory_used", None),
        ("geth", "go_held", "system_memory_held", None),
        ("nethermind", "dotnet_managed", "dotnet_total_memory_bytes", None),
        ("nethermind", "rocksdb_index_filter_pbt", "nethermind_db_index_filter_size", '%"db":"Pbt"%'),
        ("nethermind", "rocksdb_index_filter_flat", "nethermind_db_index_filter_size", '%"db":"Flat"%'),
        ("nethermind", "rocksdb_memtable_pbt", "nethermind_db_memtable_size", '%"db":"Pbt"%'),
        ("nethermind", "rocksdb_memtable_flat", "nethermind_db_memtable_size", '%"db":"Flat"%'),
        ("nethermind", "rocksdb_block_cache", "nethermind_db_block_cache_size", '%"db":"Shared"%'),
        ("nethermind", "node_record_buffer", "nethermind_node_record_buffer_memory", None),
        ("nethermind", "node_record_buffer_compressed", "nethermind_node_record_buffer_compressed_memory", None),
        ("nethermind", "snapshots", "nethermind_total_snapshot_memory", None),
        ("besu", "jvm_heap_used", "jvm_memory_used_bytes", '%"area":"heap"%'),
        ("besu", "jvm_heap_committed", "jvm_memory_committed_bytes", '%"area":"heap"%'),
        ("besu", "jvm_nonheap_used", "jvm_memory_used_bytes", '%"area":"nonheap"%'),
        ("besu", "jvm_direct_buffers", "jvm_buffer_pool_used_bytes", '%"pool":"direct"%'),
        ("erigon", "go_heap_inuse", "go_memstats_heap_inuse_bytes", None), ("erigon", "go_sys", "go_memstats_sys_bytes", None),
    ]
    parts = []
    for client, name, metric, lab in spec:
        l = f" AND labels LIKE '{lab}'" if lab else ""
        # 12 h pull chunks share their boundary timestamp: one value per (series, ts) first, then sum the series.
        parts.append(f"SELECT host, ts, '{name}' AS component, sum(value) AS value FROM ("
                     f"SELECT split_part(filename, '/', -2) AS host, ts, labels, max(value) AS value "
                     f"FROM read_parquet('{RAW}/prom/execution/{client}-*/*.parquet', filename=true) WHERE metric = '{metric}'{l} GROUP BY 1, 2, 3) GROUP BY 1, 2")
    C.sql(f"COPY ({' UNION ALL '.join(parts)} ORDER BY 1, 2) TO '{DER}/internals.parquet' (FORMAT parquet, COMPRESSION zstd)")
    log(f"internals: {C.sql(f'SELECT component, count(*) FROM read_parquet(\'{DER}/internals.parquet\') GROUP BY 1 ORDER BY 1').fetchall()}")


def blocks_node():
    C.sql(f"""COPY (
        WITH np AS (SELECT replace(split_part(meta_client_name, 'lighthouse-', 2), '', '') AS host,
                           CAST(block_number AS BIGINT) AS block, min(requested_date_time) AS t,
                           arg_min(CAST(duration_ms AS DOUBLE), requested_date_time) AS np_ms, arg_min(status, requested_date_time) AS status,
                           any_value(CAST(gas_used AS BIGINT)) AS gas_used, any_value(CAST(tx_count AS INT)) AS txs,
                           count(*) AS calls
                    FROM read_parquet('{RAW}/xatu/execution_engine_new_payload/*.parquet') GROUP BY 1, 2),
             cl AS (SELECT host, block, min(ms) AS client_ms, any_value(mgas) AS client_mgas, any_value(sstore) AS sstore,
                           any_value(sload) AS sload, any_value(creates) AS creates, any_value(triedirty_mib) AS triedirty_mib
                    FROM read_parquet('{DER}/el_blocks.parquet') WHERE block IS NOT NULL GROUP BY 1, 2)
        SELECT np.*, cl.client_ms, cl.client_mgas, cl.sstore, cl.sload, cl.creates, cl.triedirty_mib
        FROM np LEFT JOIN cl USING (host, block) ORDER BY host, block) TO '{DER}/blocks_node.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    log(f"blocks_node: {C.sql(f'SELECT host, count(*), count(client_ms), round(median(np_ms)) FROM read_parquet(\'{DER}/blocks_node.parquet\') GROUP BY 1 ORDER BY 1').fetchall()}")


def network():
    C.sql(f"""COPY (
        WITH b AS (SELECT CAST(slot AS BIGINT) AS slot, min(slot_start_date_time) AS slot_start, count(DISTINCT meta_client_name) AS sentries_block,
                          min(CAST(propagation_slot_start_diff AS BIGINT)) AS first_seen_ms
                   FROM read_parquet('{RAW}/xatu/beacon_api_eth_v1_events_block/*.parquet') GROUP BY 1),
             p AS (SELECT CAST(slot AS BIGINT) AS slot, count(DISTINCT meta_client_name) AS sentries_payload
                   FROM read_parquet('{RAW}/xatu/beacon_api_eth_v1_events_execution_payload/*.parquet') GROUP BY 1),
             s AS (SELECT CAST(slot AS BIGINT) AS slot, min(slot_start_date_time) AS slot_start, max(CAST(blocks AS INT)) AS max_blocks
                   FROM read_parquet('{RAW}/xatu/beacon_api_slot/*.parquet') GROUP BY 1)
        SELECT coalesce(s.slot, b.slot) AS slot, coalesce(s.slot_start, b.slot_start) AS slot_start,
               coalesce(b.sentries_block, 0) AS sentries_block, coalesce(p.sentries_payload, 0) AS sentries_payload,
               b.first_seen_ms, s.max_blocks,
               (coalesce(b.sentries_block, 0) = 0 AND coalesce(s.max_blocks, 0) = 0) AS missed
        FROM s FULL OUTER JOIN b USING (slot) LEFT JOIN p USING (slot) ORDER BY slot) TO '{DER}/network.parquet' (FORMAT parquet)""")
    C.sql(f"""COPY (
        SELECT 'finalized' AS kind, min(event_date_time) AS t, CAST(epoch AS BIGINT) AS epoch, NULL::BIGINT AS depth, count(DISTINCT meta_client_name) AS sentries
        FROM read_parquet('{RAW}/xatu/beacon_api_eth_v1_events_finalized_checkpoint/*.parquet') GROUP BY epoch
        UNION ALL
        SELECT 'reorg', min(event_date_time), CAST(epoch AS BIGINT), max(CAST(depth AS BIGINT)), count(DISTINCT meta_client_name)
        FROM read_parquet('{RAW}/xatu/beacon_api_eth_v1_events_chain_reorg/*.parquet') GROUP BY slot, epoch
        ORDER BY 2) TO '{DER}/network_events.parquet' (FORMAT parquet)""")
    log(f"network: {C.sql(f'SELECT count(*), sum(missed::INT), sum((sentries_payload = 0 AND NOT missed)::INT) FROM read_parquet(\'{DER}/network.parquet\')').fetchall()}")


def roots():
    C.sql(f"""COPY (
        SELECT t, sample_height AS height, count(*) FILTER (WHERE shadow IS NOT NULL) AS nodes_with_root,
               count(DISTINCT shadow) AS distinct_roots, list(host ORDER BY host) FILTER (WHERE shadow IS NOT NULL) AS hosts,
               count(DISTINCT sample_hash) AS distinct_hashes
        FROM (SELECT round(t / 60) * 60 AS t, host, sample_height, sample_hash, shadow
              FROM read_parquet('{RAW}/watch/watch_samples.parquet') WHERE sample_height IS NOT NULL)
        GROUP BY 1, 2 ORDER BY 1) TO '{DER}/roots.parquet' (FORMAT parquet)""")
    log(f"roots: {C.sql(f'SELECT count(*), sum((distinct_roots > 1)::INT), max(nodes_with_root) FROM read_parquet(\'{DER}/roots.parquet\')').fetchall()}")


def workload():
    b = f"{RAW}/rpc/boundaries.json"
    C.sql(f"""COPY (
        WITH bd AS (SELECT * FROM read_json('{b}')),
             live AS (SELECT CAST(block_number AS BIGINT) AS number, any_value(CAST(gas_used AS BIGINT)) AS gas_used,
                             any_value(CAST(gas_limit AS BIGINT)) AS gas_limit, any_value(CAST(tx_count AS INT)) AS txs,
                             min(requested_date_time) AS t
                      FROM read_parquet('{RAW}/xatu/execution_engine_new_payload/*.parquet') GROUP BY 1),
             hdr AS (SELECT number, gas_used, gas_limit, tx_count AS txs, to_timestamp(timestamp) AS t FROM read_parquet('{RAW}/rpc/headers.parquet')),
             allb AS (SELECT * FROM live UNION ALL SELECT * FROM hdr WHERE number NOT IN (SELECT number FROM live)),
             j AS (SELECT coalesce(allb.number, bal.number) AS number, allb.* EXCLUDE (number), bal.* EXCLUDE (number)
                   FROM allb FULL OUTER JOIN read_parquet('{RAW}/rpc/bal_summary.parquet') bal ON bal.number = allb.number)
        SELECT j.*,
               CASE WHEN number <= {ANCHOR_N} THEN 'bloat (pre-anchor)'
                    WHEN number < (SELECT first_live_block FROM bd) THEN 'synthetic N+1→genesis'
                    WHEN number < (SELECT load_start_block FROM bd) THEN 'live, before load'
                    ELSE 'live, XEN load' END AS era
        FROM j ORDER BY number)
        TO '{DER}/workload_devnet.parquet' (FORMAT parquet)""")
    C.sql(f"""COPY (SELECT replace(split_part(filename, '/', -2), '_', '-') AS window, * EXCLUDE (filename)
                    FROM read_parquet('{RAW}/mainnet/*/*.parquet', filename=true) ORDER BY block_number)
              TO '{DER}/workload_mainnet.parquet' (FORMAT parquet)""")
    log(f"workload: {C.sql(f'SELECT era, count(*), count(slot_writes), round(avg(gas_used)/1e6,1), round(avg(slot_writes)) FROM read_parquet(\'{DER}/workload_devnet.parquet\') GROUP BY 1').fetchall()}")


def replay():
    C.sql(f"""COPY (
        WITH r AS (SELECT * FROM read_parquet('{DER}/runs.parquet') WHERE complete IS NOT NULL AND synced IS NOT NULL),
             w AS (SELECT r.run_id, to_timestamp(s.t) AS t, s.cursor, s.head, 'watch' AS src
                   FROM r JOIN read_parquet('{RAW}/watch/watch_samples.parquet') s ON s.host = r.host
                   WHERE to_timestamp(s.t) BETWEEN r.complete AND r.synced + INTERVAL 2 MINUTE AND s.cursor IS NOT NULL),
             b AS (SELECT r.run_id, m.ts AS t, m.block AS cursor, NULL::DOUBLE AS head, 'besu roll' AS src
                   FROM r JOIN read_parquet('{DER}/markers.parquet') m ON m.host = r.host
                   WHERE m.marker IN ('roll', 'caught_up') AND m.ts BETWEEN r.complete AND r.synced)
        SELECT * FROM w UNION ALL SELECT * FROM b ORDER BY run_id, t) TO '{DER}/replay.parquet' (FORMAT parquet)""")
    log(f"replay: {C.sql(f'SELECT run_id, src, count(*) FROM read_parquet(\'{DER}/replay.parquet\') GROUP BY 1, 2 ORDER BY 1').fetchall()}")


def main():
    for step in (sys.argv[1:] or ["resources", "internals", "blocks_node", "network", "roots", "workload", "replay"]):
        globals()[step]()


if __name__ == "__main__":
    sys.exit(main())
