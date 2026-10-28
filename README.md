# pbt-devnet-1: data from an online MPT → binary-tree migration on a 9-node Ethereum devnet

This repository holds the data collected from **pbt-devnet-1** (the ethPandaOps devnet `glamsterdam-pbt-1`). On it,
nine execution clients (geth, Nethermind, Besu and erigon) moved their state from the Merkle-Patricia trie (MPT) to the
binary tree (PBT) of [EIP-8347](https://eips.ethereum.org/EIPS/eip-8347) / EIP-8297 while the chain kept running. It
also holds the derived tables and SQL views built from that data, and the scripts that pulled it.

**Data window (this release): 2026-10-22 08:00 → 2026-10-27 21:00 UTC.** The PBT fork happens at
2026-10-29 09:00 UTC. A later release extends the same files through 2026-10-31 09:00 (fork + 48 h).

Everything is [Parquet](https://parquet.apache.org/) (zstd) or JSON, about 0.6 GB in total.

## Quick start

```bash
pip install -r requirements.txt          # duckdb, pyarrow, pandas
python catalog.py                        # rebuilds data/pbt-devnet-1.duckdb (views only), from the repo root
duckdb data/pbt-devnet-1.duckdb          # or: python -c "import duckdb; ..."
```

```sql
SELECT run_id, cap_gib, t_core_s / 3600 AS core_h, t_total_s / 3600 AS total_h FROM a1_runs;   -- every counted migration
SELECT * FROM 'data/derived/resources.parquet' WHERE host = 'geth-1' LIMIT 10;                 -- any file, directly
```

The catalog's paths are relative to the repository root, so open it from there. Set `SET TimeZone='UTC'` first: every
timestamp in this repository is UTC.

## The devnet in one table

| Node | EL client | Container memory cap | Migration method (counted runs) |
|---|---|---|---|
| geth-1 | geth | 16 GiB | snapshot import from N |
| geth-2 | geth | 32 GiB | snapshot import from N |
| geth-3 | geth | 16 GiB | self-conversion at its own block P = 24,503,830 |
| nethermind-1 | Nethermind | 16 GiB | snapshot import (10-23), then self-import at P = 24,534,182 (10-27) |
| nethermind-2 | Nethermind | 32 GiB | snapshot import |
| besu-1 | Besu (`-Xmx8g`) | 16 GiB | snapshot import (run 3, 10-23), then self-conversion at P = 24,535,469 (10-27) |
| besu-2 | Besu (`-Xmx24g`) | 32 GiB | snapshot import (run 1 on an old build, then a rerun) |
| erigon-1 | erigon | 32 GiB | online dual commitment (`hex+bin`) |
| erigon-2 | erigon | 16 GiB | online dual commitment (`hex+bin`), after two stopped attempts |

- Every node runs Lighthouse as its consensus client. All nine hosts are identical DigitalOcean droplets: 16 vCPU
  (Xeon Gold 6548N), 125.8 GiB RAM and a 2.31 TiB ext4 disk. The only hardware difference is the EL container's memory cap.
- **Chain composition.** Mainnet history runs up to block 24,350,000. After that come the devnet's own blocks:
  - a synthetic state bloat up to ~24,452,000;
  - the snapshot anchor **N = 24,453,558**: 355.19 M accounts, 1.82 B storage slots, 3.55 B PBT leaves;
  - a synthetic window N+1 → 24,503,958: 50.4 k blocks of ~100 Mgas and ~5.5 k storage writes each;
  - the live chain from 24,503,959 (CL genesis 2026-10-22 09:00 UTC, 12 s slots);
  - an XEN-style transaction load from block 24,530,322 (2026-10-26 11:09 UTC).

  The block numbers are in `data/raw/rpc/boundaries.json`.
- **PBT fork:** `binaryTrieTime` 1793264400 = 2026-10-29 09:00:00 UTC.

## Layout

```
.
├── README.md                 this guide
├── requirements.txt
├── catalog.py                builds data/pbt-devnet-1.duckdb from the files below and queries/*.sql
├── queries/*.sql             24 analysis views, one SELECT per file, first line = what it answers
├── common.py, pull_*.py      the pulls that produced data/raw (need ethPandaOps `panda` access)
├── derive_*.py, manifest.py  raw → data/derived, and the manifest/validation of data/raw
└── data/
    ├── MANIFEST.json         one entry per raw file: source, exact query, window, rows, bytes, write time
    ├── validation.json       window, totals, log-chunk count, Prometheus coverage per series, gaps > 5 min
    ├── pbt-devnet-1.duckdb   views only (no data inside): every dataset + every queries/*.sql
    ├── raw/                  as pulled, 966 Parquet files + a .meta.json next to each
    │   ├── logs/             container logs (otel → ClickHouse)
    │   ├── xatu/             beacon/engine events (ethPandaOps xatu)
    │   ├── prom/             Prometheus series, 30 s
    │   ├── rpc/              block headers, block access lists (BALs), chain boundaries
    │   ├── mainnet/          mainnet per-block workload, for comparison
    │   ├── watch/            our own 1-minute monitor of the migration
    │   ├── bench/            mainnet migration benchmark results (separate machine)
    │   └── hosts/            per-host container configuration snapshot
    └── derived/              15 tables built from raw by derive_*.py
```

### Conventions

- **Time:** all UTC. Columns are `TIMESTAMPTZ` (logs, xatu, derived) or epoch seconds (`ts` in Prometheus, `t` in the
  watch, `timestamp` in headers).
- **Hosts:** always `geth-1 … erigon-2`. In xatu, `meta_client_name` is
  `ethpandaops/devnets/glamsterdam-pbt-1-lighthouse-<host>`. In ClickHouse the host is `lighthouse-<host>`.
- **Sidecars:** every raw Parquet file `X.parquet` has `X.meta.json`. Its fields:
  - `source`;
  - the exact `sql` (ClickHouse) or `query` (PromQL);
  - the window `t0`/`t1`;
  - `host` and `step` where relevant;
  - `rows`, `bytes`, `written`.

  `MANIFEST.json` merges all sidecars.
- **Chunking:** files are cut by time.
  - EL logs: 6 h per file, named `<YYYYmmddHH>` = chunk start.
  - CL and ad-hoc logs: 24 h per file.
  - Prometheus: 12 h per file.
  - xatu: one file per day (08:00 → 08:00 UTC, named by the start date).
- **Redaction:** credentials found in client flags and log lines were replaced before publication: a Nethermind
  push-gateway URL with its token, a Seq API key, and any `user:password@` in URLs. They now read `<redacted>` or
  `<redacted-pushgateway-url>`. Host IP addresses (public devnet nodes) and file paths of keys and JWT secrets are left
  as they were.

## data/raw: what each file holds

### `logs/`: container logs (ClickHouse `external.otel_logs`)

| Path | Grain | Columns | Contents |
|---|---|---|---|
| `logs/el/<host>/<YYYYmmddHH>.parquet` | one row per log line, 6 h files | `ts`, `body` | Every line of the `execution` container except transaction-submission spam and peer chatter (the `EL_NOISE` regex in `common.py`). 10.8 M lines. Migration progress, per-block lines, errors, restarts and build versions are all here. |
| `logs/cl/<host>/<YYYYmmdd08>.parquet` | log line, 24 h | `ts`, `container` (`beacon`/`validator`), `body` | Lighthouse lines at CRIT/ERRO/WARN, plus any line about the execution engine (offline, syncing…). 573 k lines. |
| `logs/adhoc/<host>/<YYYYmmdd08>.parquet` | log line, 24 h | `ts`, `container`, `image`, `body` | Every line from operator containers that are not part of the node: datadir restores, image builds, erigon's offline PBT tooling, `execution-init`. 41 k lines. |
| `logs/el_noise_hourly.parquet` | host × hour | `host`, `hour`, `lines` | How many EL lines the spam filter dropped: a proxy for the transaction load. |

### `xatu/`: ethPandaOps xatu, database `glamsterdam-pbt-1`

One directory per table, one file per day. Values are kept as xatu delivers them, numbers included (VARCHAR); cast
them when querying. Every table has `meta_client_name` (the sentry next to each node) and an event time.

| Table | Rows | What it is |
|---|---|---|
| `execution_engine_new_payload` | 329 k | Every `engine_newPayload` call each node's EL answered: `duration_ms`, `status` (VALID / SYNCING / ACCEPTED / INVALID), `block_number`, `gas_used`, `tx_count`, `validation_error`, client version. The per-block processing record of every node. |
| `beacon_api_eth_v1_events_block` | 321 k | Block gossip events seen by each sentry: `slot`, `block`, `propagation_slot_start_diff` (ms after slot start). |
| `beacon_api_eth_v1_events_head` | 320 k | Head events per sentry. |
| `beacon_api_eth_v1_events_execution_payload` | 320 k | ePBS execution-payload events (`block_hash`, `builder_index`). |
| `beacon_api_eth_v1_events_execution_payload_available` | 320 k | Payload-availability events. |
| `beacon_api_eth_v1_events_finalized_checkpoint` | 10.7 k | Finalized checkpoints (`epoch`, `block`, `state`). |
| `beacon_api_eth_v1_events_chain_reorg` | 387 | Reorgs (`depth`, `old_head_block`, `new_head_block`). |
| `beacon_api_eth_v2_beacon_block` | 36.5 k | Beacon blocks with proposer, builder and payload summary (gas, txs, bytes). |
| `beacon_api_slot` | 347 k | Per-slot aggregates per sentry (`blocks`, `attestations`), merged with `sumMerge`. |

### `prom/`: Prometheus (ethPandaOps `devnets`), 30 s step

Path `prom/<job>/<host>/<YYYYmmddHH>.parquet`, long format: `ts` (epoch s), `metric`, `labels` (JSON of the
remaining labels), `value`. 105.8 M points. Per job, only the metric families in `pull_prom.py:KEEP` were pulled:

| Job | Metrics | Contents |
|---|---|---|
| `node` | 61 | node-exporter: CPU (stored as `node_cpu_seconds_total_by_mode`, summed over CPUs per mode), pressure (PSI), disk IO per device (`vda` is the data disk), filesystem free/size, memory, vmstat (`node_vmstat_oom_kill`), load, network. |
| `cadvisor` | 15 | Containers `execution` and `beacon`: memory rss/working set/cache/usage/limit, CPU, filesystem IO. Starts at 08:52 or 11:30 on 10-22 depending on the host. |
| `exporter` | 69 | ethereum-metrics-exporter / eth-docker: volume sizes per mount (`eth_docker_volume_usage_bytes`; the EL datadir is the `execution` container's `/data` mount, the migration inputs `/pbt-migration-input`), container memory and major faults, EL/CL sync state. |
| `execution` | 639 | Each EL's own metrics. geth: `system_memory_*`, `chain_*`, `pathdb_*`, `pbt_*`. Nethermind: `nethermind_pbt_*`, `nethermind_db_*` (sizes, index/filter, memtables per DB), block processing, `dotnet_*`. Besu: `jvm_*`, `besu_bal_*`, `besu_bonsai_*`. erigon: commitment/domain/exec, `go_memstats_*`. |
| `consensus_node` | 31 | Lighthouse: head/finalized/justified epochs, block delays, execution-layer state, peers. |

### `rpc/`: from the devnet's own RPC (`panda ethnode`)

| File | Contents |
|---|---|
| `headers.parquet` | 1,420 sampled block headers (`number`, `hash`, `parent_hash`, `timestamp`, `gas_used`, `gas_limit`, `tx_count`, `size`, `miner`, `state_root`, `base_fee`, `extra_data`, `bal_hash`) over the bloat region, the synthetic window and the live chain. |
| `bal_summary.parquet` | 1,540 block access lists (EIP-7928 BALs, from Nethermind's `eth_getBlockAccessList`), one row per block. Columns: `accounts`, `accounts_written`, `slot_writes` (distinct slots written), `slot_write_entries`, `slot_reads`, `balance_changes`, `nonce_changes`, `code_changes`, `json_bytes`. **Stratified sample, not every block:** 1 in 100 blocks of the synthetic window, 1 in 50 live, 1 in 20 under the load. Treat per-era figures as sample means. |
| `bal_full/<block>.json.gz` | 154 full BALs (1 in 10 of the sample): a list of accounts with `address`, `storageChanges`, `storageReads`, `balanceChanges`, `nonceChanges`, `codeChanges`. |
| `boundaries.json` | `fork_point` (first devnet block), `anchor` (N), `first_live_block`, `load_start_block`, `head_at_pull`. |
| `fork_point.json` | Where the devnet's chain leaves mainnet (found by bisection against xatu's mainnet hashes), with the first devnet header. |

### `mainnet/`: mainnet workload for comparison (xatu canonical tables)

`mainnet/recent/*.parquet` holds the last ~50,400 mainnet blocks (26.24–26.29 M, October 2026).
`mainnet/pre_fork/*.parquet` holds the 50,000 blocks before the devnet left mainnet (24.30–24.35 M).

One row per block:
- `block_number`, `ts`, `gas_used`, `gas_limit`, `txs`;
- `slot_events`, `slots_written` (distinct address+slot changed: the BAL-equivalent);
- `slots_created`, `slots_cleared`;
- `accounts_written`, `balance_diffs`, `nonce_diffs`, `contracts` (created).

### `watch/`: our monitor

The watch was a read-only monitor that polled every node over SSH and RPC about once a minute. It is the **only source
of geth's and Nethermind's BAL-replay cursors**. It slept about 29 h in 8 gaps (a laptop).

`watch_samples.parquet` has one row per poll × host:

| Column | Meaning |
|---|---|
| `t` | Time (epoch seconds). |
| `phase_txt` | Migration phase as the node reported it. |
| `cursor`, `head`, `lag`, `rate`, `eta` | `debug_migrationProgress`: the replay cursor and the head. |
| `mem`, `limit_g`, `anon_g` | Cgroup memory, the cap, and anonymous memory. |
| `cpu`, `io` (PSI) | Cgroup CPU and IO pressure. |
| `lat`, `base` | Block processing p95, and its baseline. |
| `peers`, `fin_epoch`, `part` (participation), `behind`, `head_gas` | Node and chain state. |
| `shadow` | The node's PBT root at the shared sampled height. |
| `sample_height`, `sample_hash` | That sampled height and the canonical hash there. |
| `erigon_exec`, `shadow_folds` | erigon only. |
| `df_free_bytes`, `df_free_frac` | Free disk. |
| `link`, `cl` | SSH and CL status text. |

`watch_events.parquet` has every alert the watch raised: `t`, `sev`, `host`, `key`, `msg`.

### `bench/`: mainnet migration benchmark (separate 8-core / 32 GiB box)

`phases.parquet` (`round`, `client`, `phase`, `edge` start/end, `unix`, `note`) records phase boundaries of our
mainnet-state migration benchmark rounds. `pair-meta.json` describes the benchmark's mainnet anchor at block 26,142,444:
roots, digests, counts.

### `hosts/`: configuration snapshot

`hosts/<host>.json` was taken on 2026-10-27. Keys:
- `inspect`: `docker inspect` of `execution`, `beacon` and `validator`: image, args, mounts, memory limit, env with
  secrets removed;
- `ps`: all containers;
- `vmstat`, `uptime`, `cpu`, `mem`, `df`, `lsblk`, `kernel`.

## data/derived: tables built by `derive_*.py`

| Table | Grain | What it holds |
|---|---|---|
| `markers.parquet` | log line | Every migration-relevant EL log line, classified: `marker` (e.g. `import_start`, `preimages_done`, `check1_done`, `write_progress`, `check2_done`, `import_complete`, `convert_complete`, `self_complete`, `rebuild_done`, `roll`, `caught_up`, `jvm_oom`, `segv`, `shadow_stopped`, `hexbin_datadir`, `bal_starved`, `no_space`), with parsed `pct`, `count`, `unit`, `rate`, `block`, `elapsed`, `stage`, progress `bar`, and the `text`. |
| `lifecycle.parquet` | process start/stop | `kind` (`start` / clean `stop`) and the `build` the process logged. |
| `el_blocks.parquet` | node × block | The client's own per-block log line: `ms`, `mgas`, `txs`, Nethermind's `sload`/`sstore`/`creates`, geth's trie-dirty sizes. |
| `runs.parquet` | migration attempt | Every attempt per node and method. `status`: `valid`, `failed`, `superseded` or `reference`. Times `deploy`, `start`, `complete`, `synced` (with `synced_source`). Durations `t_wait_s`, `t_core_s`, `t_replay_s`, `t_total_s`. `replay_blocks` and the rate. |
| `phases.parquet` | run × phase | Universal phases U0 wait, U1 keys/preimages, U2 leaves/scan, U3 build+write, U4 code, U5 check 2, U6 read-back/commit, U7 replay, with `start`, `end`, `dur_s`. |
| `failures.parquet` | event | cgroup OOM kills (host `oom_kill` counter), JVM heap OOMs, SIGSEGVs, unclean restarts, replay stalls, erigon shadow stops, with `detail`. |
| `resources.parquet` | host × 30 s | One wide row per sample. Memory: `mem_rss`, `mem_ws`, `mem_cache`, `mem_usage`, `mem_limit`, the CL's `cl_mem_rss`, `host_mem_avail`. CPU: `host_cpu_busy` (busy cores of 16), `cpu_cores` (EL container). Disk IO: bytes/s and IOPS on `vda`, `disk_util`, `disk_qtime`. PSI: cpu, io and memory, wait and stall. Space: `fs_free`, `fs_size`, `datadir`, `mig_input`. Also `majfault_ps` and `oom_kills`. |
| `internals.parquet` | host × 30 s × component | What each client reports its memory is made of. Go runtime (geth, erigon); .NET managed memory, RocksDB index/filter, memtables, block cache and snapshots per DB (Nethermind); JVM heap, non-heap and direct buffers (Besu). |
| `blocks_node.parquet` | node × block | xatu `engine_newPayload` (first call: `np_ms`, `status`, `gas_used`, `txs`, `calls`) joined with the client's own log line (`client_ms`, …). |
| `network.parquet` | slot | Block and payload seen by how many sentries, first-seen delay, and `missed`. |
| `network_events.parquet` | event | Finalized checkpoints and reorgs (`depth`, `sentries`). |
| `roots.parquet` | sampled height | The PBT root of every node at the same height (watch): `nodes_with_root`, `distinct_roots` (>1 = divergence; there are none), `hosts`. |
| `replay.parquet` | run × sample | Replay progress from N or P to the head: `cursor` (and `head`) from the watch, or from Besu's 128-block roll lines. |
| `workload_devnet.parquet` | block | Gas, txs and timestamp (live blocks from xatu, older blocks from sampled headers), BAL counts where sampled, and `era` (bloat / synthetic N+1→genesis / live before load / XEN load). |
| `workload_mainnet.parquet` | block | The `mainnet/` files with a `window` column. |

## The views (`queries/*.sql`, in the catalog)

Each file is one view; its first comment line says what it answers.

| Group | Views |
|---|---|
| Runs and time to migrate | `a1_runs`, `a2_phases`, `a3_effective` |
| Per node and state | `c0_states`, then `d1_latency`, `e1_memory`, `f1_disk`, `i1_state_compare`, `i2_internals_state` |
| Throughput and replay | `c1_run_resources`, `d2_replay`, `d3_replay_by_era`, `g1_phase_resources` |
| Network and correctness | `h1_network_hourly`, `h2_node_hourly`, `h3_missed_attribution`, `l1_roots` |
| Load, mainnet and the projection | `j1_workload`, `j2_projection` |
| Failures and builds | `k1_failures`, `k2_recovery`, `p1_builds` |
| Comparison rows | `r1_compare` |
| Fork activation (empty until the fork is in the data) | `n1_fork` |

`c0_states` labels what each node was doing over time: `MPT only`, `migrating`, `failed attempt`, `replay`, `synced`;
for erigon, `dual attempt`, `shadow stopped`, `dual (hex+bin)`. Each state after the XEN load start carries a
` (load)` suffix. Most per-node views group by it.

## How it was produced

- `pull_*.py` read ethPandaOps' ClickHouse (logs, xatu), Prometheus and devnet RPC through the
  `panda` CLI, plus our own watch and benchmark records. The exact query of every raw
  file is in its `.meta.json`.
- Pulls are idempotent and chunked. A chunk is refetched only if it was cut short by an earlier window end
  (`common.covers`).
- `WIN_END=<iso time>` pins the window end.
- Order: `pull_*` → `manifest.py` → `derive_logs.py` → `derive_ledger.py` → `derive_metrics.py` → `catalog.py`.

## Caveats

- **BALs are sampled.** A synthetic-window BAL is ~1.9 MB, so fetching every block would have meant ~130 GB of transfer.
- **Container metrics start late.** cadvisor starts at 08:52 or 11:30 on 10-22; host metrics (`node`) cover the whole
  window.
- **Prometheus coverage** is below 95 % for `execution/besu-1` (0.94) and `execution/erigon-2` (0.84): those
  containers were down. See `validation.json` for every gap over 5 minutes.
- **Replay cursors have gaps.** Replay cursors for geth and Nethermind exist only in the watch samples.
  nethermind-1's 10-27 self-import replay end (05:31) is inferred from CPU and disk activity, because the watch was
  asleep.
- **Undocumented erigon build.** No container logged how erigon-1's `hex+bin` datadir was produced (10-25 06:06–06:26).
