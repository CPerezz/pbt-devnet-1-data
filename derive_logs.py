"""Derive structured tables from the EL logs (data/raw/logs/el).

Outputs (data/derived):
- markers.parquet: every migration / failure / lifecycle log line, named by rule,
  with the numbers each line carries (percent, counts, rates, block numbers);
- el_blocks.parquet: one row per block a client logged as processed
  (block, ms, Mgas, txs, and client extras such as SSTORE counts);
- lifecycle.parquet: process starts (with the build they report) and clean shutdowns.
Nethermind colours its output: ANSI escapes are stripped first.
"""
import sys

import duckdb

from common import CLIENT, DER, RAW, log

EL = f"read_parquet('{RAW}/logs/el/*/*.parquet', filename=true)"

# (client, marker, regex). Clients: geth, nethermind, besu, erigon, * (any).
RULES = [
    # geth: snapshot import and self-convert phases
    ("geth", "import_start", r"Starting binary tree import in the background"),
    ("geth", "anchor_wait", r"Binary tree import waiting for its anchor"),
    ("geth", "preimages_progress", r"Verifying preimage file"),
    ("geth", "preimages_done", r"Verified preimage file"),
    ("geth", "check1_progress", r"Verifying snapshot\s"),
    ("geth", "check1_done", r"Verified snapshot consistency"),
    ("geth", "convert_wait", r"conversion waiting for (a complete state|an access list)"),
    ("geth", "convert_start", r"Starting binary tree conversion in the background"),
    ("geth", "scan_progress", r"Converting state\s"),
    ("geth", "build_progress", r"Building the binary tree"),
    ("geth", "build_done", r"Built the binary tree"),
    ("geth", "write_progress", r"Writing tree nodes"),
    ("geth", "write_done", r"Wrote tree nodes"),
    ("geth", "code_progress", r"Verifying code\s"),
    ("geth", "code_done", r"Verified code against its hashes"),
    ("geth", "check2_progress", r"Re-deriving merkle state"),
    ("geth", "check2_done", r"Verified consensus anchoring"),
    ("geth", "readback_progress", r"Reading back .* records"),
    ("geth", "readback_done", r"Verified the written state"),
    ("geth", "import_complete", r"\bImport complete\b"),
    ("geth", "convert_complete", r"\bConversion complete\b"),
    ("geth", "import_reused", r"Binary tree already imported"),
    ("geth", "import_failed", r"Binary tree import failed"),
    ("geth", "replay_stalled", r"Migration direction stalled"),
    ("geth", "rewound", r"Rewound to block with available state"),
    ("geth", "sethead_finalized", r"SetHead invalidated finalized block"),
    ("geth", "journal_failed", r"Failed to load journal"),
    ("geth", "merkle_disposal", r"Disposing of the merkle state|State migration finished"),
    # Nethermind
    ("nethermind", "preimages_progress", r"PBT verify preimages\s+[\d.]+ %"),
    ("nethermind", "import_progress", r"PBT import( \([A-Za-z]+\))?\s+[\d.]+ %"),
    ("nethermind", "rebuild_done", r"PBT rebuild complete"),
    ("nethermind", "check2_progress", r"PBT verify MPT\s+[\d.]+ %"),
    ("nethermind", "check2_done", r"PBT verified .* against the MPT root"),
    ("nethermind", "import_complete", r"Imported the PBT migration anchor"),
    ("nethermind", "import_reused", r"was imported earlier; reusing it"),
    ("nethermind", "bootstrap_aligned", r"EIP-8347 migration: flat state at"),
    ("nethermind", "self_start", r"Importing PBT state at the flat state"),
    ("nethermind", "self_scan_progress", r"PBT self import scan\s+[\d.]+ %"),
    ("nethermind", "self_complete", r"Imported the PBT state at"),
    ("nethermind", "self_skipped", r"PBT state already populated; skipping the self import"),
    ("nethermind", "replay_retry", r"PBT BAL replay towards .* failed"),
    ("nethermind", "root_mismatch", r"BAL replay of .* produced PBT root"),
    ("nethermind", "anchor_failed", r"migration anchor (failed validation|import failed)"),
    ("nethermind", "disk_low", r"Running out of free disk space"),
    # Besu
    ("besu", "anchor_wait", r"PBT migrator waiting for snapshot anchor"),
    ("besu", "import_start", r"PBT migrator verifying and loading snapshot"),
    ("besu", "step_progress", r"EIP-8347 verification in progress, step \d/\d"),
    ("besu", "check1_done", r"EIP-8347 internal PBT consistency OK"),
    ("besu", "check2_done", r"EIP-8347 consensus anchoring OK"),
    ("besu", "column_loaded", r"PBT column loaded from"),
    ("besu", "import_complete", r"PBT migrator bootstrapped from snapshot"),
    ("besu", "self_wait", r"waiting to follow the head before its self-conversion"),
    ("besu", "self_start", r"PBT migrator converting its flat database at block"),
    ("besu", "self_stage", r"PBT self-conversion: (preimages from the BALs|reading the flat database|keys created after|codes of|building the PBT)"),
    ("besu", "self_progress", r"PBT self-conversion in progress, "),
    ("besu", "self_done", r"PBT self-conversion done in"),
    ("besu", "self_complete", r"PBT migrator bootstrapped from self-conversion"),
    ("besu", "roll", r"PBT migrator rolled to block"),
    ("besu", "caught_up", r"PBT migrator caught up with the head"),
    ("besu", "resumed", r"PBT migrator resuming from"),
    ("besu", "retired", r"PBT migrator retired at block"),
    ("besu", "backward_sync", r"Backward sync phase \d of \d completed|Starting a new backward sync session"),
    ("besu", "jvm_oom", r"OutOfMemoryError"),
    ("besu", "pbt_error", r"\| (ERROR|FATAL) +\| .*(Pbt|PBT|Eip8347|SnapshotColumnWriter|SelfConversion)"),
    # erigon
    ("erigon", "hexbin_datadir", r"datadir uses (hex\+bin commitment|the bin commitment trie)"),
    ("erigon", "trie_variant", r"erigondb settings .*trie_variant="),
    ("erigon", "shadow_stopped", r"binary commitment domain is stopped"),
    ("erigon", "bal_starved", r"peer did not serve BALs"),
    ("erigon", "commitment_failed", r"commitment compute failed|commitmentCalculator: reporting failure|invalid node record: missing child"),
    ("erigon", "build_failed", r"Failed to build PoS block"),
    # any client: failures and lifecycle
    ("*", "segv", r"SIG(SEGV|BUS|ABRT|ILL)\b|signal arrived during|unexpected fault address|^panic: |fatal error: "),
    ("*", "no_space", r"(?i)no space left on device"),
    ("*", "bad_block", r"BAD BLOCK|invalid merkle root|InvalidStateRoot|Invalid block"),
]

LIFECYCLE = [
    ("geth", "start", r"Maximum peer count\s+ETH="), ("geth", "stop", r"Got interrupt, shutting down"),
    ("nethermind", "start", r"Nethermind is starting up"), ("nethermind", "stop", r"Nethermind is shutting down"),
    ("besu", "start", r"\| Besu \| Starting Besu"), ("besu", "stop", r"BesuCommand-Shutdown-Hook .*Shutting down Auto transaction logs caching"),
    ("erigon", "start", r"Build info\s+git_branch="), ("erigon", "stop", r"Got interrupt, shutting down"),
]
BUILD = [  # version strings logged at start
    ("geth", r"Geth/v[\w.\-+]+"), ("nethermind", r"Version: [\w.\-+]+"),
    ("besu", r"Besu version [\w.\-+]+"), ("erigon", r"git_commit=[0-9a-f]+"),
]
COARSE = (r"PBT|EIP-8347|binary tree|Binary tree|Verif|Import complete|Conversion complete|Converting state|"
          r"Writing tree|Wrote tree|Re-deriving|Reading back|Migration direction|Rewound|SetHead|journal|migrat|"
          r"self-conversion|disk space|OutOfMemory|SIG[A-Z]{3}|panic|fatal error|no space|BAD BLOCK|invalid|Invalid|"
          r"commitment|datadir uses|erigondb settings|BALs|Backward sync|backward sync|Disposing|State migration")


def main():
    DER.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql("SET TimeZone='UTC'")
    con.sql(f"""CREATE TABLE el AS SELECT split_part(filename, '/', -2) AS host, ts::TIMESTAMPTZ AS ts,
                regexp_replace(body, '\\x1b\\[[0-9;]*m', '', 'g') AS b FROM {EL}""")
    con.sql("CREATE TABLE hostclient AS SELECT * FROM (VALUES " + ",".join(f"('{h}','{c}')" for h, c in CLIENT.items()) + ") t(host, client)")
    con.sql(f"CREATE TABLE cand AS SELECT el.*, client FROM el JOIN hostclient USING (host) WHERE regexp_matches(b, '{COARSE}')")
    log(f"candidate lines: {con.sql('SELECT count(*) FROM cand').fetchall()[0][0]:,}")

    parts = []
    for client, marker, rx in RULES:
        cond = "TRUE" if client == "*" else f"client = '{client}'"
        parts.append(f"SELECT host, client, ts, '{marker}' AS marker, left(b, 600) AS text FROM cand WHERE {cond} AND regexp_matches(b, '{rx.replace(chr(39), chr(39) * 2)}')")
    con.sql("CREATE TABLE markers AS " + " UNION ALL ".join(parts))
    # Numbers the lines carry: percent, a leading count with its unit, rates, block numbers.
    con.sql(r"""CREATE TABLE markers2 AS SELECT *,
        TRY_CAST(coalesce(nullif(regexp_extract(text, 'progress=([\d.]+)%', 1), ''),
                          nullif(regexp_extract(text, '([\d.]+) % \[', 1), ''),
                          nullif(regexp_extract(text, ': ([\d.]+)%', 1), '')) AS DOUBLE) AS pct,
        TRY_CAST(replace(nullif(regexp_extract(text, '\|\s+([\d,]+) (leaf|acc|slot)', 1), ''), ',', '') AS BIGINT) AS count,
        nullif(regexp_extract(text, '\|\s+[\d,]+ (leaf|acc|slot)', 1), '') AS unit,
        TRY_CAST(replace(nullif(regexp_extract(text, '\(\s*([\d,]+)/s\)', 1), ''), ',', '') AS DOUBLE) AS rate,
        TRY_CAST(replace(coalesce(nullif(regexp_extract(text, '(?:rolled to block|caught up with the head at block|resuming from previously-materialised base block|at block) (\d+)', 1), ''),
                                  nullif(regexp_extract(text, 'BlockNumber = (\d+)', 1), ''),
                                  nullif(regexp_extract(text, 'anchor=([\d,]+)', 1), '')), ',', '') AS BIGINT) AS block,
        nullif(regexp_extract(text, 'elapsed="?([\dhms.µ]+)', 1), '') AS elapsed,
        nullif(regexp_extract(text, '(?:step \d/\d |self-conversion in progress, )([^:]+):', 1), '') AS stage,
        nullif(regexp_extract(text, '(PBT [A-Za-z ()]+?)\s+[\d.]+ %', 1), '') AS bar
        FROM markers""")
    con.sql(f"COPY (SELECT * FROM markers2 ORDER BY host, ts) TO '{DER}/markers.parquet' (FORMAT parquet, COMPRESSION zstd)")
    log(f"markers: {con.sql('SELECT count(*) FROM markers2').fetchall()[0][0]:,} rows; "
        f"{con.sql('SELECT marker, count(*) FROM markers2 GROUP BY 1 ORDER BY 2 DESC LIMIT 8').fetchall()}")

    # Lifecycle: starts (with the build logged right after) and clean stops.
    lc = []
    for client, kind, rx in LIFECYCLE:
        lc.append(f"SELECT host, ts, '{kind}' AS kind FROM el JOIN hostclient USING (host) WHERE client='{client}' AND regexp_matches(b, '{rx}')")
    con.sql("CREATE TABLE lc AS " + " UNION ALL ".join(lc))
    bl = []
    for client, rx in BUILD:
        bl.append(f"SELECT host, ts, regexp_extract(b, '{rx}') AS build FROM el JOIN hostclient USING (host) WHERE client='{client}' AND regexp_matches(b, '{rx}')")
    con.sql("CREATE TABLE builds AS " + " UNION ALL ".join(bl))
    con.sql(f"""COPY (SELECT lc.host, lc.ts, lc.kind,
                       (SELECT build FROM builds b WHERE b.host = lc.host AND b.ts BETWEEN lc.ts - INTERVAL 5 SECOND AND lc.ts + INTERVAL 120 SECOND ORDER BY b.ts LIMIT 1) AS build
                FROM lc ORDER BY host, ts) TO '{DER}/lifecycle.parquet' (FORMAT parquet)""")
    log(f"lifecycle: {con.sql(f'SELECT kind, count(*) FROM read_parquet(\'{DER}/lifecycle.parquet\') GROUP BY 1').fetchall()}")

    # Per-block processing lines.
    con.sql(r"""CREATE TABLE blk_geth AS SELECT host, ts,
        TRY_CAST(replace(regexp_extract(b, 'number=([\d,]+)', 1), ',', '') AS BIGINT) AS block,
        TRY_CAST(replace(regexp_extract(b, 'blocks=([\d,]+)', 1), ',', '') AS INT) AS blocks,
        TRY_CAST(replace(regexp_extract(b, 'txs=([\d,]+)', 1), ',', '') AS INT) AS txs,
        TRY_CAST(regexp_extract(b, 'mgas=([\d.]+)', 1) AS DOUBLE) AS mgas,
        regexp_extract(b, 'elapsed="?([\d.]+[µm]?s)', 1) AS elapsed_txt,
        TRY_CAST(regexp_extract(b, 'triediffs=([\d.]+)MiB', 1) AS DOUBLE) AS triediffs_mib,
        TRY_CAST(regexp_extract(b, 'triedirty=([\d.]+)MiB', 1) AS DOUBLE) AS triedirty_mib
        FROM el JOIN hostclient USING (host) WHERE client='geth' AND regexp_matches(b, 'Imported new (potential )?chain segment')""")
    con.sql(r"""CREATE TABLE nm_proc AS SELECT host, ts,
        TRY_CAST(regexp_extract(b, 'Processed\s+(\d+)', 1) AS BIGINT) AS block,
        TRY_CAST(regexp_extract(b, '\|\s+([\d.]+) ms', 1) AS DOUBLE) AS ms
        FROM el JOIN hostclient USING (host) WHERE client='nethermind' AND regexp_matches(b, '\| Processed\s+\d+')""")
    con.sql(r"""CREATE TABLE nm_blk AS SELECT host, ts,
        TRY_CAST(regexp_extract(b, '([\d.]+) MGas', 1) AS DOUBLE) AS mgas,
        TRY_CAST(replace(regexp_extract(b, '([\d,]+)\s+txs', 1), ',', '') AS INT) AS txs,
        TRY_CAST(replace(regexp_extract(b, 'sload\s+([\d,]+)', 1), ',', '') AS INT) AS sload,
        TRY_CAST(replace(regexp_extract(b, 'sstore\s+([\d,]+)', 1), ',', '') AS INT) AS sstore,
        TRY_CAST(replace(regexp_extract(b, 'create\s+([\d,]+)', 1), ',', '') AS INT) AS creates
        FROM el JOIN hostclient USING (host) WHERE client='nethermind' AND regexp_matches(b, '\|\s+Block\s+[\d.]+ ETH')""")
    con.sql(r"""CREATE TABLE blk_besu AS SELECT host, ts,
        TRY_CAST(replace(regexp_extract(b, 'Imported #([\d,]+)', 1), ',', '') AS BIGINT) AS block,
        TRY_CAST(regexp_extract(b, '\|\s*(\d+) tx', 1) AS INT) AS txs,
        TRY_CAST(replace(regexp_extract(b, '\|\s*([\d,]+) \(\s*[\d.]+%\) gas used', 1), ',', '') AS DOUBLE) / 1e6 AS mgas,
        TRY_CAST(regexp_extract(b, '([\d.]+)ms exec', 1) AS DOUBLE) AS ms
        FROM el JOIN hostclient USING (host) WHERE client='besu' AND regexp_matches(b, 'Imported #[\d,]+')""")
    con.sql(r"""CREATE TABLE blk_erigon AS SELECT host, ts,
        TRY_CAST(regexp_extract(b, 'number=(\d+)', 1) AS BIGINT) AS block,
        regexp_extract(b, 'execution=([\d.]+[µm]?s)', 1) AS elapsed_txt,
        TRY_CAST(regexp_extract(b, ' mgas/s=([\d.]+)', 1) AS DOUBLE) AS mgas_per_s
        FROM el JOIN hostclient USING (host) WHERE client='erigon' AND regexp_matches(b, 'head validated')""")

    def ms(col):
        return (f"CASE WHEN {col} LIKE '%µs' THEN TRY_CAST(replace({col}, 'µs', '') AS DOUBLE) / 1000 "
                f"WHEN {col} LIKE '%ms' THEN TRY_CAST(replace({col}, 'ms', '') AS DOUBLE) "
                f"WHEN {col} LIKE '%s' THEN TRY_CAST(replace({col}, 's', '') AS DOUBLE) * 1000 END")
    con.sql(f"""COPY (
        SELECT host, 'geth' AS client, ts, block, txs, mgas, {ms('elapsed_txt')} AS ms, blocks, triediffs_mib, triedirty_mib,
               NULL::INT AS sload, NULL::INT AS sstore, NULL::INT AS creates FROM blk_geth
        UNION ALL
        SELECT p.host, 'nethermind', p.ts, p.block, k.txs, k.mgas, p.ms, 1, NULL, NULL, k.sload, k.sstore, k.creates
        FROM nm_proc p ASOF LEFT JOIN nm_blk k ON p.host = k.host AND k.ts >= p.ts
        UNION ALL
        SELECT host, 'besu', ts, block, txs, mgas, ms, 1, NULL, NULL, NULL, NULL, NULL FROM blk_besu
        UNION ALL
        SELECT host, 'erigon', ts, block, NULL, NULL, {ms('elapsed_txt')}, 1, NULL, NULL, NULL, NULL, NULL FROM blk_erigon
        ORDER BY host, ts) TO '{DER}/el_blocks.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    log(f"el_blocks: {con.sql(f'SELECT client, count(*), count(ms) FROM read_parquet(\'{DER}/el_blocks.parquet\') GROUP BY 1').fetchall()}")


if __name__ == "__main__":
    sys.exit(main())
