"""Export the pbt-devnet-1 post-mortem catalog to the devnets-page JSON contract
(~/dev/pbt-website/goals/devnets-page/data-contract.md). SQL mirrors
~/dev/articles/pbt-devnet-1-migration/make_figures.py (the reference for every number);
cross-check against that article's figures/numbers.json with --check.

    .venv/bin/python export_site.py [--out DIR] [--check]
"""
import argparse
import json
import os
import re
from datetime import datetime, timezone

from catalog import ROOT, connect

OUT_DEFAULT = os.path.expanduser("~/dev/pbt-website/uploads/devnets/pbt-devnet-1")
NUMBERS_JSON = os.path.expanduser("~/dev/articles/pbt-devnet-1-migration/figures/numbers.json")

HOSTS = ["geth-1", "geth-2", "geth-3", "nethermind-1", "nethermind-2", "besu-1", "besu-2", "erigon-1", "erigon-2"]
CAP = {"geth-1": 16, "geth-2": 32, "geth-3": 16, "nethermind-1": 16, "nethermind-2": 32,
       "besu-1": 16, "besu-2": 32, "erigon-1": 32, "erigon-2": 16}
METHODS = {"geth-1": ["import"], "geth-2": ["import"], "geth-3": ["self"], "nethermind-1": ["import", "self"],
           "nethermind-2": ["import"], "besu-1": ["import", "self"], "besu-2": ["import"],
           "erigon-1": ["dual"], "erigon-2": ["dual"]}
RUNS16 = ["geth-1/import", "geth-3/self", "nethermind-1/import", "nethermind-1/self", "besu-1/import", "besu-1/self"]
RUNS_ALL = ["geth-1/import", "geth-2/import", "geth-3/self", "nethermind-1/import", "nethermind-2/import",
            "nethermind-1/self", "besu-1/import", "besu-2/import", "besu-1/self"]
# Periods, named as the devnets page shows them ("replaying", not a1_phases'/c0_states' "replay").
STATES = ["one tree", "migrating", "replaying", "two trees", "two trees + load", "binary only"]
PERIOD_SQL = """CASE WHEN state LIKE 'migrating%' THEN 'migrating' WHEN state LIKE 'replay%' THEN 'replaying'
                     WHEN state IN ('MPT only (hex)', 'MPT only') THEN 'one tree'
                     WHEN state IN ('synced', 'dual (hex+bin)') THEN 'two trees'
                     WHEN state IN ('synced (load)', 'dual (hex+bin) (load)') THEN 'two trees + load' END"""
FORK = 1793264400  # 2026-10-29 09:00:00 UTC, binaryTrieTime
FORK_BLOCK = 24551102
ANCHOR = 24453558
VERIFY = {"geth-1/import": "preimages \u00b7 check 1 \u00b7 check 2", "geth-2/import": "preimages \u00b7 check 1 \u00b7 check 2",
          "geth-3/self": "check 2 at P", "nethermind-1/import": "check 1",
          "nethermind-2/import": "check 1 \u00b7 check 2", "nethermind-1/self": "none until the fork's header",
          "besu-1/import": "check 1 \u00b7 check 2", "besu-2/import": "check 1 \u00b7 check 2",
          "besu-1/self": "none until the fork's header"}
# When each geth ran a fixed build on the binary tree alone (the others: one hour after the fork).
BIN_START = {"geth-1": "10-30 09:55", "geth-3": "10-30 09:57", "geth-2": "10-30 11:48"}
# (host, time, label, PR url|None) -- the client fix or build that answered a failure.
FIXES = [
    ("besu-1", "10-22 22:34", "Besu 76a88a11: batched BAL catch-up", None),
    ("erigon-2", "10-22 16:52", "erigon build", None), ("erigon-1", "10-22 16:56", "erigon build", None),
    ("nethermind-1", "10-27 10:17", "Nethermind f609f2fd: memory budget", None),
    ("geth-1", "10-27 12:53", "#78 profiler leak", "https://github.com/CPerezz/go-ethereum/pull/78"),
    ("geth-1", "10-29 12:21", "#79 recover", "https://github.com/CPerezz/go-ethereum/pull/79"),
    ("geth-3", "10-29 12:22", "#79 recover", "https://github.com/CPerezz/go-ethereum/pull/79"),
    ("erigon-1", "10-30 04:10", "erigon build", None), ("erigon-2", "10-30 04:10", "erigon build", None),
    ("geth-1", "10-30 09:53", "#80\u2013#82", "https://github.com/CPerezz/go-ethereum/pull/80"),
    ("geth-3", "10-30 09:55", "#80\u2013#82", "https://github.com/CPerezz/go-ethereum/pull/80"),
    ("geth-2", "10-30 10:31", "#83 replay route", "https://github.com/CPerezz/go-ethereum/pull/83"),
]
# The fork-day finality pause (f20): 12 epochs stuck between finalized epoch 1601 and 1610.
PAUSE = {"from": "10-29 12:10", "to": "10-29 13:08", "epochs": 12, "participation_min": 0.22, "validating": 4,
         "down": ["geth-1", "geth-2", "geth-3", "erigon-1", "erigon-2"]}


def utc(s):
    """'10-22 09:38' or ISO -> epoch seconds (2026, UTC)."""
    if len(s) <= 14:
        s = "2026-" + s
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp())


T0 = utc("10-22 08:00")
XEN_T = utc("10-26 11:09")

DB = None


def q(sql):
    return DB.sql(sql).fetchall()


def client(host):
    return host.rsplit("-", 1)[0]


def rnd(x, nd):
    return None if x is None else (round(x, nd) if nd else int(round(x)))


def ms(x):
    return rnd(x, 0)


def gib(x):
    return rnd(x, 1)


def frac(x):
    return rnd(x, 3)


def ep(x):
    """A timestamp field (DuckDB epoch(...) float, or None) -> int seconds."""
    return None if x is None else int(round(x))


def dump(obj, name, outdir):
    path = os.path.join(outdir, name)
    with open(path, "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    return path


def build_label(host, sha, t):
    for h, when, label, _url in FIXES:
        if h == host and abs(t - utc(when)) < 1800:
            m = re.match(r"(#\d+(?:\u2013#\d+)?)", label)
            return f"pbt + {m.group(1)}" if m else label
    return f"build {sha}"


def n1():
    return {r[0]: r for r in q("""SELECT host, pre_p50_ms, first10_p50_ms, first10_max_ms, post_p50_ms, non_valid_first_hour,
                                   first_valid_pbt_block_t, rss_pre_gib, rss_post_gib, datadir_pre_gib, datadir_post_min_gib,
                                   disk_released_gib FROM n1_fork""")}


def n2():
    return {r[0]: r for r in q("""SELECT host, pre_blocks, pre_p50_ms, pre_p95_ms, post_blocks, post_p50_ms, post_p95_ms,
                                   post_prefix_blocks, post_prefix_p50_ms, post_over_pre FROM n2_postfork_cost""")}


def state_costs():
    """Per host x period: median ms/MGas, ms/block, n (full VALID blocks). Mirrors make_figures._state_costs()."""
    rows = q(f"""WITH per AS (SELECT host, t0, CASE WHEN t1 > to_timestamp({FORK}) THEN to_timestamp({FORK}) ELSE t1 END AS t1,
                              {PERIOD_SQL} AS period FROM c0_states)
                 SELECT b.host, p.period, median(b.np_ms / (b.gas_used / 1e6)), median(b.np_ms), count(*)
                 FROM blocks_node b JOIN per p ON p.host = b.host AND b.t >= p.t0 AND b.t < p.t1
                 WHERE p.period IS NOT NULL AND b.status = 'VALID' AND b.gas_used >= 50e6 GROUP BY 1, 2""")
    d = {(h, p): (a, b, n) for h, p, a, b, n in rows}
    for h in HOSTS:
        start = utc(BIN_START[h]) if h in BIN_START else FORK + 3600
        a, b, n = q(f"""SELECT median(np_ms / (gas_used / 1e6)), median(np_ms), count(*) FROM blocks_node
                        WHERE host = '{h}' AND status = 'VALID' AND gas_used >= 50e6 AND t >= to_timestamp({start})""")[0]
        d[(h, "binary only")] = (a, b, n)
    return d


# ------------------------------------------------------------------------------------------------------- devnet.json
def devnet():
    anchor_root = q(f"SELECT state_root FROM rpc_headers WHERE number = {ANCHOR}")[0][0]
    chain_end_t = q("SELECT epoch(max(t)) FROM workload_devnet WHERE t IS NOT NULL")[0][0]
    chain_end_block = q("SELECT max(number) FROM workload_devnet")[0][0]
    fork_epoch = 50400 // 32  # slot at FORK // slots-per-epoch
    wct = q(f"""SELECT epoch(t) FROM network_events WHERE kind = 'finalized' AND epoch > {fork_epoch}
                ORDER BY t LIMIT 1""")[0][0]
    leaves = q("SELECT leaves FROM a1_runs WHERE run_id = 'geth-1/import'")[0][0]
    roots_tot = q("""SELECT count(*), sum(nodes_with_root * (nodes_with_root - 1) / 2), sum((distinct_roots > 1)::INT)
                      FROM roots WHERE nodes_with_root >= 2""")[0]
    per_host = {h: n for h, n in q("SELECT host, comparisons FROM l1_roots")}
    builds = {}
    for h, sha, t0 in q("SELECT host, build, epoch(first_start) FROM p1_builds ORDER BY host, first_start"):
        builds.setdefault(h, []).append({"t": ep(t0), "sha": sha, "label": build_label(h, sha, t0)})
    nodes = [{"host": h, "client": client(h), "cap_gib": CAP[h], "methods": METHODS[h], "builds": builds.get(h, [])}
             for h in HOSTS]
    mn = q("""SELECT avg(gas_used) / 1e6, avg(txs), avg(slots_written), avg(contracts)
              FROM workload_mainnet WHERE "window" = 'recent'""")[0]
    pre = q("""SELECT avg(gas_used) / 1e6, avg(txs), avg(slot_writes), avg(code_changes)
               FROM workload_devnet WHERE era = 'live, before load' AND slot_writes IS NOT NULL""")[0]
    xen = q("""SELECT avg(gas_used) / 1e6, avg(txs), avg(slot_writes), avg(code_changes)
               FROM workload_devnet WHERE era = 'live, XEN load' AND slot_writes IS NOT NULL AND gas_used >= 150e6""")[0]
    syn = q("""SELECT avg(gas_used) / 1e6, avg(txs), avg(slot_writes), avg(code_changes)
               FROM workload_devnet WHERE era = 'synthetic N+1\u2192genesis' AND slot_writes IS NOT NULL""")[0]
    empty = q("SELECT avg((gas_used < 5e6)::INT) FROM workload_devnet WHERE era = 'live, XEN load'")[0][0]
    eras = [
        {"id": "mainnet", "label": "mainnet today", "mgas": rnd(mn[0], 1), "txs": rnd(mn[1], 1), "writes": rnd(mn[2], 1), "creates": rnd(mn[3], 1)},
        {"id": "pre", "label": "devnet before the load", "mgas": rnd(pre[0], 1), "txs": rnd(pre[1], 1), "writes": rnd(pre[2], 1), "creates": rnd(pre[3], 1)},
        {"id": "xen", "label": "XEN load, full blocks", "mgas": rnd(xen[0], 1), "txs": rnd(xen[1], 1), "writes": rnd(xen[2], 1), "creates": rnd(xen[3], 1)},
        {"id": "synthetic", "label": "replay window (synthetic)", "mgas": rnd(syn[0], 1), "txs": rnd(syn[1], 1), "writes": rnd(syn[2], 1), "creates": rnd(syn[3], 1)},
    ]
    rows = q("""SELECT epoch(date_trunc('hour', t)), sum(gas_used) / 3600 / 1e6, sum(txs) / 3600.0
                FROM workload_devnet WHERE era LIKE 'live%' AND t IS NOT NULL GROUP BY 1 HAVING count(*) >= 60 ORDER BY 1""")
    pre_g = [r for r in rows if utc("10-24 00:00") <= r[0] < XEN_T]
    under_g = [r for r in rows if XEN_T + 3600 < r[0] < FORK]
    after_g = [r for r in rows if r[0] > FORK + 3600]
    g0 = sum(r[1] for r in pre_g) / len(pre_g)
    g1 = sum(r[1] for r in under_g) / len(under_g)
    g2 = sum(r[1] for r in after_g) / len(after_g)
    return {
        "_views": ["boundaries", "rpc_headers", "a1_runs", "p1_builds", "roots", "l1_roots", "workload_devnet", "workload_mainnet"],
        "id": "pbt-devnet-1", "status": "terminated",
        "window": {"t0": T0, "t1": ep(chain_end_t), "chain_end_block": int(chain_end_block)},
        "fork": {"t": FORK, "block": FORK_BLOCK, "window_close_t": ep(wct), "window_close_min": round((wct - FORK) / 60)},
        "xen_t": XEN_T,
        "state": {"leaves": int(leaves), "accounts": 355000000, "slots": 1820000000, "codes": 2500000,
                  "vs_mainnet_leaves": f"+~{round((leaves / 3.02e9 - 1) * 100)} %",
                  "anchor_block": ANCHOR, "anchor_root": anchor_root},
        "hosts": {"vcpu": 16, "ram_gib": 125.8, "disk_tib": 2.31},
        "eip7870": {"full_ram_gb": 32, "attester_ram_gb": 64, "disk_tb": 4, "full_cpu": "4c / 8t", "attester_cpu": "8c / 16t"},
        "nodes": nodes,
        "roots": {"heights": int(roots_tot[0]), "comparisons": int(roots_tot[1]), "divergences": int(roots_tot[2]),
                  "per_host": {h: int(per_host[h]) for h in HOSTS}},
        "load": {"eras": eras, "xen_empty_share": frac(empty), "synthetic_blocks": 50400,
                 "mgas_s": {"before": rnd(g0, 2), "under": rnd(g1, 2), "after_fork": rnd(g2, 2), "mainnet": rnd(30.4 / 12, 2)},
                 "hourly": [[ep(t), rnd(mg, 2), rnd(tx, 2)] for t, mg, tx in rows]},
    }


# ----------------------------------------------------------------------------------------------------- timeline.json
def timeline():
    runs = q("""SELECT run_id, host, method, status, epoch(start), epoch(complete), epoch(synced), t_total_s
                FROM runs WHERE method IN ('import', 'self') AND start IS NOT NULL ORDER BY host, start""")
    dual = {h: ep(t) for h, t in q("SELECT host, epoch(min(t0)) FROM c0_states WHERE state LIKE 'dual (hex+bin)%' GROUP BY 1")}
    down = {}
    t0_iso = datetime.fromtimestamp(T0, timezone.utc).isoformat()
    for h, hr in q(f"SELECT host, epoch(hour) FROM h2_node_hourly WHERE valid_share < 0.5 AND hour >= TIMESTAMPTZ '{t0_iso}'"):
        down.setdefault(h, []).append(ep(hr))
    kind_map = {"oom_kill": "oom_kill", "jvm_heap_oom": "jvm_heap_oom", "segv": "segv",
                "replay_stall": "replay_stall", "crash_restart": "crash_loop", "shadow_stopped": "replay_stall"}
    recov = {}
    for h, t, valid_again in q("""SELECT host, epoch(ts), epoch((SELECT min(b.t) FROM blocks_node b
                                   WHERE b.host = f.host AND b.status = 'VALID' AND b.t > f.ts + INTERVAL 30 SECOND))
                                   FROM failures f WHERE kind IN ('oom_kill', 'jvm_heap_oom', 'segv')"""):
        recov[(h, round(t))] = ep(valid_again)
    events_by_host = {}
    for h, t, k, detail in q("SELECT host, epoch(ts), kind, detail FROM failures ORDER BY host, ts"):
        if k not in kind_map:
            continue
        events_by_host.setdefault(h, []).append({"kind": kind_map[k], "t": ep(t), "recovered_t": recov.get((h, round(t))), "note": detail})
    builds = {}
    for h, sha, t0 in q("SELECT host, build, epoch(first_start) FROM p1_builds ORDER BY host, first_start"):
        builds.setdefault(h, []).append({"t": ep(t0), "sha": sha, "label": build_label(h, sha, t0)})
    lanes = []
    for h in HOSTS:
        lane_runs = [{"run_id": rid, "method": method, "status": status, "start": ep(start), "complete": ep(complete),
                       "synced": ep(synced), "t_total_s": ms(total)}
                     for rid, host, method, status, start, complete, synced, total in runs if host == h]
        lanes.append({"host": h, "client": client(h), "cap_gib": CAP[h], "runs": lane_runs,
                      "dual_from": dual.get(h), "down_hours": down.get(h, []),
                      "events": events_by_host.get(h, []), "builds": builds.get(h, [])})
    chain_end_t = q("SELECT epoch(max(t)) FROM workload_devnet WHERE t IS NOT NULL")[0][0]
    return {"_views": ["runs", "c0_states", "h2_node_hourly", "failures", "k2_recovery", "p1_builds"],
            "t0": T0, "t1": ep(chain_end_t), "xen_t": XEN_T, "fork_t": FORK, "lanes": lanes}


# ------------------------------------------------------------------------------------------------------ methods.json
def methods_json():
    runs = {r[0]: r for r in q("SELECT run_id, host, client, method, build, t_core_s, t_total_s, replay_blocks FROM a1_runs WHERE status = 'valid'")}
    # a2_phases.class: verification -> verify, replay -> replay, construction/commit -> build (U0 already excluded).
    ph = q("""SELECT p.run_id, p.phase, p.class, epoch(p.start) - epoch(a.start), p.dur_s
              FROM a2_phases p JOIN a1_runs a USING (run_id) ORDER BY p.run_id, p.start""")
    mem = {(r, s): frac(v) for r, s, v in q("SELECT run_id, span, rss_peak_frac FROM c1_run_resources")}
    ooms_in_run = {r: n for r, n in q("""SELECT a.run_id, count(*) FROM a1_runs a JOIN failures f ON f.host = a.host
                        AND f.kind IN ('oom_kill', 'jvm_heap_oom') AND f.ts >= a.start AND f.ts < coalesce(a.synced, a.complete)
                        WHERE a.status = 'valid' GROUP BY 1""")}
    kind_of = {"verification": "verify", "replay": "replay", "construction": "build", "commit": "build"}
    run_list = []
    for rid in RUNS_ALL:
        _, host, cl, method, build, core, total, replay_blocks = runs[rid]
        phs = [{"name": phase, "kind": kind_of[cls], "start_s": round(off), "end_s": round(off + dur)}
               for _rid, phase, cls, off, dur in ph if _rid == rid and cls != "wait"]
        run_list.append({
            "run_id": rid, "host": host, "client": cl, "cap_gib": CAP[host], "method": method, "status": "valid",
            "build": build, "t_core_s": ms(core), "t_total_s": ms(total),
            "replay_blocks": int(replay_blocks) if replay_blocks is not None else None,
            "mem_core": mem.get((rid, "core")), "mem_replay": mem.get((rid, "replay")),
            "verify": VERIFY[rid], "oom_kills": int(ooms_in_run.get(rid, 0)), "phases": phs,
        })
    proj = {}
    for cl, tci, tcs, r_mainnet, be in q("SELECT client, t_core_i, t_core_s, r_mainnet, breakeven_days FROM j2_projection WHERE cap_gib = 16"):
        basis_imp = {"geth": "geth-1", "nethermind": "nethermind-1", "besu": "besu-1"}[cl]
        basis_self = {"geth": "geth-3", "nethermind": "nethermind-1", "besu": "besu-1"}[cl]
        import_h = [round((tci + (a * 7200 + tci / 12) / (r_mainnet - 1 / 12)) / 3600, 2) for a in range(41)]
        self_h = round((tcs + (tcs / 12) / (r_mainnet - 1 / 12)) / 3600, 2)
        breakeven_d = round(be, 1) if be > 0 else None
        proj[cl] = {"import_h": import_h, "self_h": self_h, "breakeven_d": breakeven_d,
                    "wins": "self" if breakeven_d is None else "import", "basis": f"{basis_imp} import, {basis_self} self"}
    return {"_views": ["a1_runs", "a2_phases", "c1_run_resources", "failures", "j2_projection"],
            "runs": run_list, "projection": {"ages_d": list(range(41)), "clients": proj}}


# --------------------------------------------------------------------------------------------------- validating.json
def validating():
    costs = state_costs()
    grid = {}
    for h in HOSTS:
        row = {}
        for s in STATES:
            v = costs.get((h, s))
            if v is None or v[2] < 20:
                continue
            row[s] = {"ms_per_mgas": rnd(v[0], 2), "ms_per_block": ms(v[1]), "n": int(v[2])}
        grid[h] = row
    n2v = n2()
    ratio = {h: {"pre_ms": ms(n2v[h][2]), "post_ms": ms(n2v[h][5]), "ratio": rnd(n2v[h][9], 2)} for h in HOSTS if h in n2v}
    binary_only_from = {h: utc(BIN_START[h]) if h in BIN_START else FORK + 3600 for h in HOSTS}
    hourly = {}
    for h, t, p50, n in q("""SELECT host, epoch(date_trunc('hour', t)), median(np_ms), count(*) FROM blocks_node
                             WHERE status = 'VALID' AND gas_used >= 50e6 GROUP BY 1, 2 ORDER BY 1, 2"""):
        if n < 20:
            continue
        hourly.setdefault(h, []).append([ep(t), ms(p50), int(n)])
    migrating_spans = {}
    for h, start, synced in q("SELECT host, epoch(start), epoch(synced) FROM a1_runs WHERE status = 'valid' AND synced IS NOT NULL ORDER BY host, start"):
        migrating_spans.setdefault(h, []).append([ep(start), ep(synced)])
    replay = []
    for rid, live, syn, blk, mins in q("""SELECT run_id, live_blk_s, synthetic_blk_s, blk_s, minutes_per_mainnet_day FROM d4_replay_mainnet_minutes ORDER BY run_id"""):
        lv = live if live and live > 0 else blk
        replay.append({"run_id": rid, "live_blk_s": rnd(lv, 1), "synthetic_blk_s": rnd(syn or 0, 1),
                        "min_per_mainnet_day_live": rnd(7200 / lv / 60, 1), "min_per_mainnet_day_write_scaled": rnd(mins, 1)})
    return {"_views": ["c0_states", "blocks_node", "n2_postfork_cost", "a1_runs", "d4_replay_mainnet_minutes"],
            "states": STATES, "grid": grid, "ratio": ratio, "binary_only_from": binary_only_from,
            "hourly": hourly, "migrating_spans": migrating_spans, "replay": replay}


# --------------------------------------------------------------------------------------------------------- cost.json
def cost():
    chain_end = q("SELECT epoch(max(t)) FROM workload_devnet WHERE t IS NOT NULL")[0][0]
    hours = {(h, p): v for h, p, v in q(f"""WITH per AS (SELECT host, t0, CASE WHEN t1 > to_timestamp({FORK}) THEN to_timestamp({FORK}) ELSE t1 END AS t1,
                              {PERIOD_SQL} AS period FROM c0_states
                              UNION ALL SELECT host, to_timestamp({FORK}) + INTERVAL 1 HOUR, to_timestamp({chain_end}), 'binary only'
                              FROM (SELECT DISTINCT host FROM resources))
                 SELECT host, period, sum(epoch(t1) - epoch(t0)) / 3600.0 FROM per WHERE period IS NOT NULL GROUP BY 1, 2""")}
    rows = q(f"""WITH per AS (SELECT host, t0, CASE WHEN t1 > to_timestamp({FORK}) THEN to_timestamp({FORK}) ELSE t1 END AS t1,
                              {PERIOD_SQL} AS period FROM c0_states
                              UNION ALL SELECT host, to_timestamp({FORK}) + INTERVAL 1 HOUR, to_timestamp({chain_end}), 'binary only'
                              FROM (SELECT DISTINCT host FROM resources)),
                      oom AS (SELECT host, ts FROM failures WHERE kind IN ('oom_kill', 'jvm_heap_oom'))
                 SELECT p.host, p.period, avg(r.host_cpu_busy), avg(r.disk_util), avg(r.psi_io_wait),
                        quantile_cont(r.mem_rss / r.mem_limit, 0.95) FILTER (WHERE NOT EXISTS
                          (SELECT 1 FROM oom o WHERE o.host = r.host AND abs(epoch(r.t) - epoch(o.ts)) < 600)),
                        count(*)
                 FROM per p JOIN resources r ON r.host = p.host AND r.t >= p.t0 AND r.t < p.t1
                 WHERE p.period IS NOT NULL AND r.mem_limit > 0 GROUP BY 1, 2""")
    machines = {}
    for h, p, cpu, disk, io, mem, n in rows:
        if n < 20:
            continue
        machines.setdefault(h, {})[p] = {"cpu_cores": rnd(cpu, 1), "disk_busy": frac(disk), "io_wait": frac(io),
                                          "mem_p95": frac(mem), "hours": rnd(hours.get((h, p), 0), 1)}
    first = {}
    for h, a in q("SELECT host, min(epoch(start)) FROM a1_runs WHERE status = 'valid' AND synced IS NOT NULL GROUP BY 1"):
        first[h] = a
    buckets = q("""SELECT host, epoch(time_bucket(INTERVAL '10 min', t)), max(mem_rss / mem_limit) FROM resources
                   WHERE mem_rss IS NOT NULL AND mem_limit > 0 GROUP BY 1, 2 ORDER BY 1, 2""")
    series = {}
    for h, tb, peak in buckets:
        series.setdefault(h, []).append([ep(tb), frac(peak)])
    cap_changes = {}
    for h in ("geth-1", "geth-2", "geth-3"):
        for t, lim in q(f"""WITH s AS (SELECT t, mem_limit, lag(mem_limit) OVER (ORDER BY t) AS prev FROM resources
                             WHERE host = '{h}' AND mem_limit IS NOT NULL)
                             SELECT epoch(t), mem_limit FROM s WHERE prev IS NOT NULL AND mem_limit <> prev ORDER BY t"""):
            cap_changes.setdefault(h, []).append([ep(t), round(lim / 2 ** 30)])
    ooms = {}
    for h, t in q("SELECT host, epoch(ts) FROM failures WHERE kind IN ('oom_kill', 'jvm_heap_oom') ORDER BY host, ts"):
        if t >= first.get(h, 0):
            ooms.setdefault(h, []).append(ep(t))
    memory = {h: {"cap_gib": CAP[h], "counted_from": ep(first.get(h)), "series": series.get(h, []),
                  "cap_changes": cap_changes.get(h, []), "oom": ooms.get(h, [])} for h in HOSTS}
    io = {(r[0], r[1]): r[2:] for r in q("SELECT run_id, span, written_gib, datadir_scratch_gib FROM c1_run_resources")}
    fork = n1()
    disk_runs = []
    for rid in RUNS16:
        host = rid.split("/")[0]
        peak = q(f"""SELECT max(r.datadir) / 2^40 FROM resources r JOIN a1_runs a ON a.host = r.host AND a.run_id = '{rid}'
                     WHERE r.host = '{host}' AND r.t BETWEEN a.start AND a.synced""")[0][0]
        core, scratch = io.get((rid, "core"), (0, 0))
        rep = io.get((rid, "replay"), (0, 0))[0] or 0
        input_gib = q(f"""SELECT max(r.mig_input) / 2^30 FROM resources r JOIN a1_runs a ON a.host = r.host AND a.run_id = '{rid}'
                          WHERE r.host = '{host}' AND r.t BETWEEN a.start AND a.complete""")[0][0]
        disk_runs.append({"run_id": rid, "peak_datadir_tib": rnd(peak, 2), "written_core_gib": gib(core or 0),
                           "written_replay_gib": gib(rep), "input_gib": gib(input_gib) if input_gib else None,
                           "scratch_gib": gib(scratch or 0), "released_gib": round(fork[host][11])})
    return {"_views": ["c0_states", "resources", "failures", "c1_run_resources", "n1_fork"],
            "states": STATES, "machines": machines, "memory": memory,
            "disk": {"fitted_tib": 2.31, "eip7870_tib": rnd(4e12 / 2 ** 40, 2), "runs": disk_runs}}


# -------------------------------------------------------------------------------------------------------- fork.json
def fork_json():
    rows = q(f"""SELECT host, floor((epoch(t) - {FORK}) / 120) * 120 + 60 AS dt, median(np_ms)
                 FROM blocks_node WHERE epoch(t) BETWEEN {FORK} - 7200 AND {FORK} + 21600 AND status = 'VALID'
                 GROUP BY 1, 2 ORDER BY 1, 2""")
    by_host = {}
    for h, dt, ms_ in rows:
        by_host.setdefault(h, {})[int(dt)] = ms(ms_)
    bins = list(range(-7200 + 60, 21600, 120))
    series = {h: [[b, by_host.get(h, {}).get(b)] for b in bins] for h in HOSTS}
    first = n1()
    first10 = {h: {"p50_ms": ms(first[h][2]), "max_ms": ms(first[h][3]), "non_valid_first_hour": int(first[h][5])} for h in HOSTS}
    costv = n2()
    label = {"geth-1": "fixed build #80\u2013#82", "geth-3": "fixed build #80\u2013#82", "geth-2": "#83 (last 3 h)"}
    after = {}
    for h in HOSTS:
        f, c = first[h], costv[h]
        after[h] = {"disk_released_gib": round(f[11]), "rss_pre_gib": gib(f[7]), "rss_post_gib": gib(f[8]),
                    "cap_post_gib": 64 if client(h) == "geth" else CAP[h], "pre_ms": ms(c[2]), "post_ms": ms(c[5]),
                    "ratio": rnd(c[9], 2), "post_blocks": int(c[4]), "build": label.get(h, "unchanged")}
    slot_missed = q("SELECT missed FROM network WHERE slot = 50400")[0][0]
    part = q(f"SELECT quantile_cont(part, 0.5) FROM watch_samples WHERE t BETWEEN {FORK - 60} AND {FORK + 60} AND part IS NOT NULL")[0][0]
    return {"_views": ["blocks_node", "n1_fork", "n2_postfork_cost", "network", "watch_samples"],
            "fork_t": FORK, "fork_block": FORK_BLOCK,
            "window_close_t": None,
            "series": series, "first10": first10, "after": after,
            "boundary": {"participation": rnd(part, 3) if part is not None else None, "missed_slots": int(bool(slot_missed))}}


# ----------------------------------------------------------------------------------------------------- network.json
def network_json():
    hrs = q("SELECT epoch(hour), max_finality_lag_epochs, participation_p50, nodes_validating FROM h4_finality_network ORDER BY hour")
    missed = {ep(h): m for h, m in q("SELECT epoch(hour), missed FROM h1_network_hourly")}
    hourly = [{"t": ep(t), "participation": rnd(p, 3) if p is not None else None, "validating": nv,
               "finality_lag": lag, "missed": missed.get(ep(t))} for t, lag, p, nv in hrs]
    valid_share = {}
    for h, t, s in q("SELECT host, epoch(hour), valid_share FROM h2_node_hourly ORDER BY host, hour"):
        valid_share.setdefault(h, []).append([ep(t), frac(s)])
    tot = q("SELECT sum(slots), sum(missed) FROM h1_network_hourly")[0]
    reorg = q("SELECT sum(network_reorgs), sum(sentry_reorg_events) FROM h4_finality_network")[0]
    att = q("""WITH j AS (SELECT h.hour, h.missed, 9 - coalesce(f.nodes_validating, 9) AS down
               FROM h1_network_hourly h LEFT JOIN h4_finality_network f USING (hour))
               SELECT corr(missed, down), sum(missed) FILTER (WHERE down > 0) / sum(missed), sum(missed) FILTER (WHERE down = 0) FROM j""")[0]
    totals = {"slots": int(tot[0]), "missed": int(tot[1]), "missed_share_node_down": rnd(att[1], 3),
              "corr": rnd(att[0], 2), "missed_all_up": int(att[2]), "sentry_reorgs": int(reorg[1] or 0),
              "network_reorgs": int(reorg[0] or 0)}
    ledger = []
    recov = {}
    for h, t, restarted, valid_again, recover_s in q("""SELECT host, epoch(ts), epoch(restarted), epoch(valid_again), recover_s FROM k2_recovery"""):
        recov[(h, round(t))] = (ep(valid_again), rnd(recover_s, 0))
    for h, t, k, detail in q("SELECT host, epoch(ts), kind, detail FROM failures ORDER BY host, ts"):
        rt, rs = recov.get((h, round(t)), (None, None))
        ledger.append({"host": h, "kind": k, "t": ep(t), "recovered_t": rt, "recovery_s": rs, "note": detail})
    fixes = [{"host": h, "t": utc(when), "label": label, "url": url} for h, when, label, url in FIXES]
    return {"_views": ["h1_network_hourly", "h2_node_hourly", "h4_finality_network", "failures", "k2_recovery"],
            "hourly": hourly, "valid_share": valid_share, "totals": totals,
            "pause": {"from": utc(PAUSE["from"]), "to": utc(PAUSE["to"]), "epochs": PAUSE["epochs"],
                      "participation_min": PAUSE["participation_min"], "validating": PAUSE["validating"], "down": PAUSE["down"]},
            "ledger": ledger, "fixes": fixes}


def window_close_t():
    fork_epoch = 50400 // 32
    return ep(q(f"SELECT epoch(t) FROM network_events WHERE kind = 'finalized' AND epoch > {fork_epoch} ORDER BY t LIMIT 1")[0][0])


# ---------------------------------------------------------------------------------------------------------- --check
def check():
    import math
    ref = json.load(open(NUMBERS_JSON))
    rows = []

    def cmp(label, got, want, tol=0.05):
        if want is None:
            return
        ok = abs(got - want) <= max(tol, abs(want) * 0.02)
        rows.append((label, got, want, "OK" if ok else "FAIL"))

    cmp("f1 sampled_heights", q("SELECT count(*) FROM roots WHERE nodes_with_root >= 2")[0][0], ref["f1"]["sampled_heights"])
    cmp("f1 pairwise_comparisons", q("SELECT sum(nodes_with_root * (nodes_with_root - 1) / 2) FROM roots WHERE nodes_with_root >= 2")[0][0], ref["f1"]["pairwise_comparisons"])
    cmp("f1 divergences", q("SELECT sum((distinct_roots > 1)::INT) FROM roots WHERE nodes_with_root >= 2")[0][0], ref["f1"]["divergences"])
    for rid in RUNS_ALL:
        host = rid.split("/")[0]
        peak = q(f"""SELECT max(r.datadir) / 2^40 FROM resources r JOIN a1_runs a ON a.host = r.host AND a.run_id = '{rid}'
                     WHERE r.host = '{host}' AND r.t BETWEEN a.start AND a.synced""")[0][0]
        cmp(f"f5 {rid} peak_datadir_tib", round(peak, 2), ref["f5"].get(f"{rid} peak_datadir_tib"), tol=0.02)
    for rid in RUNS16:
        core, total = q(f"SELECT t_core_s, t_total_s FROM a1_runs WHERE run_id = '{rid}'")[0]
        fig = "f6" if rid in ("geth-1/import", "nethermind-1/import", "besu-1/import") else "f7"
        key = "f6-import-phases" if fig == "f6" else "f7-self-phases"
        want_core = ref.get(key, {}).get(f"{rid} t_core")
        want_total = ref.get(key, {}).get(f"{rid} t_total")
        if want_core:
            rows.append((f"{key} {rid} t_core", fmt_dur(core), want_core, "OK" if fmt_dur(core) == want_core else "FAIL"))
        if want_total:
            rows.append((f"{key} {rid} t_total", fmt_dur(total), want_total, "OK" if fmt_dur(total) == want_total else "FAIL"))
    costs = state_costs()
    for h in HOSTS:
        a = costs.get((h, "two trees + load"))
        b = costs.get((h, "binary only"))
        if a and b:
            cmp(f"f9 {h} ratio_binary_over_two_trees_load", round(b[1] / a[1], 2), ref["f9"].get(f"{h} ratio_binary_over_two_trees_load"), tol=0.03)
    for rid, live, syn, blk in q("SELECT run_id, live_blk_s, synthetic_blk_s, blk_s FROM d4_replay_mainnet_minutes"):
        lv = live if live and live > 0 else blk
        cmp(f"f10 {rid} live_blk_s", round(lv, 1), ref["f10"].get(f"{rid} live_blk_s"))
    fork = n1()
    for h in HOSTS:
        cmp(f"f14 {h} disk_released_gib", round(fork[h][11]), ref["f14"].get(f"{h} disk_released_gib"), tol=1)
    tot = q("SELECT sum(slots), sum(missed) FROM h1_network_hourly")[0]
    cmp("f20 slots", int(tot[0]), ref["f20"]["slots"], tol=50)
    cmp("f20 missed", int(tot[1]), ref["f20"]["missed"], tol=5)
    width = max(len(r[0]) for r in rows)
    for label, got, want, status in rows:
        print(f"{label:<{width}}  got={got!r:<14} want={want!r:<14} {status}")
    fails = [r for r in rows if r[3] == "FAIL"]
    print(f"\n{len(rows)} checks, {len(fails)} failed")
    return not fails


def fmt_dur(s):
    s = int(round(s))
    h, m = s // 3600, (s % 3600) // 60
    return (f"{h}h{m:02d}m" if h else f"{m}m{s % 60:02d}s") if h or m >= 1 else f"{s}s"


def main():
    global DB
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    os.chdir(ROOT)
    DB = connect()
    os.makedirs(args.out, exist_ok=True)
    wct = window_close_t()
    d = devnet()
    d["fork"]["window_close_t"] = wct
    f = fork_json()
    f["window_close_t"] = wct
    files = {"devnet.json": d, "timeline.json": timeline(), "methods.json": methods_json(),
             "validating.json": validating(), "cost.json": cost(), "fork.json": f, "network.json": network_json()}
    for name, obj in files.items():
        path = dump(obj, name, args.out)
        print(f"{name}: {os.path.getsize(path)} bytes")
    if args.check:
        ok = check()
        raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
