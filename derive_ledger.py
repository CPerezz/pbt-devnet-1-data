"""Run, phase and failure ledgers (data/derived).

runs.parquet     one row per migration run or attempt: deploy, migration start, tree
                 complete, synced at the head, the anchor block, durations, validity.
phases.parquet   the universal phases U0..U7 of every run, from log markers.
failures.parquet OOM kills (host counter and container events), crashes (a process
                 start with no clean stop before it), JVM heap OOMs, disk-full,
                 replay stalls, shadow stops.
Run windows are curated (plan §2); every timestamp inside them comes from the logs,
the watch's samples, or Prometheus.
"""
import json
import sys

import duckdb
import pandas as pd

from common import ANCHOR_N, DER, RAW, log

T = lambda s: pd.Timestamp(f"2026-{s}", tz="UTC")  # noqa: E731

# run_id, host, method, build, deploy, window end, status, note
RUNS = [
    ("geth-1/import", "geth-1", "import", "84a12eff9", "10-22 09:38:42", None, "valid", ""),
    ("geth-2/import", "geth-2", "import", "84a12eff9", "10-22 10:32:28", None, "valid", ""),
    ("geth-3/self", "geth-3", "self", "84a12eff9", "10-22 09:51:24", None, "valid", ""),
    ("nethermind-1/import", "nethermind-1", "import", "713979e7", "10-23 11:55:50", "10-27 00:29:27", "valid",
     "28 OOM kills during the replay, each resumed"),
    ("nethermind-1/self", "nethermind-1", "self", "3f29bb53", "10-27 00:29:27", None, "valid", ""),
    ("nethermind-2/import", "nethermind-2", "import", "f4e29519", "10-22 10:21:06", None, "valid", ""),
    ("besu-1/import-run1", "besu-1", "import", "39377a2", "10-22 09:38:42", "10-22 22:34:07", "superseded",
     "old build: unbatched catch-up; operator stop 20:27"),
    ("besu-1/import-run2", "besu-1", "import", "76a88a1", "10-22 22:34:07", "10-23 07:44:32", "failed",
     "JVM heap OOMs (one restarted the import); operator stop 05:34"),
    ("besu-1/import", "besu-1", "import", "76a88a1", "10-23 07:44:32", "10-27 02:36:04", "valid", "run 3"),
    ("besu-1/self", "besu-1", "self", "cc56425", "10-27 02:36:04", None, "valid", "datadir restored ~30 k blocks back"),
    ("besu-2/import-run1", "besu-2", "import", "39377a2", "10-22 10:12:15", "10-23 13:20:25", "reference",
     "old build: unbatched catch-up"),
    ("besu-2/import", "besu-2", "import", "cd2e227", "10-23 13:20:25", None, "valid", "rerun"),
    ("erigon-1/dual", "erigon-1", "dual", "9ac7b462→fb9da7fd", "10-25 06:26:52", None, "valid", "hex+bin since this start"),
    ("erigon-2/dual", "erigon-2", "dual", "9ac7b462→fb9da7fd", "10-25 07:47:37", None, "valid", "hex+bin since this start"),
]
# Phase rules: phase -> (start, end); each side is ("first"|"last", marker[, text filter]) or "deploy".
P_IMPORT_GETH = {
    "U0 wait": ("deploy", ("first", "import_start")),
    "U1 preimages": (("first", "import_start"), ("first", "preimages_done")),
    "U2 leaves+check1": (("first", "preimages_done"), ("first", "check1_done")),
    "U3 write nodes": (("first", "check1_done"), ("first", "code_progress")),
    "U4 code": (("first", "code_progress"), ("first", "code_done")),
    "U5 check2": (("first", "check2_progress"), ("first", "check2_done")),
    "U6 read-back": (("first", "check2_done"), ("first", "import_complete")),
}
P_SELF_GETH = {
    "U0 wait": ("deploy", ("first", "convert_start")),
    "U2 scan": (("first", "convert_start"), ("first", "build_progress")),
    "U3 build+write": (("first", "build_progress"), ("first", "code_progress")),
    "U4 code": (("first", "code_progress"), ("first", "code_done")),
    "U5 check2": (("first", "check2_progress"), ("first", "check2_done")),
    "U6 read-back": (("first", "check2_done"), ("first", "convert_complete")),
}
P_IMPORT_NM = {
    "U0 wait": ("deploy", ("first", "nm_start")),
    "U1 preimages": (("first", "preimages_progress"), ("last", "preimages_progress")),
    "U2+U3 leaves+build": (("first", "import_progress"), ("first", "rebuild_done")),
    "U5 check2": (("first", "check2_progress"), ("first", "check2_done")),
    "U6 commit": (("first", "nm_commit"), ("first", "import_complete")),
}
P_SELF_NM = {
    "U0 wait": ("deploy", ("first", "self_start")),
    "U1 BAL keys": (("first", "self_start"), ("first", "self_scan_progress")),
    "U2 scan": (("first", "self_scan_progress"), ("last", "self_scan_progress")),
    "U3 build": (("first", "import_progress"), ("first", "rebuild_done")),
    "U6 commit": (("first", "rebuild_done"), ("first", "self_complete")),
}
P_IMPORT_BESU = {
    "U0 wait": ("deploy", ("first", "import_start")),
    # 39377a2 / 76a88a1 log no step lines: check 1 spans preimages, leaves and the build; the gap to check 2 is the MPT pass.
    "U1-U3 preimages+leaves+build (check 1)": (("first", "import_start"), ("first", "check1_done")),
    "U5 check2": (("first", "check1_done"), ("first", "check2_done")),
    "U6 commit": (("first", "check2_done"), ("first", "import_complete")),
}
P_SELF_BESU = {
    "U0 wait": ("deploy", ("first", "self_start")),
    "U1 BAL keys": (("first", "self_stage", "preimages from the BALs"), ("first", "self_stage", "reading the flat database")),
    "U2 scan": (("first", "self_stage", "reading the flat database"), ("first", "self_stage", "codes of")),
    "U4 code": (("first", "self_stage", "codes of"), ("first", "self_stage", "building the PBT")),
    "U3 build": (("first", "self_stage", "building the PBT"), ("first", "self_done")),
    "U6 commit": (("first", "self_done"), ("first", "self_complete")),
}
RULES = {("geth", "import"): P_IMPORT_GETH, ("geth", "self"): P_SELF_GETH, ("nethermind", "import"): P_IMPORT_NM,
         ("nethermind", "self"): P_SELF_NM, ("besu", "import"): P_IMPORT_BESU, ("besu", "self"): P_SELF_BESU}
START = {"import": {"geth": "import_start", "nethermind": "preimages_progress", "besu": "import_start"},
         "self": {"geth": "convert_start", "nethermind": "self_start", "besu": "self_start"}}
DONE = {"import": {"geth": "import_complete", "nethermind": "import_complete", "besu": "import_complete"},
        "self": {"geth": "convert_complete", "nethermind": "self_complete", "besu": "self_complete"}}
# Replay ends the watch could not see (the Mac slept); each override names its evidence.
SYNCED_OVERRIDE = {
    "nethermind-1/self": ("10-27 05:31:00", "inferred: container CPU (2.1 → 0.5 cores) and disk writes back to baseline "
                                            "at 05:31, ~2 min after the 05:29 restart; the watch slept 02:01–06:40"),
}


def main():
    con = duckdb.connect()
    con.sql("SET TimeZone='UTC'")
    mk = con.sql(f"SELECT host, ts, marker, text, block FROM '{DER}/markers.parquet'").df()
    lc = con.sql(f"SELECT host, ts, kind, build FROM '{DER}/lifecycle.parquet'").df()
    ws = con.sql(f"""SELECT host, to_timestamp(t) AS ts, phase_txt, cursor, head, lag FROM '{RAW}/watch/watch_samples.parquet'""").df()
    elb = con.sql(f"SELECT host, ts, block FROM '{DER}/el_blocks.parquet'").df()
    end_all = pd.Timestamp.now(tz="UTC")

    # nethermind-1's attempts on f4e29519: one per process start between its importer deploy and 713979e7.
    runs = list(RUNS)
    starts = lc[(lc.host == "nethermind-1") & (lc.kind == "start") & (lc.ts >= T("10-22 10:00:00")) & (lc.ts < T("10-23 11:55:00"))].ts.sort_values().tolist()
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else T("10-23 11:55:50")
        runs.append((f"nethermind-1/import-f4e2-a{i + 1}", "nethermind-1", "import", "f4e29519",
                     s.strftime("%m-%d %H:%M:%S"), e.strftime("%m-%d %H:%M:%S"), "failed", "attempt died; import restarts from scratch"))

    rows, phases = [], []
    for run_id, host, method, build, deploy, wend, status, note in runs:
        client = host.rsplit("-", 1)[0]
        t0, t1 = T(deploy), (T(wend) if wend else end_all)
        m = mk[(mk.host == host) & (mk.ts >= t0 - pd.Timedelta(seconds=30)) & (mk.ts < t1)]

        def pick(spec):
            if spec == "deploy":
                return t0
            how, marker, *flt = spec
            s = m[m.marker == marker]
            if flt:
                s = s[s.text.str.contains(flt[0], regex=False)]
            if s.empty:
                return None
            return s.ts.min() if how == "first" else s.ts.max()

        r = {"run_id": run_id, "host": host, "client": client, "method": method, "build": build, "status": status,
             "note": note, "deploy": t0, "window_end": t1}
        if method in ("import", "self"):
            if client == "nethermind" and method == "import":
                # 713979e7 logs no preimage bar: the import starts at its first progress line of either kind.
                firsts = [t for t in (pick(("first", "preimages_progress")), pick(("first", "import_progress"))) if t is not None]
                r["start"] = min(firsts) if firsts else None
                m = pd.concat([m, pd.DataFrame([{"host": host, "ts": r["start"], "marker": "nm_start", "text": "", "block": None}])]) if firsts else m
                cs = pick(("first", "check2_done")) or pick(("first", "rebuild_done"))
                m = pd.concat([m, pd.DataFrame([{"host": host, "ts": cs, "marker": "nm_commit", "text": "", "block": None}])]) if cs is not None else m
            else:
                r["start"] = pick(("first", START[method][client]))
            r["complete"] = pick(("first", DONE[method][client]))
            anchor = ANCHOR_N
            if method == "self":
                sm = m[m.marker == START["self"][client]]
                if not sm.empty:
                    txt = sm.iloc[0].text
                    import re
                    g = re.search(r"anchor=([\d,]+)|BlockNumber = (\d+)|at block (\d+)", txt)
                    anchor = int(next(x for x in g.groups() if x).replace(",", "")) if g else None
            r["anchor_block"] = anchor
            # Synced: Besu logs it; the others only via the watch's debug_migrationProgress samples.
            synced, head_at_synced, how = None, None, None
            if r["complete"] is not None:
                if client == "besu":
                    cu = m[(m.marker == "caught_up") & (m.ts >= r["complete"])]
                    if not cu.empty:
                        synced, head_at_synced, how = cu.ts.min(), int(cu.iloc[0].block or 0) or None, "log: caught up with the head"
                if synced is None:
                    w = ws[(ws.host == host) & (ws.ts >= r["complete"]) & (ws.ts < t1) & (ws["cursor"].notna()) & (ws["head"].notna())
                           & (ws["head"] - ws["cursor"] <= 2) & (ws["cursor"] > (anchor or 0))]
                    if not w.empty:
                        synced, head_at_synced = w.ts.min(), int(w.iloc[0]["head"])
                        prev = ws[(ws.host == host) & (ws.ts < synced) & (ws.ts >= r["complete"])]
                        gap = (synced - (prev.ts.max() if not prev.empty else r["complete"])).total_seconds()
                        how = "watch sample" + (f" (previous sample {gap / 60:.0f} min earlier: upper bound)" if gap > 300 else "")
            if run_id in SYNCED_OVERRIDE:
                when, why = SYNCED_OVERRIDE[run_id]
                synced, how = T(when), why
                hb = elb[(elb.host == host) & (elb.ts <= synced)]
                head_at_synced = int(hb.block.max()) if not hb.empty else head_at_synced
            r.update({"synced": synced, "head_at_synced": head_at_synced, "synced_source": how})
            for ph, (a, b) in RULES[(client, method)].items():
                pa_, pb_ = pick(a), pick(b)
                if pa_ is not None and pb_ is not None and pb_ >= pa_:
                    phases.append({"run_id": run_id, "phase": ph, "start": pa_, "end": pb_, "dur_s": (pb_ - pa_).total_seconds()})
            if r["complete"] is not None and synced is not None:
                phases.append({"run_id": run_id, "phase": "U7 replay", "start": r["complete"], "end": synced,
                               "dur_s": (synced - r["complete"]).total_seconds()})
        rows.append(r)

    runs_df = pd.DataFrame(rows)
    for c, (a, b) in {"t_wait_s": ("deploy", "start"), "t_core_s": ("start", "complete"),
                      "t_replay_s": ("complete", "synced"), "t_total_s": ("start", "synced"),
                      "t_deploy_to_synced_s": ("deploy", "synced")}.items():
        runs_df[c] = (runs_df[b] - runs_df[a]).dt.total_seconds()
    runs_df["replay_blocks"] = runs_df.head_at_synced - runs_df.anchor_block
    runs_df["replay_blk_per_s"] = runs_df.replay_blocks / runs_df.t_replay_s
    runs_df.to_parquet(DER / "runs.parquet", index=False)
    pd.DataFrame(phases).to_parquet(DER / "phases.parquet", index=False)

    # Failures.
    fails = []
    oom = con.sql(f"""WITH s AS (SELECT split_part(filename, '/', -2) AS host, ts, value FROM read_parquet('{RAW}/prom/node/*/*.parquet', filename=true)
                                 WHERE metric = 'node_vmstat_oom_kill'),
                           d AS (SELECT host, ts, value - lag(value) OVER (PARTITION BY host ORDER BY ts) AS inc FROM s)
                      SELECT host, to_timestamp(ts) AS ts, inc FROM d WHERE inc > 0 ORDER BY host, ts""").df()
    for _, x in oom.iterrows():
        fails.append({"host": x.host, "ts": x.ts, "kind": "oom_kill", "count": int(x.inc), "detail": "host node_vmstat_oom_kill increment (30 s resolution)"})
    lcs = lc.sort_values(["host", "ts"])
    for host, g in lcs.groupby("host"):
        prev = None
        for _, x in g.iterrows():
            if x.kind == "start" and prev is not None and prev.kind == "start":
                fails.append({"host": host, "ts": x.ts, "kind": "crash_restart", "count": 1,
                              "detail": f"start with no clean stop since {prev.ts:%m-%d %H:%M:%S} ({x.build})"})
            prev = x
    for marker, kind in [("jvm_oom", "jvm_heap_oom"), ("no_space", "disk_full"), ("replay_stalled", "replay_stall"),
                         ("segv", "segv"), ("import_failed", "import_failed"), ("root_mismatch", "root_mismatch"),
                         ("anchor_failed", "anchor_failed")]:
        s = mk[mk.marker == marker]
        for host, g in s.groupby("host"):
            # one event per burst (lines within 5 min belong together)
            last = None
            for _, x in g.sort_values("ts").iterrows():
                if last is None or (x.ts - last).total_seconds() > 300:
                    fails.append({"host": host, "ts": x.ts, "kind": kind, "count": 1, "detail": x.text[:240]})
                last = x.ts
    st = mk[mk.marker == "shadow_stopped"]
    for host, g in st.groupby("host"):
        fails.append({"host": host, "ts": g.ts.min(), "kind": "shadow_stopped", "count": len(g),
                      "detail": f"{len(g)} lines {g.ts.min():%m-%d %H:%M} → {g.ts.max():%m-%d %H:%M}"})
    pd.DataFrame(fails).sort_values(["host", "ts"]).to_parquet(DER / "failures.parquet", index=False)
    log(f"runs {len(runs_df)}, phases {len(phases)}, failures {len(fails)}")
    show = runs_df[runs_df.status != "failed"][["run_id", "start", "complete", "synced", "t_core_s", "t_replay_s", "t_total_s", "replay_blocks", "synced_source"]]
    with pd.option_context("display.width", 250, "display.max_columns", 20):
        print(show.to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
