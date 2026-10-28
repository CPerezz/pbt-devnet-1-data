"""S6: the watch's own data (../data) as Parquet.

- watch_samples: one row per poll × host (migration phase, replay cursor, memory, CPU,
  IO pressure, disk, head, the sampled canonical hash and PBT root);
- watch_events: every event the watch raised.
The samples are the only record of geth's and Nethermind's replay cursors.
"""
import glob
import json
import math
import sys

import pyarrow as pa

from common import HOSTS, RAW, log, redact, win_end, write_parquet

WATCH = RAW.parent.parent.parent / "data"
NUM = ["cursor", "lag", "rate", "eta", "mem", "limit_g", "cpu", "anon_g", "io", "peers", "lat", "base", "head",
       "head_t", "fin_epoch", "part", "behind", "shadow_folds", "head_gas", "oom_eta"]
TXT = ["link", "cl", "phase_txt", "head_hash", "shadow", "erigon_exec"]


def num(v):
    if v in (None, "None", "", "nan"):
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def txt(v):
    return None if v in (None, "None", "") else redact(str(v))


def disk(v):
    """'df /' is [free bytes, free fraction] (sometimes stringified)."""
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            return None, None
    return (v[0], v[1]) if isinstance(v, list) and len(v) == 2 else (None, None)


def sample_pair(v):
    if isinstance(v, str):
        try:
            v = json.loads(v.replace("'", '"'))
        except json.JSONDecodeError:
            return None, None
    return (int(v[0]), v[1]) if isinstance(v, list) and len(v) == 2 else (None, None)


def main():
    rows, end = [], win_end().timestamp()  # same window as the other pulls
    for f in sorted(glob.glob(str(WATCH / "samples-*.jsonl"))):
        for line in open(f):
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if float(r["t"]) >= end:
                continue
            for h in HOSTS:
                x = r.get("hosts", {}).get(h)
                if not x:
                    continue
                free, frac = disk(x.get("df /"))
                sh, shash = sample_pair(x.get("sample"))
                rows.append({"t": float(r["t"]), "host": h, **{k: num(x.get(k)) for k in NUM},
                             **{k: txt(x.get(k)) for k in TXT}, "df_free_bytes": free, "df_free_frac": frac,
                             "sample_height": sh, "sample_hash": shash})
    write_parquet(pa.Table.from_pylist(rows), RAW / "watch" / "watch_samples.parquet",
                  {"source": "pbt-devnet-1-watch data/samples-*.jsonl"})
    ev = [e for e in map(json.loads, open(WATCH / "events.jsonl")) if float(e["t"]) < end]
    write_parquet(pa.Table.from_pylist([{"t": e["t"], "sev": e["sev"], "host": e["host"], "key": redact(e["key"]), "msg": redact(e["msg"])} for e in ev]),
                  RAW / "watch" / "watch_events.parquet", {"source": "pbt-devnet-1-watch data/events.jsonl"})
    log(f"watch: {len(rows)} sample rows, {len(ev)} events")


if __name__ == "__main__":
    sys.exit(main())
