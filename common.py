"""Shared constants and helpers for the pbt-devnet-1 report pipeline.

Every pull goes through the local `panda` CLI (ClickHouse, Prometheus, ethnode).
Raw data lands in data/raw as zstd Parquet. Each file gets a sidecar `<name>.meta.json`
(source query, row count, time bounds); manifest.py folds them into MANIFEST.json,
so parallel pulls never race on one file and a pull can resume by skipping files
that already exist.
"""
import datetime as dt
import json
import re
import os
import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW = DATA / "raw"
DER = DATA / "derived"

NET = "glamsterdam-pbt-1"
HOSTS = ["geth-1", "geth-2", "geth-3", "nethermind-1", "nethermind-2",
         "besu-1", "besu-2", "erigon-1", "erigon-2"]
CLIENT = {h: h.rsplit("-", 1)[0] for h in HOSTS}
CAP_GIB = {"geth-1": 16, "geth-2": 32, "geth-3": 16, "nethermind-1": 16, "nethermind-2": 32,
           "besu-1": 16, "besu-2": 32, "erigon-1": 32, "erigon-2": 16}
ANCHOR_N = 24_453_558
FORK_TS = 1_793_264_400            # 2026-10-29 09:00:00 UTC
FINAL_TS = FORK_TS + 48 * 3600     # 2026-10-31 09:00:00 UTC
WIN_START = dt.datetime(2026, 10, 22, 8, 0, tzinfo=dt.UTC)

# EL log lines that are spamoor/tx-submission or peer chatter (discard list X4).
EL_NOISE = (r"Submitted transaction|Served eth_sendRawTransaction|eth_sendRawTransaction|"
            r"eth_getTransactionCount|Looking for peers|Announced transaction|already known|"
            r"nonce too high|Error response handling JsonRpc")

# Credentials that leak through client flags and log lines; the dataset is published, so they are
# replaced before anything is written. Values that are file paths (jwt secrets, key files) are kept.
_REDACT = [
    (re.compile(r"https?://[^\s\"'/]+/metrics/[^\s\"',]+"), "<redacted-pushgateway-url>"),   # Nethermind push-gateway URL + token
    (re.compile(r"(--Metrics\.PushGatewayUrl=)[^\s\"',]+"), r"\1<redacted>"),
    (re.compile(r"(--[\w.\-]*(?:ApiKey|Api-Key|Token|Secret|Password|Passwd)[\w.\-]*=)(?!/)[^\s\"',]+", re.I), r"\1<redacted>"),
    (re.compile(r"(\b[a-z][a-z0-9+.\-]*://)[^\s/:@\"']+:[^\s/@\"']+@", re.I), r"\1<redacted>@"),  # user:pass@ in URLs
]


def redact(s):
    if not s:
        return s
    for rx, sub in _REDACT:
        s = rx.sub(sub, s)
    return s


def win_end() -> dt.datetime:
    """End of the data window: now (floored to the hour) until the final cut-off."""
    if os.environ.get("WIN_END"):
        return dt.datetime.fromisoformat(os.environ["WIN_END"]).replace(tzinfo=dt.UTC)
    now = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
    return min(now, dt.datetime.fromtimestamp(FINAL_TS, dt.UTC))


def chunks(start: dt.datetime, end: dt.datetime, hours: int):
    t = start
    while t < end:
        yield t, min(t + dt.timedelta(hours=hours), end)
        t += dt.timedelta(hours=hours)


def ts(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S")


def _run(args, timeout):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def _retry(fn, what: str, retries: int = 4, pause: float = 5):
    err = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:  # transient proxy errors: back off and retry
            err = e
            time.sleep(pause * (attempt + 1))
    raise RuntimeError(f"{what} failed after {retries} tries: {err}")


def ch(sql: str, timeout: int = 900):
    """ClickHouse query through panda; returns (columns, rows)."""
    def go():
        r = _run(["panda", "clickhouse", "query-raw", "clickhouse-raw", sql], timeout)
        try:
            d = json.loads(r.stdout)
        except json.JSONDecodeError:
            raise RuntimeError((r.stdout + r.stderr)[:400])
        return d["columns"], d["rows"]
    return _retry(go, f"clickhouse: {sql[:160]}")


def prom_range(q: str, start: dt.datetime, end: dt.datetime, step: str = "30s", timeout: int = 900):
    """Prometheus range query through panda; returns the result list."""
    def go():
        r = _run(["panda", "-o", "json", "prometheus", "query-range", "devnets", q,
                  "--start", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "--end", end.strftime("%Y-%m-%dT%H:%M:%SZ"), "--step", step], timeout)
        d = json.loads(r.stdout)
        if d.get("status") != "success":
            raise RuntimeError(str(d)[:300])
        return d["data"]["result"]
    return _retry(go, f"prometheus: {q[:160]}")


def rpc(method: str, params: list, instance: str = "lb", timeout: int = 180):
    """Execution JSON-RPC through panda ethnode; returns the decoded result."""
    def go():
        r = _run(["panda", "-o", "json", "ethnode", "exec-rpc", NET, instance, method, json.dumps(params)], timeout)
        try:
            d = json.loads(r.stdout)
        except json.JSONDecodeError:
            raise RuntimeError((r.stdout + r.stderr)[:300])
        if isinstance(d, dict) and d.get("error"):
            raise RuntimeError(str(d["error"])[:300])
        return d["result"] if isinstance(d, dict) and "result" in d else d
    return _retry(go, f"rpc {method}", pause=3)


def write_parquet(table: pa.Table, path: Path, meta: dict | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)
    if meta is not None:
        side = path.with_suffix(".meta.json")
        side.write_text(json.dumps({**meta, "rows": table.num_rows, "bytes": path.stat().st_size,
                                    "written": dt.datetime.now(dt.UTC).isoformat(timespec="seconds")}, indent=1))


def covers(path: Path, t1: dt.datetime) -> bool:
    """A chunk file is reusable only if it was written for a window reaching t1: the previous pull's last chunk
    was cut at that pull's window end and must be refetched once the window grows."""
    side = path.with_suffix(".meta.json")
    return path.exists() and side.exists() and json.loads(side.read_text()).get("t1", "") >= ts(t1)


def log(msg: str):
    print(f"[{dt.datetime.now(dt.UTC):%H:%M:%S}] {msg}", flush=True)
