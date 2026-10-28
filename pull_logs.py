"""S1: container logs from ClickHouse external.otel_logs (via panda).

Kept (discard list X2, X4, X5 applied):
- execution: every line except spamoor/tx-submission and peer chatter (EL_NOISE),
  plus hourly counts of the dropped lines as a load indicator;
- beacon / validator: CRIT, ERRO, WARN lines and lines about the execution engine;
- ad-hoc operator containers (restores, builds, profiling): every line.
Chunks already on disk are skipped; each chunk's row count is checked against a
count() of the same filter.
"""
import datetime as dt
import sys
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from common import EL_NOISE, HOSTS, NET, RAW, WIN_START, ch, chunks, covers, log, redact, ts, win_end, write_parquet

OUT = RAW / "logs"
INFRA = ("execution", "beacon", "validator", "xatu-sentry", "cadvisor", "dns_server", "cert_issuer",
         "eth-inventory-web", "nginx-proxy", "nginx-proxy-gen", "nginx-proxy-cert-linker",
         "nginx-proxy-cert-loader", "vector", "node_exporter", "prometheus", "snooper-engine",
         "ethereum-metrics-exporter", "cadvisor-test")
CL_KEEP = r" (CRIT|ERRO|ERROR|WARN) |(?i:execution (engine|endpoint|node|layer))"
BASE = f"Timestamp >= '{{t0}}' AND Timestamp < '{{t1}}' AND ResourceAttributes['network']='{NET}' AND ResourceAttributes['host.name']='lighthouse-{{host}}'"


def to_table(cols, rows) -> pa.Table:
    # query-raw can drop trailing empty fields on a row: pad them back.
    data = {c: [(r[i] if i < len(r) else "") for r in rows] for i, c in enumerate(cols)}
    if "body" in data:
        data["body"] = [redact(b) for b in data["body"]]
    t = np.array([s.replace(" ", "T") for s in data.pop("ts")], dtype="datetime64[ns]")
    return pa.table({"ts": pa.array(t, pa.timestamp("ns", tz="UTC")), **{k: pa.array(v, pa.string()) for k, v in data.items()}})


def pull_chunk(kind: str, host: str, t0, t1, where: str, select: str):
    path = OUT / kind / host / f"{t0:%Y%m%d%H}.parquet"
    # Chunks that end within 2 h of now may still be filling at the source: always refetch them.
    fresh = t1 > dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    if covers(path, t1) and not fresh:
        return path, pq.read_metadata(path).num_rows, True
    base = BASE.format(t0=ts(t0), t1=ts(t1), host=host)
    sql = f"SELECT {select} FROM external.otel_logs WHERE {base} AND {where} ORDER BY Timestamp"
    cols, rows = ch(sql)
    write_parquet(to_table(cols, rows), path, {"source": "clickhouse-raw external.otel_logs", "sql": sql,
                                               "t0": ts(t0), "t1": ts(t1), "host": host})
    return path, len(rows), False


def source_counts(kind: str, host: str, end, where: str, hours: int) -> dict:
    base = BASE.format(t0=ts(WIN_START), t1=ts(end), host=host)
    _, rows = ch(f"SELECT intDiv(toUnixTimestamp(Timestamp) - {int(WIN_START.timestamp())}, {hours * 3600}) AS b, count() "
                 f"FROM external.otel_logs WHERE {base} AND {where} GROUP BY b")
    return {int(b): int(n) for b, n in rows}


SPECS = {
    # kind: (where, select, chunk hours)
    "el": (f"ResourceAttributes['container.name']='execution' AND NOT match(Body, '{EL_NOISE}')",
           "Timestamp AS ts, Body AS body", 6),
    "cl": (f"ResourceAttributes['container.name'] IN ('beacon','validator') AND match(Body, '{CL_KEEP}')",
           "Timestamp AS ts, ResourceAttributes['container.name'] AS container, Body AS body", 24),
    "adhoc": (f"ResourceAttributes['container.name'] NOT IN {INFRA}",
              "Timestamp AS ts, ResourceAttributes['container.name'] AS container, "
              "ResourceAttributes['container.image.name'] AS image, Body AS body", 24),
}


def pull(kind: str, end):
    where, select, hours = SPECS[kind]
    jobs = [(host, t0, t1) for host in HOSTS for t0, t1 in chunks(WIN_START, end, hours)]
    bad = []
    with ThreadPoolExecutor(6) as ex:
        counts = dict(zip(HOSTS, ex.map(lambda h: source_counts(kind, h, end, where, hours), HOSTS)))
        for (host, t0, t1), (path, n, cached) in zip(jobs, ex.map(lambda j: pull_chunk(kind, *j, where, select), jobs)):
            want = counts[host].get(int((t0.timestamp() - WIN_START.timestamp()) // (hours * 3600)), 0)
            if n != want:
                bad.append((host, f"{t0:%m-%d %H}", n, want))
    log(f"{kind}: {len(jobs)} chunks, mismatches {bad[:10]}")
    return bad


def noise_hourly(end):
    path = OUT / "el_noise_hourly.parquet"
    cols, rows = ch(f"SELECT ResourceAttributes['host.name'] AS host, toStartOfHour(Timestamp) AS hour, count() AS lines "
                    f"FROM external.otel_logs WHERE Timestamp >= '{ts(WIN_START)}' AND Timestamp < '{ts(end)}' "
                    f"AND ResourceAttributes['network']='{NET}' AND ResourceAttributes['container.name']='execution' "
                    f"AND match(Body, '{EL_NOISE}') GROUP BY host, hour ORDER BY host, hour")
    write_parquet(pa.table({"host": [r[0].replace("lighthouse-", "") for r in rows],
                            "hour": pa.array(np.array([r[1].replace(" ", "T") for r in rows], dtype="datetime64[s]"), pa.timestamp("s", tz="UTC")),
                            "lines": [int(r[2]) for r in rows]}), path, {"source": "otel_logs noise counts"})


def main():
    end = win_end()
    log(f"window {WIN_START:%m-%d %H:%M} → {end:%m-%d %H:%M}")
    kinds = sys.argv[1:] or ["el", "cl", "adhoc", "noise"]
    bad = []
    for k in kinds:
        if k == "noise":
            noise_hourly(end)
        else:
            bad += pull(k, end)
    if bad:
        log(f"MISMATCHES: {bad}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
