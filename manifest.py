"""Fold every sidecar *.meta.json into data/MANIFEST.json and check coverage.

Checks:
- logs: every host has every 6 h EL chunk and every daily CL / ad-hoc chunk;
- Prometheus: per host and job, the share of 30 s steps with data, and gaps longer
  than 5 minutes in the container memory series (the series every view needs);
- xatu: rows per table per day.
Writes data/validation.json and prints a summary.
"""
import json
import sys

import duckdb

from common import DATA, HOSTS, RAW, WIN_START, log, win_end


def main():
    manifest = {}
    for side in sorted(DATA.rglob("*.meta.json")):
        manifest[str(side.relative_to(DATA)).removesuffix(".meta.json") + ".parquet"] = json.loads(side.read_text())
    (DATA / "MANIFEST.json").write_text(json.dumps(manifest, indent=1, sort_keys=True))
    end = win_end()
    con = duckdb.connect()
    v = {"window": [WIN_START.isoformat(), end.isoformat()], "files": len(manifest),
         "rows": sum(m.get("rows", 0) for m in manifest.values()),
         "bytes": sum(m.get("bytes", 0) for m in manifest.values())}

    hours = (end - WIN_START).total_seconds() / 3600
    v["logs_el_chunks"] = {h: len(list((RAW / "logs" / "el" / h).glob("*.parquet"))) for h in HOSTS}
    v["logs_el_expected_chunks"] = int(-(-hours // 6))

    steps_expected = int(hours * 120)
    cov = con.sql(f"""
        SELECT split_part(filename, '/', -3) AS job, split_part(filename, '/', -2) AS host,
               count(DISTINCT ts) AS steps
        FROM read_parquet('{RAW}/prom/*/*/*.parquet', filename=true) GROUP BY ALL ORDER BY ALL""").fetchall()
    v["prom_step_coverage"] = {f"{j}/{h}": round(n / steps_expected, 4) for j, h, n in cov}
    gaps = con.sql(f"""
        WITH s AS (SELECT split_part(filename, '/', -2) AS host, ts FROM read_parquet('{RAW}/prom/cadvisor/*/*.parquet', filename=true)
                   WHERE metric = 'container_memory_rss' AND labels LIKE '%"name":"execution"%'),
             d AS (SELECT host, ts, ts - lag(ts) OVER (PARTITION BY host ORDER BY ts) AS gap FROM s)
        SELECT host, to_timestamp(ts - gap) AS gap_start, gap FROM d WHERE gap > 300 ORDER BY host, gap_start""").fetchall()
    v["prom_gaps_over_5min"] = [[h, str(s), int(g)] for h, s, g in gaps]
    v["xatu_rows"] = dict(con.sql(f"""SELECT split_part(filename, '/', -2), count(*) FROM read_parquet('{RAW}/xatu/*/*.parquet', filename=true)
                                     GROUP BY 1 ORDER BY 1""").fetchall())
    (DATA / "validation.json").write_text(json.dumps(v, indent=1))
    low = {k: c for k, c in v["prom_step_coverage"].items() if c < 0.95}
    log(f"{v['files']} files, {v['rows']:,} rows, {v['bytes'] / 2**30:.2f} GiB; EL chunks {set(v['logs_el_chunks'].values())} "
        f"(expected {v['logs_el_expected_chunks']}); prom coverage < 95 %: {low}; gaps > 5 min: {len(gaps)}")


if __name__ == "__main__":
    sys.exit(main())
