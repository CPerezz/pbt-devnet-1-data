"""S8: our PBT mainnet benchmark rounds (~/dev/pbt-bench-export) as Parquet.

Mainnet anchor 26,142,444 (3.02 B leaves), bench box: 8 cores, 32 GiB shared with the
CL, a dedicated disk. One row per phase edge from each round's phases.jsonl.
"""
import glob
import json
import sys
from pathlib import Path

import pyarrow as pa

from common import RAW, log, write_parquet

BENCH = Path.home() / "dev" / "pbt-bench-export"


def main():
    rows = []
    for f in sorted(glob.glob(str(BENCH / "*" / "*" / "phases.jsonl"))):
        rnd, client = Path(f).parts[-3], Path(f).parts[-2]
        for line in open(f):
            if line.strip():
                e = json.loads(line)
                rows.append({"round": rnd, "client": client, "phase": e.get("phase"), "edge": e.get("edge"),
                             "unix": float(e.get("unix", 0)), "note": e.get("note", "")})
    write_parquet(pa.Table.from_pylist(rows), RAW / "bench" / "phases.parquet", {"source": str(BENCH)})
    meta = json.loads((BENCH / "round2" / "pair-meta.json").read_text())
    (RAW / "bench" / "pair-meta.json").write_text(json.dumps(meta, indent=1))
    log(f"bench: {len(rows)} phase edges")


if __name__ == "__main__":
    sys.exit(main())
