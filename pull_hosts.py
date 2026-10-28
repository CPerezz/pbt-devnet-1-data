"""C13: per-host configuration snapshot over read-only ssh (the watch's ssh_config).

Only reads: docker inspect / ps, /proc and lsblk. Environment variables that look
like secrets are dropped before anything is written.
"""
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

from common import HOSTS, RAW, log, redact

OUT = RAW / "hosts"
SSH = ["ssh", "-F", str((RAW.parent.parent.parent / "ssh_config").resolve()), "-o", "ControlMaster=no",
       "-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]
CMD = r"""set -e
echo '@@inspect'; docker inspect execution beacon validator 2>/dev/null || true
echo '@@ps'; docker ps -a --no-trunc --format '{{json .}}'
echo '@@vmstat'; grep -E '^(oom_kill|pgmajfault) ' /proc/vmstat
echo '@@uptime'; uptime -s
echo '@@cpu'; nproc
echo '@@mem'; grep -E '^(MemTotal|SwapTotal):' /proc/meminfo
echo '@@df'; df -B1 --output=target,source,fstype,size,used,avail / /data 2>/dev/null || df -B1 /
echo '@@lsblk'; lsblk -b -d -o NAME,SIZE,ROTA,TYPE,MODEL 2>/dev/null || true
echo '@@kernel'; uname -r
"""
SECRET = re.compile(r"(SECRET|TOKEN|PASSWORD|PASSWD|PRIVATE|KEY)=", re.I)


def snapshot(host: str) -> dict:
    r = subprocess.run(SSH + [host, "bash", "-s"], input=CMD, capture_output=True, text=True, timeout=120)
    parts, cur = {}, None
    for line in r.stdout.splitlines():
        if line.startswith("@@"):
            cur = line[2:]
            parts[cur] = []
        elif cur:
            parts[cur].append(line)
    inspect = json.loads("\n".join(parts.get("inspect", [])) or "[]")
    for c in inspect:
        env = c.get("Config", {}).get("Env") or []
        c["Config"]["Env"] = [e for e in env if not SECRET.search(e)]
    return {"host": host, "inspect": inspect, "ps": [json.loads(x) for x in parts.get("ps", []) if x.strip()],
            **{k: parts.get(k, []) for k in ("vmstat", "uptime", "cpu", "mem", "df", "lsblk", "kernel")},
            "stderr": r.stderr[-500:]}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(9) as ex:
        for snap in ex.map(snapshot, HOSTS):
            (OUT / f"{snap['host']}.json").write_text(redact(json.dumps(snap, indent=1)))
            ins = {c["Name"].strip("/"): c.get("Config", {}).get("Image", "") for c in snap["inspect"]}
            log(f"{snap['host']}: {len(snap['inspect'])} inspected {ins} | {len(snap['ps'])} containers | {snap['vmstat']}")


if __name__ == "__main__":
    sys.exit(main())
