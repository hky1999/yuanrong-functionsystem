#!/usr/bin/env python3
"""Read-only deployment precheck for the managed/anonymous checkpoint
sub-root upgrade (empty-old-cache precondition). FAIL-CLOSED.

Every check is MANDATORY: any probe error, timeout, unparseable output,
context mismatch, or incomplete node coverage makes the script exit
nonzero. Probes use the VERIFIED collector inputs (the committed
collect_stats.py / sdk_lifecycle.py at /data/work/plan-a/<case>) for the
real sandbox List, authoritative budget inventory and proxy identity —
a pod list is NEVER treated as a sandbox list.

Ineligible (nonzero exit) when:
  1) any node's checkpoint base holds legacy entries besides anonymous/
     managed/ dotfiles, or the listing itself cannot be read;
  2) any node's real sandbox List is nonzero, unknown, or any node is
     not covered;
  3) the authoritative budget inventory shows entries or is unknown;
  4) the persistent snapshot store (etcd /yr/snapshots/v1/) holds any
     record or the probe fails;
  5) kubectl context is not the expected kind-akernel-bm1.

Never deletes, moves, or writes anything.
"""
import argparse
import json
import subprocess
import sys

EXPECTED_CONTEXT = "kind-akernel-bm1"


def fail(report, reason):
    report["eligible"] = False
    report["reasons"].append(reason)


def run(cmd, timeout):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"rc": r.returncode, "out": r.stdout, "err": r.stderr}
    except subprocess.TimeoutExpired:
        return {"rc": None, "out": "", "err": "timeout"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", action="append", required=True)
    ap.add_argument("--checkpoint-base", default="/home/akernel/sandboxd/root/checkpoints")
    ap.add_argument("--collector-dir", required=True,
                    help="dir with the VERIFIED collect_stats.py + sdk_lifecycle.py")
    ap.add_argument("--etcd-pod", required=True)
    ap.add_argument("--etcd-endpoint", required=True)
    ap.add_argument("--venv-python", default="/data/venv/bin/python3")
    args = ap.parse_args()

    report = {"eligible": True, "reasons": [], "nodes": {}, "snapshot_store": None}

    # 0) context check (fail-closed)
    ctx = run(["kubectl", "config", "current-context"], 30)
    if ctx["rc"] != 0 or ctx["out"].strip() != EXPECTED_CONTEXT:
        fail(report, f"kubectl context {ctx['out'].strip()!r} != {EXPECTED_CONTEXT!r} (rc={ctx['rc']})")

    # 1) legacy directory entries per node (probe errors are failures)
    for node in args.node:
        nr = {}
        ls = run(["kubectl", "-n", "akernel", "exec", node, "--", "sh", "-c",
                  f"ls -A {args.checkpoint_base}"], 60)
        nr["ls_rc"] = ls["rc"]
        if ls["rc"] != 0:
            fail(report, f"{node}: checkpoint base listing rc={ls['rc']}: {ls['err'][:120]}")
            nr["base_entries"] = None
        else:
            entries = [e for e in ls["out"].split() if e]
            legacy = [e for e in entries if e not in ("anonymous", "managed")]
            nr["base_entries"] = entries
            nr["legacy_entries"] = legacy
            if legacy:
                fail(report, f"{node}: legacy entries {legacy} — migration is a separate gap")
        report["nodes"][node] = nr

    # 2) ACTUAL node set from the cluster: every akernel-node pod must be
    # covered by --node (a missing node makes the check incomplete)
    pods = run(["kubectl", "-n", "akernel", "get", "pods", "-o", "name",
                "-l", "app=akernel-node"], 60)
    report["node_pods_rc"] = pods["rc"]
    actual_nodes = []
    if pods["rc"] != 0:
        fail(report, f"node pod listing rc={pods['rc']}: {pods['err'][:120]}")
    else:
        actual_nodes = [l.split("/", 1)[1] for l in pods["out"].splitlines()
                        if l.startswith("pod/akernel-node-")]
        if not actual_nodes:
            fail(report, "no akernel-node pods discovered — coverage unverifiable")
        missing = [n for n in actual_nodes if n not in args.node]
        if missing:
            fail(report, f"node coverage incomplete: {missing} not passed via --node")

    # 3) real sandbox List + PER-NODE authoritative budget via the VERIFIED
    # collectors. list_by_instance with an empty instance selector only
    # matches empty-label entries — NOT a node-wide list — so the probe
    # drives grpcurl's SandboxService/List with NO selector (the collector's
    # transport seam) and counts every returned sandbox.
    probe_script = """
import argparse, json, sys
ap = argparse.ArgumentParser()
ap.add_argument("--collector-dir", required=True)
ap.add_argument("--nodes", required=True)
a = ap.parse_args()
sys.path.insert(0, a.collector_dir)
import collect_stats, sdk_lifecycle as drv, json as _j
out = {"sandboxes": {}, "budget": {}, "list_raw_counts": {}}
for n in a.nodes.split(","):
    # node-wide List: empty JSON selector object => server returns all
    resp = collect_stats.grpc_call(n, "akernel", "runtime.v1.SandboxService/List", {}, 15)
    sandboxes = resp.get("sandboxes", []) if isinstance(resp, dict) else None
    if sandboxes is None:
        out["sandboxes"][n] = None
    else:
        out["sandboxes"][n] = len(sandboxes)
    # per-node authoritative budget inventory
    probe = drv.proxy_inventory_snapshot(n, "akernel", 120, 30)
    auth = drv.authoritative_snapshot(probe)
    snap = auth.get("snapshot", {}) if isinstance(auth, dict) else {}
    out["budget"][n] = {"instance_count": snap.get("instance_count"),
                        "entries": snap.get("entries", [])}
print(_j.dumps(out))
"""
    probe = run([args.venv_python, "-c", probe_script, "--collector-dir", args.collector_dir,
                 "--nodes", ",".join(args.node)], 240)
    report["collector_rc"] = probe["rc"]
    if probe["rc"] != 0:
        fail(report, f"collector probe rc={probe['rc']}: {probe['err'][:200]}")
    else:
        try:
            data = json.loads(probe["out"])
        except ValueError:
            fail(report, "collector output unparseable")
            data = {}
        report["collector"] = data
        for node in args.node:
            count = data.get("sandboxes", {}).get(node)
            if count is None:
                fail(report, f"{node}: sandbox count unknown (node not covered)")
            elif count != 0:
                fail(report, f"{node}: {count} live sandbox(es) — parked/live records must be empty")
        for node in args.node:
            b = (data.get("budget") or {}).get(node)
            if b is None or b.get("instance_count") is None or b.get("entries") is None:
                fail(report, f"{node}: authoritative budget inventory unknown")
            elif b.get("instance_count") != 0 or b.get("entries"):
                fail(report, f"{node}: budget not empty: {b}")

    # 4) persistent snapshot store
    etcd = run(["kubectl", "-n", "akernel", "exec", args.etcd_pod, "-c", "etcd", "--",
                "/opt/bitnami/etcd/bin/etcdctl", "--endpoints", args.etcd_endpoint,
                "get", "/yr/snapshots/v1/", "--prefix", "--keys-only"], 60)
    report["snapshot_store"] = {"rc": etcd["rc"]}
    if etcd["rc"] != 0:
        fail(report, f"etcd probe rc={etcd['rc']}: {etcd['err'][:120]}")
    else:
        # --keys-only output must be clean full-key lines; trailing stats
        # lines or garbage mean the response format is unusable => unknown
        lines = [l for l in etcd["out"].splitlines() if l.strip()]
        malformed = [l for l in lines if not l.startswith("/")]
        keys = [l for l in lines if l.startswith("/yr/snapshots")]
        report["snapshot_store"]["keys"] = keys
        if malformed:
            fail(report, f"etcd response format unusable ({len(malformed)} malformed lines)")
        if keys:
            fail(report, f"snapshot store holds {len(keys)} persistent record(s)")

    print(json.dumps(report, ensure_ascii=False, indent=1))
    return 0 if report["eligible"] else 1


if __name__ == "__main__":
    sys.exit(main())
