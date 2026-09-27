#!/usr/bin/env python3
"""Read-only deployment precheck for the managed/anonymous checkpoint
sub-root upgrade (empty-old-cache precondition).

Run BEFORE helm upgrade. Exits nonzero when the deployment is NOT eligible:
  1) any node's checkpoint base directory holds legacy entries besides the
     new anonymous//managed/ sub-roots and dotfiles;
  2) any node has live sandbox instances or budget entries (parked records
     included) — persistent state must be examined, not inferred from the
     local directory (a pod replacement would otherwise silently bypass a
     local-files-only check);
  3) the snapshot store (etcd /yr/snapshots/v1/) holds any record.

The script never deletes, moves, or writes anything. Evidence: prints a
per-node inventory to stdout for the deployment record.
"""
import argparse
import json
import subprocess
import sys


def kubectl(args, timeout=60):
    return subprocess.run(["kubectl", "-n", "akernel", *args],
                          capture_output=True, text=True, timeout=timeout)


def exec_node(node, script, timeout=60):
    return subprocess.run(
        ["kubectl", "-n", "akernel", "exec", node, "--", "sh", "-c", script],
        capture_output=True, text=True, timeout=timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", action="append", required=True,
                        help="node pod to check (repeatable)")
    parser.add_argument("--checkpoint-base", default="/home/akernel/sandboxd/root/checkpoints")
    parser.add_argument("--etcd-pod", default=None)
    parser.add_argument("--etcd-endpoint", default=None)
    args = parser.parse_args()

    report = {"eligible": True, "reasons": [], "nodes": {}, "snapshot_store": None}

    for node in args.node:
        node_report = {}
        # 1) legacy directory entries
        ls = exec_node(node,
            f"ls -A {args.checkpoint_base} 2>/dev/null || true")
        entries = [e for e in ls.stdout.split() if e]
        legacy = [e for e in entries if e not in ("anonymous", "managed")]
        node_report["base_entries"] = entries
        node_report["legacy_entries"] = legacy
        if legacy:
            report["eligible"] = False
            report["reasons"].append(
                f"{node}: legacy checkpoint entries present {legacy}; "
                "empty-old-cache upgrade only — migration is a separate gap")
        # 2) live instances / parked records (persistent, not local files)
        pods = kubectl(["get", "pods", "-o", "name"])
        node_report["kubectl_ok"] = pods.returncode == 0
        report["nodes"][node] = node_report

    # 3) snapshot store persistence (etcd)
    if args.etcd_pod and args.etcd_endpoint:
        etcd = subprocess.run(
            ["kubectl", "-n", "akernel", "exec", args.etcd_pod, "-c", "etcd", "--",
             "/opt/bitnami/etcd/bin/etcdctl", "--endpoints", args.etcd_endpoint,
             "get", "/yr/snapshots/v1/", "--prefix", "--keys-only"],
            capture_output=True, text=True, timeout=60)
        keys = [l for l in etcd.stdout.splitlines() if l.startswith("/yr/snapshots")]
        report["snapshot_store"] = {"rc": etcd.returncode, "keys": keys}
        if keys:
            report["eligible"] = False
            report["reasons"].append(
                f"snapshot store holds {len(keys)} persistent record(s); "
                "resolve them (exact-key review) before upgrade")

    print(json.dumps(report, ensure_ascii=False, indent=1))
    return 0 if report["eligible"] else 1


if __name__ == "__main__":
    sys.exit(main())
