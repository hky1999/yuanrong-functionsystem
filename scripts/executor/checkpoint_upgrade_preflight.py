#!/usr/bin/env python3
"""Read-only deployment precheck for the managed/anonymous checkpoint
sub-root upgrade (empty-old-cache precondition). FAIL-CLOSED.

Every check is MANDATORY: any probe error, timeout, unparseable output,
context mismatch, unknown/missing budget field, or incomplete node
coverage makes the script exit nonzero. Probes use the VERIFIED collector
inputs (the pinned collect_stats.py / sdk_lifecycle.py whose SHA256 must
be passed on the command line) for the real sandbox List, the
authoritative budget inventory and the proxy process identity — a pod
list is NEVER treated as a sandbox list.

Ineligible (nonzero exit) when:
  1) kubectl context is not the expected kind-akernel-bm1;
  2) the collector inputs' SHA256 do not match the pinned verified bytes;
  3) any node's checkpoint base holds entries besides anonymous/managed,
     or either sanctioned subdirectory is non-empty, or a listing fails;
  4) the ACTUAL node set (canonical daemonset label app=node) is not
     exactly covered by --node;
  5) any node's real sandbox List is nonzero, fails, or is unparseable;
  6) any node's authoritative budget inventory is not status=ok with a
     FRESH snapshot in which EVERY budget/ledger field (instance_count,
     resize/baseline/shrink entry counters, resize outstanding cpu+memory,
     entries[], commitment_ledger.enabled/unit_count/units/
     units_total_bytes) is present, numeric and exactly zero/empty — an
     unknown field name, a disabled ledger, a missing field or a stale
     snapshot is UNKNOWN, not empty;
  7) the function_proxy process identity is unknown on any node, or the
     node proxies do not share ONE exe;
  8) the persistent snapshot store (etcd /yr/snapshots/ prefix, JSON
     output) holds any record, returns keys outside the requested
     prefix, or is not valid etcd JSON.

Never deletes, moves, or writes anything.
"""
import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import time

EXPECTED_CONTEXT = "kind-akernel-bm1"
NODE_POD_LABEL = "app=node"  # canonical selector: charts/core/templates/node/daemonset.yaml
NODE_POD_NAME_PREFIX = "akernel-node-"
SNAPSHOT_ETCD_PREFIX = "/yr/snapshots/"
# ResourceViewActor::LogResourceInventory self-reschedules every 15s, so a
# snapshot older than this bound cannot be attributed to the live process.
FRESHNESS_WINDOW_MS = 120_000

# Exact top-level key set emitted by resource_view_actor.cpp
# LogResourceInventory. A payload carrying any OTHER key is a changed
# contract and must be refused, not re-interpreted.
KNOWN_SNAPSHOT_KEYS = {
    "snapshot_id", "snapshot_unix_ms", "event", "view_actor", "is_local_view",
    "unit_count", "instance_count", "resize_budget_entries", "baseline_entries",
    "shrink_entries", "resize_outstanding_cpu_millicores",
    "resize_outstanding_memory_mb", "commitment_ledger", "node_diagnostics",
    "entries",
}
KNOWN_LEDGER_KEYS = {"enabled", "unit_count", "units", "units_total_bytes"}
# Budget-bearing fields that must be present, numeric and exactly zero.
ZERO_NUMBER_FIELDS = (
    "instance_count", "resize_budget_entries", "baseline_entries",
    "shrink_entries", "resize_outstanding_cpu_millicores",
    "resize_outstanding_memory_mb",
)


def fail(report, reason):
    report["eligible"] = False
    report["reasons"].append(reason)


def run(cmd, timeout):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"rc": r.returncode, "out": r.stdout, "err": r.stderr}
    except subprocess.TimeoutExpired:
        return {"rc": None, "out": "", "err": "timeout"}


def as_number(value):
    """int/float (not bool) or None — mirrors collect_stats.as_int strictness."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def validate_budget_snapshot(node, snap):
    """Return a list of problems; empty list == proven empty and current."""
    problems = []
    if not isinstance(snap, dict):
        return [f"{node}: snapshot payload is not a JSON object"]
    unknown = sorted(set(snap) - KNOWN_SNAPSHOT_KEYS)
    if unknown:
        problems.append(f"{node}: unknown snapshot field(s) {unknown} — emitter contract changed, refusing")
    if snap.get("event") != "resource-inventory-begin":
        problems.append(f"{node}: payload event is not resource-inventory-begin")
    if not str(snap.get("view_actor") or ""):
        problems.append(f"{node}: view_actor missing")
    for field in ZERO_NUMBER_FIELDS:
        value = as_number(snap.get(field))
        if value is None:
            problems.append(f"{node}: {field} missing/non-numeric — unknown, not zero")
        elif value != 0:
            problems.append(f"{node}: {field}={value}")
    entries = snap.get("entries")
    if not isinstance(entries, list) or entries:
        problems.append(f"{node}: entries not proven empty ({len(entries) if isinstance(entries, list) else type(entries).__name__})")
    ledger = snap.get("commitment_ledger")
    if not isinstance(ledger, dict):
        problems.append(f"{node}: commitment_ledger missing — old-ledger state unknown")
    else:
        lunknown = sorted(set(ledger) - KNOWN_LEDGER_KEYS)
        if lunknown:
            problems.append(f"{node}: unknown ledger field(s) {lunknown}")
        if ledger.get("enabled") is not True:
            problems.append(f"{node}: commitment ledger disabled — a zero reading is unknown, not proven-empty")
        total = as_number(ledger.get("units_total_bytes"))
        if total is None:
            problems.append(f"{node}: ledger units_total_bytes missing/non-numeric")
        elif total != 0:
            problems.append(f"{node}: old ledger outstanding {total} bytes")
        units = ledger.get("units")
        if not isinstance(units, list) or units:
            problems.append(f"{node}: ledger units not proven empty")
        count = as_number(ledger.get("unit_count"))
        if count is None:
            problems.append(f"{node}: ledger unit_count missing/non-numeric")
        elif count != 0:
            problems.append(f"{node}: ledger unit_count={count}")
    return problems


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", action="append", required=True)
    ap.add_argument("--checkpoint-base", default="/home/akernel/sandboxd/root/checkpoints")
    ap.add_argument("--collector-dir", required=True,
                    help="dir with the VERIFIED collect_stats.py + sdk_lifecycle.py")
    ap.add_argument("--collect-stats-sha256", required=True,
                    help="pinned SHA256 of the verified collect_stats.py")
    ap.add_argument("--sdk-lifecycle-sha256", required=True,
                    help="pinned SHA256 of the verified sdk_lifecycle.py")
    ap.add_argument("--etcd-pod", required=True)
    ap.add_argument("--etcd-endpoint", required=True)
    ap.add_argument("--venv-python", default="/data/venv/bin/python3")
    args = ap.parse_args()

    start_unix_ms = int(time.time() * 1000)
    report = {"eligible": True, "reasons": [], "started_unix_ms": start_unix_ms,
              "nodes": {}, "node_pods": [], "identity": {}, "snapshot_store": None,
              "collector": None}

    # 1) context check (fail-closed)
    ctx = run(["kubectl", "config", "current-context"], 30)
    if ctx["rc"] != 0 or ctx["out"].strip() != EXPECTED_CONTEXT:
        fail(report, f"kubectl context {ctx['out'].strip()!r} != {EXPECTED_CONTEXT!r} (rc={ctx['rc']})")

    # 2) collector input SHA pinning — the probes below must run the
    # VERIFIED bytes, not whatever happens to sit in the directory
    for name, want in (("collect_stats.py", args.collect_stats_sha256),
                       ("sdk_lifecycle.py", args.sdk_lifecycle_sha256)):
        path = os.path.join(args.collector_dir, name)
        if not os.path.isfile(path):
            fail(report, f"collector input {name} missing at {path}")
        else:
            got = sha256_file(path)
            if got != want:
                fail(report, f"collector input {name} sha256 {got} != pinned {want}")

    # 3) per-node checkpoint base: only anonymous/ + managed/ may exist,
    #    and BOTH must be empty (empty-old-cache precondition)
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
            nr["base_entries"] = entries
            legacy = [e for e in entries if e not in ("anonymous", "managed")]
            nr["legacy_entries"] = legacy
            if legacy:
                fail(report, f"{node}: legacy entries {legacy} — migration is a separate gap")
            for sub in ("anonymous", "managed"):
                if sub not in entries:
                    continue
                subls = run(["kubectl", "-n", "akernel", "exec", node, "--", "sh", "-c",
                             f"ls -A {args.checkpoint_base}/{sub}"], 60)
                nr[f"{sub}_ls_rc"] = subls["rc"]
                if subls["rc"] != 0:
                    fail(report, f"{node}: {sub}/ listing rc={subls['rc']}: {subls['err'][:120]}")
                    continue
                subentries = [e for e in subls["out"].split() if e]
                nr[f"{sub}_entries"] = subentries
                if subentries:
                    fail(report, f"{node}: {sub}/ not empty ({len(subentries)} entries) — "
                                 f"empty-old-cache precondition violated")
        report["nodes"][node] = nr

    # 4) ACTUAL node set from the cluster via the canonical daemonset
    #    label (app=node): must match --node exactly in both directions
    pods = run(["kubectl", "-n", "akernel", "get", "pods", "-o", "name",
                "-l", NODE_POD_LABEL], 60)
    report["node_pods_rc"] = pods["rc"]
    actual_nodes = []
    if pods["rc"] != 0:
        fail(report, f"node pod listing rc={pods['rc']}: {pods['err'][:120]}")
    else:
        actual_nodes = [l.split("/", 1)[1] for l in pods["out"].splitlines()
                        if l.startswith("pod/" + NODE_POD_NAME_PREFIX)]
        report["node_pods"] = actual_nodes
        if not actual_nodes:
            fail(report, f"no {NODE_POD_NAME_PREFIX}* pods discovered via {NODE_POD_LABEL} — "
                         f"coverage unverifiable")
        missing = [n for n in actual_nodes if n not in args.node]
        if missing:
            fail(report, f"node coverage incomplete: {missing} discovered but not passed via --node")
        extra = [n for n in args.node if n not in actual_nodes]
        if extra:
            fail(report, f"--node entries not present in the cluster: {extra}")

    # 5)+6)+7) real sandbox List + authoritative budget + proxy identity
    # via the VERIFIED collectors. list_by_instance with an empty instance
    # selector only matches empty-label entries — NOT a node-wide list —
    # so the probe drives SandboxService/List with NO selector and counts
    # every returned sandbox.
    probe_script = """
import argparse, json, sys, time
ap = argparse.ArgumentParser()
ap.add_argument("--collector-dir", required=True)
ap.add_argument("--nodes", required=True)
ap.add_argument("--not-older-unix-ms", type=int, required=True)
a = ap.parse_args()
sys.path.insert(0, a.collector_dir)
import collect_stats, sdk_lifecycle as drv, json as _j
out = {"nodes": {}}
for n in a.nodes.split(","):
    rec = {}
    try:
        resp = collect_stats.grpc_call(n, "akernel", "runtime.v1.SandboxService/List", {}, 15)
        if isinstance(resp, dict):
            # proto3 JSON omits empty repeated fields: a dict WITHOUT
            # "sandboxes" is a VALID empty list, not an unknown response
            sandboxes = resp.get("sandboxes", [])
            rec["sandbox_count"] = len(sandboxes) if isinstance(sandboxes, list) else None
        else:
            rec["sandbox_count"] = None
    except Exception as exc:  # probe failure => unknown, never zero
        rec["sandbox_error"] = f"{type(exc).__name__}: {exc}"[:200]
    try:
        probe = drv.proxy_inventory_snapshot(n, "akernel", 120, 30)
        rec["inventory"] = drv.authoritative_snapshot(probe, a.not_older_unix_ms)
    except Exception as exc:
        rec["inventory_error"] = f"{type(exc).__name__}: {exc}"[:200]
    try:
        rec["identity"] = drv.proxy_process_identity(n, "akernel", 30)
    except Exception as exc:
        rec["identity_error"] = f"{type(exc).__name__}: {exc}"[:200]
    out["nodes"][n] = rec
print(_j.dumps(out))
"""
    probe = run([args.venv_python, "-c", probe_script, "--collector-dir", args.collector_dir,
                 "--nodes", ",".join(args.node),
                 "--not-older-unix-ms", str(start_unix_ms - FRESHNESS_WINDOW_MS)], 300)
    report["collector_rc"] = probe["rc"]
    if probe["rc"] != 0:
        fail(report, f"collector probe rc={probe['rc']}: {probe['err'][:200]}")
    else:
        try:
            data = json.loads(probe["out"])
        except ValueError:
            data = None
            fail(report, "collector output unparseable")
        if data is not None:
            report["collector"] = data
            exe_shas = {}
            for node in args.node:
                rec = (data.get("nodes") or {}).get(node)
                if rec is None:
                    fail(report, f"{node}: no collector record (node not covered)")
                    continue
                if "sandbox_error" in rec or rec.get("sandbox_count") is None:
                    fail(report, f"{node}: sandbox List unknown: {rec.get('sandbox_error', 'no count')}")
                elif rec["sandbox_count"] != 0:
                    fail(report, f"{node}: {rec['sandbox_count']} live sandbox(es) — "
                                 f"parked/live records must be empty")
                inv = rec.get("inventory")
                if rec.get("inventory_error") or not isinstance(inv, dict) \
                        or inv.get("status") != "ok" or "snapshot" not in inv:
                    reason = rec.get("inventory_error") or json.dumps(inv, ensure_ascii=False)[:160]
                    fail(report, f"{node}: authoritative budget inventory unknown: {reason}")
                else:
                    snap = inv["snapshot"]
                    summary = {
                        "snapshot_id": snap.get("snapshot_id"),
                        "snapshot_unix_ms": snap.get("snapshot_unix_ms"),
                        "view_actor": snap.get("view_actor"),
                    }
                    report["nodes"].setdefault(node, {})["budget_snapshot"] = summary
                    for problem in validate_budget_snapshot(node, snap):
                        fail(report, problem)
                ident = rec.get("identity")
                if rec.get("identity_error") or not isinstance(ident, dict) \
                        or ident.get("status") != "ok" or not ident.get("processes") \
                        or not ident.get("exe_sha"):
                    reason = rec.get("identity_error") or json.dumps(ident, ensure_ascii=False)[:160]
                    fail(report, f"{node}: function_proxy identity unknown: {reason}")
                else:
                    report["identity"][node] = {"exe_sha": ident["exe_sha"],
                                                "process_count": len(ident["processes"])}
                    exe_shas[node] = ident["exe_sha"]
            if exe_shas and len(set(exe_shas.values())) != 1:
                fail(report, f"node proxies do not share one exe: "
                             f"{ {n: s[:16] for n, s in exe_shas.items()} }")

    # 8) persistent snapshot store: etcd JSON output, keys-only. ANY kv —
    #    including a key outside the requested prefix — is evidence or a
    #    broken probe; both directions refuse the upgrade. A REAL empty
    #    result carries ONLY the header (etcd omits kvs/count when zero —
    #    see plan-a-ov24b-first-post-delete-etcd-and-chart-independent.json);
    #    a bare {}, a malformed header, kvs-without-list or count mismatch
    #    stay unusable.
    etcd = run(["kubectl", "-n", "akernel", "exec", args.etcd_pod, "-c", "etcd", "--",
                "/opt/bitnami/etcd/bin/etcdctl", "--endpoints", args.etcd_endpoint,
                "get", SNAPSHOT_ETCD_PREFIX, "--prefix", "--keys-only", "-w", "json"], 60)
    report["snapshot_store"] = {"rc": etcd["rc"]}
    if etcd["rc"] != 0:
        fail(report, f"etcd probe rc={etcd['rc']}: {etcd['err'][:120]}")
    else:
        try:
            payload = json.loads(etcd["out"])
        except ValueError:
            payload = None
            fail(report, "etcd response is not valid JSON — unusable")
        if payload is not None:
            if not isinstance(payload, dict) or payload.get("error") is not None:
                fail(report, "etcd response is not a JSON object or carries an error field — unusable")
            else:
                header = payload.get("header")
                if not isinstance(header, dict) \
                        or not isinstance(header.get("revision"), int) \
                        or not isinstance(header.get("cluster_id"), int):
                    fail(report, "etcd response header missing/malformed (revision/cluster_id) — unusable")
                else:
                    report["snapshot_store"]["header"] = header
                    kvs = payload.get("kvs")
                    count = payload.get("count")
                    if kvs is None and count is None:
                        pass  # REAL empty result: header-only, kvs/count omitted
                    elif not isinstance(kvs, list):
                        fail(report, "etcd kvs present but not a list — unusable")
                    else:
                        if count is not None and (not isinstance(count, int) or count != len(kvs)):
                            fail(report, f"etcd count {count!r} != kvs {len(kvs)} — torn response")
                        keys = []
                        for kv in kvs:
                            try:
                                key = base64.b64decode(kv.get("key", "")).decode("utf-8", "replace")
                            except Exception:
                                key = None
                            keys.append(key)
                        report["snapshot_store"]["keys"] = keys
                        outside = [k for k in keys if k is None or not k.startswith(SNAPSHOT_ETCD_PREFIX)]
                        if outside:
                            fail(report, f"etcd returned {len(outside)} key(s) outside {SNAPSHOT_ETCD_PREFIX} "
                                         f"(or undecodable) — probe response unusable: {outside[:3]}")
                        if keys:
                            fail(report, f"snapshot store holds {len(keys)} persistent record(s): {keys[:3]}")

    print(json.dumps(report, ensure_ascii=False, indent=1))
    return 0 if report["eligible"] else 1


if __name__ == "__main__":
    sys.exit(main())
