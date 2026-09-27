#!/usr/bin/env python3
"""Black-box counterexample suite for checkpoint_upgrade_preflight.py.

Drives the ACTUAL preflight script as a subprocess against an isolated
fake kubectl and fake collector modules (never the cluster), reproducing
the independent reviewer's counterexamples (status ignored, ledger fields
ignored, etcd foreign-prefix keys silently dropped) plus the additional
fail-closed contract points: canonical label discovery, subdirectory
emptiness, collector input SHA pinning, freshness, identity, and probe
failures. A case passes only when the script's REAL exit code matches the
expected one. Optionally re-runs the same scenarios against a previous
script revision (--old-script) to prove the suite bites: the pinned
counterexample cases must exit 0 there.

Usage:
  checkpoint_upgrade_preflight_selftest.py --script <preflight.py> \
      [--old-script <previous preflight.py>] --output-dir <new empty dir>
"""
import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

NODES = ["akernel-node-a", "akernel-node-b", "akernel-node-c"]
BASE = "/selftest-ck-base"

FAKE_KUBECTL = r'''#!/usr/bin/env python3
import os
import sys

argv = sys.argv[1:]

def out(text, rc=0):
    sys.stdout.write(text)
    sys.exit(rc)

if argv[:2] == ["config", "current-context"]:
    out(os.environ.get("KCTX", "kind-akernel-bm1") + "\n")

if "get" in argv and "pods" in argv:
    label = argv[argv.index("-l") + 1] if "-l" in argv else ""
    canonical_only = os.environ.get("CANONICAL_ONLY") == "1"
    # default repro mode answers BOTH labels (reviewer's fake did); the
    # canonical cluster answers only the daemonset selector app=node
    known = {"app=node"} if canonical_only else {"app=node", "app=akernel-node"}
    if label in known:
        out("".join(f"pod/{n}\n" for n in ["akernel-node-a", "akernel-node-b", "akernel-node-c"]))
    out("")

if "exec" in argv and "--" in argv:
    node = argv[argv.index("exec") + 1]
    tail = argv[argv.index("--") + 1:]
    if tail[:2] == ["sh", "-c"] and len(tail) >= 3:
        cmd = tail[2]
        if cmd.startswith("ls -A "):
            path = cmd[len("ls -A "):]
            if os.environ.get("LS_FAIL_NODE") == node:
                sys.stderr.write("injected: listing failure\n")
                sys.exit(1)
            letter = node.rsplit("-", 1)[1]
            if path == "/selftest-ck-base":
                out(os.environ.get(f"NODE_ENTRIES_{letter}", "anonymous managed") + "\n")
            if path.endswith("/anonymous"):
                out(os.environ.get(f"SUBDIR_ANON_{letter}", "") + "\n")
            if path.endswith("/managed"):
                out(os.environ.get(f"SUBDIR_MANAGED_{letter}", "") + "\n")
            out("")
    if any("etcdctl" in t for t in tail):
        if os.environ.get("ETCD_FAIL") == "1":
            sys.stderr.write("injected: etcd failure\n")
            sys.exit(1)
        if "-w" in tail and "json" in tail:
            out(os.environ.get(
                "ETCD_JSON",
                json.dumps({"count": 0, "kvs": []})) + "\n")
        out(os.environ.get("ETCD_RAW", ""))

sys.stderr.write("fake kubectl: unsupported invocation: %r\n" % (argv,))
sys.exit(1)
'''

# keep the fake self-contained for the etcd default: json import injected
FAKE_KUBECTL = FAKE_KUBECTL.replace(
    "import os\nimport sys\n",
    "import json\nimport os\nimport sys\n", 1)

FAKE_COLLECT_STATS = r'''
import os


def as_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def as_float(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def grpc_call(node, namespace, method, request, timeout):
    """Fake SandboxService/List boundary (same signature as the verified
    collector): injected failures raise, injected live sandboxes return a
    non-empty list, everything else is an empty node."""
    if os.environ.get("LIST_FAIL_NODE") == node:
        raise RuntimeError("grpcurl exit 1: injected List failure")
    if os.environ.get("SANDBOX_NODE") == node:
        return {"sandboxes": [{"id": "sbox-selftest"}]}
    return {"sandboxes": []}
'''

FAKE_SDK_LIFECYCLE = r'''
import os
import time

import collect_stats as cs


def _clean_payload(node):
    letter = node.rsplit("-", 1)[1]
    now_ms = int(time.time() * 1000)
    age = 600_000 if os.environ.get("STALE") == "1" else 1_000
    ledger_total = 0
    ledger_count = 0
    ledger_units = []
    injected = os.environ.get(f"LEDGER_BYTES_{letter}")
    if injected:
        ledger_total = int(injected)
        ledger_count = 1
        ledger_units = [{"unit": "legacy-unit", "outstanding_bytes": ledger_total}]
    payload = {
        "snapshot_id": f"selftest-{node}",
        "snapshot_unix_ms": now_ms - age,
        "event": "resource-inventory-begin",
        "view_actor": f"{node}-ResourceViewActor",
        "is_local_view": True,
        "unit_count": 1,
        "instance_count": 0,
        "resize_budget_entries": 0,
        "baseline_entries": 0,
        "shrink_entries": 0,
        "resize_outstanding_cpu_millicores": 0,
        "resize_outstanding_memory_mb": float(os.environ.get(f"OUTSTANDING_MB_{letter}") or 0),
        "commitment_ledger": {
            "enabled": os.environ.get("LEDGER_ENABLED", "1") == "1",
            "unit_count": ledger_count,
            "units": ledger_units,
            "units_total_bytes": ledger_total,
        },
        "node_diagnostics": [],
        "entries": [],
    }
    if os.environ.get("MISSING_FIELD"):
        payload.pop(os.environ["MISSING_FIELD"], None)
    if os.environ.get("EXTRA_FIELD"):
        payload["future_ledger_bytes"] = 5
    return payload


def proxy_inventory_snapshot(node, namespace, since_s, timeout):
    if os.environ.get("PROBE_UNKNOWN_NODE") == node:
        return {"status": "unknown", "reason": "injected: proxy log unreadable"}
    return {"status": "ok", "snapshots": [_clean_payload(node)], "snapshot_count": 1}


def authoritative_snapshot(probe, not_older_unix_ms=None):
    if probe.get("status") != "ok":
        return {"status": probe.get("status", "unknown")}
    candidates = []
    for payload in probe.get("snapshots", []):
        if "-virtual" in str(payload.get("view_actor", "")):
            continue
        when = cs.as_int(payload.get("snapshot_unix_ms"))
        if not_older_unix_ms is not None and (when is None or when < not_older_unix_ms):
            continue
        candidates.append(payload)
    if not candidates:
        return {"status": "unknown",
                "reason": "no complete non-stale primary-view snapshot"}
    latest = max(candidates, key=lambda p: cs.as_int(p.get("snapshot_unix_ms")) or 0)
    if os.environ.get("AUTH_UNKNOWN_WITH_SNAPSHOT") == "1":
        # the reviewer's counterexample shape: status unknown WITH a
        # clean-looking snapshot attached — the caller must check status
        return {"status": "unknown", "snapshot": latest}
    return {"status": "ok", "snapshot": latest}


def proxy_process_identity(node, namespace, timeout):
    if os.environ.get("IDENTITY_UNKNOWN") == "1":
        return {"status": "unknown", "reason": "injected: identity probe failure"}
    letter = node.rsplit("-", 1)[1]
    exe = "a" * 64
    if os.environ.get("IDENTITY_SPLIT") == "1" and letter == "b":
        exe = "b" * 64
    return {"status": "ok", "exe_sha": exe,
            "processes": [{"pid": "1", "starttime": "99", "exe_sha": exe}]}
'''

B64 = lambda s: base64.b64encode(s.encode()).decode()

# Cases for the CURRENT script: (name, env, nodes, sha_override, expected_rc)
NEW_CASES = [
    ("clean", {}, None, None, 0),
    ("old_ledger_debt_node_c", {"LEDGER_BYTES_c": "1024"}, None, None, 1),
    ("outstanding_growth_tail_node_b", {"OUTSTANDING_MB_b": "128"}, None, None, 1),
    ("auth_status_unknown_plain", {"PROBE_UNKNOWN_NODE": "akernel-node-b"}, None, None, 1),
    ("auth_status_unknown_with_snapshot", {"AUTH_UNKNOWN_WITH_SNAPSHOT": "1"}, None, None, 1),
    ("etcd_foreign_prefix_keys",
     {"ETCD_JSON": json.dumps({"count": 2, "kvs": [
         {"key": B64("/other/prefix/foo")}, {"key": B64("/yr/other/x")}]})},
     None, None, 1),
    ("etcd_holds_snapshot_record",
     {"ETCD_JSON": json.dumps({"count": 1, "kvs": [
         {"key": B64("/yr/snapshots/v1/t/snap-x")}]})},
     None, None, 1),
    ("etcd_not_json", {"ETCD_JSON": "not json at all"}, None, None, 1),
    ("etcd_probe_failure", {"ETCD_FAIL": "1"}, None, None, 1),
    ("canonical_label_discovery_still_clean", {"CANONICAL_ONLY": "1"}, None, None, 0),
    ("coverage_missing_node", {}, NODES[:2], None, 1),
    ("unknown_extra_node_arg", {}, NODES + ["akernel-node-z"], None, 1),
    ("subdir_anonymous_nonempty", {"SUBDIR_ANON_a": "ckpt-leftover"}, None, None, 1),
    ("subdir_managed_nonempty", {"SUBDIR_MANAGED_b": "snap-old"}, None, None, 1),
    ("legacy_base_entry", {"NODE_ENTRIES_c": "anonymous managed ckpt-old-123"}, None, None, 1),
    ("collector_sha_mismatch", {}, None, "f" * 64, 1),
    ("ledger_disabled_is_unknown", {"LEDGER_ENABLED": "0"}, None, None, 1),
    ("unknown_snapshot_field", {"EXTRA_FIELD": "1"}, None, None, 1),
    ("missing_budget_field", {"MISSING_FIELD": "instance_count"}, None, None, 1),
    ("live_sandbox_present", {"SANDBOX_NODE": "akernel-node-b"}, None, None, 1),
    ("sandbox_list_probe_error", {"LIST_FAIL_NODE": "akernel-node-b"}, None, None, 1),
    ("stale_budget_snapshot", {"STALE": "1"}, None, None, 1),
    ("proxy_identity_split", {"IDENTITY_SPLIT": "1"}, None, None, 1),
    ("proxy_identity_unknown", {"IDENTITY_UNKNOWN": "1"}, None, None, 1),
    ("context_mismatch", {"KCTX": "kind-somewhere-else"}, None, None, 1),
    ("base_listing_failure", {"LS_FAIL_NODE": "akernel-node-c"}, None, None, 1),
]

# Reviewer's pinned counterexamples + adjacent gaps re-run against the
# OLD script: they must exit 0 there (proving this suite detects the
# defects it was written for) except canonical-label, where the old
# script wrongly refuses a clean canonical cluster.
OLD_CASES = [
    ("clean", {}, 0),
    ("old_ledger_debt_node_c", {"LEDGER_BYTES_c": "1024"}, 0),
    ("outstanding_growth_tail_node_b", {"OUTSTANDING_MB_b": "128"}, 0),
    ("auth_status_unknown_with_snapshot", {"AUTH_UNKNOWN_WITH_SNAPSHOT": "1"}, 0),
    ("etcd_foreign_prefix_keys",
     {"ETCD_RAW": "/other/prefix/foo\n/yr/other/x\n",
      "ETCD_JSON": json.dumps({"count": 2, "kvs": [
          {"key": B64("/other/prefix/foo")}, {"key": B64("/yr/other/x")}]})},
     0),
    ("subdir_anonymous_nonempty", {"SUBDIR_ANON_a": "ckpt-leftover"}, 0),
    ("canonical_label_discovery", {"CANONICAL_ONLY": "1"}, 1),
]


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_fixtures(workdir):
    bin_dir = os.path.join(workdir, "bin")
    collector_dir = os.path.join(workdir, "collectors")
    os.makedirs(bin_dir)
    os.makedirs(collector_dir)
    kubectl = os.path.join(bin_dir, "kubectl")
    with open(kubectl, "w") as f:
        f.write(FAKE_KUBECTL)
    os.chmod(kubectl, 0o755)
    with open(os.path.join(collector_dir, "collect_stats.py"), "w") as f:
        f.write(FAKE_COLLECT_STATS)
    with open(os.path.join(collector_dir, "sdk_lifecycle.py"), "w") as f:
        f.write(FAKE_SDK_LIFECYCLE)
    return bin_dir, collector_dir


def run_case(script, case_env, nodes, sha_collect, sha_sdk, bin_dir, collector_dir):
    cmd = [sys.executable, os.path.abspath(script)]
    for n in (nodes or NODES):
        cmd += ["--node", n]
    cmd += ["--checkpoint-base", BASE,
            "--collector-dir", collector_dir,
            "--etcd-pod", "akernel-etcd-0",
            "--etcd-endpoint", "http://10.244.0.10:2379",
            "--venv-python", sys.executable]
    if sha_collect is not None:
        cmd += ["--collect-stats-sha256", sha_collect]
    if sha_sdk is not None:
        cmd += ["--sdk-lifecycle-sha256", sha_sdk]
    env = dict(os.environ)
    env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    env.update(case_env)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180, env=env)
    report = None
    try:
        report = json.loads(proc.stdout)
    except ValueError:
        pass
    return {"command": cmd, "rc": proc.returncode, "stderr": proc.stderr[-500:],
            "report": report}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True, help="preflight script under test")
    ap.add_argument("--old-script", default=None,
                    help="previous revision to prove the suite bites (optional)")
    ap.add_argument("--output-dir", required=True, help="new empty evidence dir")
    args = ap.parse_args()

    if os.path.exists(args.output_dir):
        print(f"REFUSING: output dir already exists: {args.output_dir}", file=sys.stderr)
        return 2
    os.makedirs(args.output_dir)

    workdir = tempfile.mkdtemp(prefix="preflight-selftest-")
    try:
        bin_dir, collector_dir = build_fixtures(workdir)
        sha_collect = sha256_file(os.path.join(collector_dir, "collect_stats.py"))
        sha_sdk = sha256_file(os.path.join(collector_dir, "sdk_lifecycle.py"))

        results = {
            "script": os.path.abspath(args.script),
            "script_sha256": sha256_file(args.script),
            "old_script": os.path.abspath(args.old_script) if args.old_script else None,
            "old_script_sha256": sha256_file(args.old_script) if args.old_script else None,
            "fake_collector_sha256": {"collect_stats.py": sha_collect,
                                      "sdk_lifecycle.py": sha_sdk},
            "cases": [],
        }
        failures = 0

        for name, env, nodes, sha_override, expected in NEW_CASES:
            ran = run_case(args.script, env, nodes,
                           sha_override or sha_collect, sha_sdk, bin_dir, collector_dir)
            ok = ran["rc"] == expected
            failures += 0 if ok else 1
            results["cases"].append({
                "case": name, "target": "new", "expected_rc": expected,
                "actual_rc": ran["rc"], "ok": ok, "env": env,
                "nodes": nodes or NODES,
                "eligible": (ran["report"] or {}).get("eligible"),
                "reasons": (ran["report"] or {}).get("reasons", [])[:6],
                "stderr_tail": ran["stderr"][-200:] if ran["rc"] != expected else "",
            })
            print(f"{'PASS' if ok else 'FAIL'} new  {name}: expected {expected} got {ran['rc']}")

        if args.old_script:
            for name, env, expected in OLD_CASES:
                ran = run_case(args.old_script, env, None, None, None, bin_dir, collector_dir)
                ok = ran["rc"] == expected
                failures += 0 if ok else 1
                results["cases"].append({
                    "case": name, "target": "old", "expected_rc": expected,
                    "actual_rc": ran["rc"], "ok": ok, "env": env, "nodes": NODES,
                    "eligible": (ran["report"] or {}).get("eligible"),
                    "reasons": (ran["report"] or {}).get("reasons", [])[:6],
                    "stderr_tail": ran["stderr"][-200:] if ran["rc"] != expected else "",
                })
                print(f"{'PASS' if ok else 'FAIL'} old  {name}: expected {expected} got {ran['rc']}")

        results["summary"] = {"total": len(results["cases"]),
                              "failures": failures,
                              "all_passed": failures == 0}
        with open(os.path.join(args.output_dir, "selftest-results.json"), "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=1)
        print(json.dumps(results["summary"]))
        return 0 if failures == 0 else 1
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
