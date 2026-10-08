#!/usr/bin/env python3
# B2b runner (root-corrected): launches ONLY DsGate3HoldOrder in a per-case
# directory; saves the exact command, probe-binary SHA, raw exit rc, stdout
# and XML — no grep/head filtering of the exit code, no PhaseA rerun.
import hashlib, json, os, subprocess, sys, time

CASE_ROOT = "/root/akernel-bm1/ds-exp/case"
BIN = ("/root/akernel-bm1/src/fs-work/functionsystem/build-test/bin/"
       "instance_ctrl_intent_gate_test")
LD = ":".join([
    "/root/akernel-bm1/src/fs-work/vendor/output/Install/grpc/lib",
    "/root/akernel-bm1/src/fs-work/vendor/output/Install/protobuf/lib",
    "/root/akernel-bm1/src/fs-work/vendor/output/Install/datasystem/sdk/cpp/lib",
    "/root/akernel-bm1/src/fs-work/vendor/output/Install/obs/lib",
    "/root/akernel-bm1/src/fs-work/vendor/output/Install/openssl/lib",
    "/root/akernel-bm1/src/fs-work/vendor/src/datasystem/service/lib",
    "/root/akernel-bm1/src/fs-work/common/litebus/output/lib",
    "/root/akernel-bm1/src/fs-work/common/logs/output/lib",
    "/root/akernel-bm1/src/fs-work/common/metrics/output/lib",
])

def main():
    run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    case = os.path.join(CASE_ROOT, "b2-hold-" + run)
    os.makedirs(case, exist_ok=True)
    for f in ("ARM", "RELEASE"):
        p = os.path.join(case, f)
        if os.path.exists(p):
            os.remove(p)
    # OWN proxy for THIS run: hold mode gated on THIS case's ARM/RELEASE —
    # never reuse a proxy still bound to an older case's gate paths.
    proxy = subprocess.Popen(
        [sys.executable, "/root/akernel-bm1/ds-exp/fault_proxy.py"],
        env=dict(os.environ, **{
            "DS_PROXY_LISTEN": "127.0.0.1:31592",
            "DS_PROXY_TARGET": "127.0.0.1:31594",
            "DS_PROXY_MODE": "hold:" + os.path.join(case, "RELEASE"),
            "DS_PROXY_ARM": os.path.join(case, "ARM"),
            "DS_PROXY_LOG": os.path.join(case, "proxy.log"),
            "DS_PROXY_HOLD_MAX_S": "120",
        }), stdout=open(os.path.join(case, "proxy.stdout"), "w"),
        stderr=subprocess.STDOUT)
    import time as _t; _t.sleep(2)
    proxy_sha = hashlib.sha256(
        open("/root/akernel-bm1/ds-exp/fault_proxy.py", "rb").read()).hexdigest()
    sha = hashlib.sha256(open(BIN, "rb").read()).hexdigest()
    cmd = [BIN, "--gtest_filter=DsGate3HoldOrder.*",
           "--gtest_output=xml:" + os.path.join(case, "order.xml")]
    env = dict(os.environ)
    env.update({
        "LD_LIBRARY_PATH": LD,
        "DS_GATE3_ORDER": "1",
        "DS_GATE3_RUNID": run,
        "DS_GATE3_ARM": os.path.join(case, "ARM"),
        "DS_GATE3_RELEASE": os.path.join(case, "RELEASE"),
        "DS_GATE3_OUT": os.path.join(case, "order.jsonl"),
    })
    with open(os.path.join(case, "runner.json"), "w") as rj:
        json.dump({"cmd": cmd, "env_gates": {k: env[k] for k in env if k.startswith("DS_GATE3")},
                   "bin_sha256": sha, "proxy_pid": proxy.pid,
                   "proxy_sha256": proxy_sha,
                   "gate_paths": {"ARM": os.path.join(case, "ARM"),
                                   "RELEASE": os.path.join(case, "RELEASE")},
                   "t": time.time()}, rj, indent=1)
    with open(os.path.join(case, "stdout.log"), "w") as out:
        rc = subprocess.call(cmd, env=env, stdout=out, stderr=subprocess.STDOUT,
                             timeout=300)
    with open(os.path.join(case, "rc.txt"), "w") as rf:
        rf.write(str(rc) + "\n")
    proxy.terminate(); proxy.wait(5)
    print("case:", case, "rc:", rc)
    return rc

if __name__ == "__main__":
    sys.exit(main())
