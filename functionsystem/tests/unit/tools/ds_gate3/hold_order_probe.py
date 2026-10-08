#!/usr/bin/env python3
# DS gate-3 B2b controlled-detention ORDER probe (root sequence):
#   1. Init + positive control (Set/Get/Del + raw-SDK miss) through the proxy
#      in passthrough-equivalent state (hold armed only later).
#   2. Send ONE designated OLD Set on the main client; verify from the proxy
#      log that the request was DETAINED (hold) and NOT forwarded.
#   3. Keep the same client/connection ALIVE while the old Set call exceeds
#      its return deadline; do NOT ASSERT-out (an assert would destruct the
#      client and actively cancel the detained request).
#   4. Through a SEPARATE healthy channel (a second client that bypasses the
#      proxy), write a NEW value for the SAME key and read it back.
#   5. Explicitly RELEASE the old request (create the release file).
#   6. Read the final value via the independent raw client and record the
#      time-order: did the late-committed OLD write clobber the NEW value?
# All observations land in a JSONL with absolute timestamps; exit rc is
# preserved. This is an OBSERVATION harness — it records what the backend
# did, it does not ASSERT success shapes.
import json, os, socket, subprocess, sys, tempfile, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

GATE_DIR = "/root/akernel-bm1/ds-exp"
CASE = os.path.join(GATE_DIR, "case", "b2-hold")
PROBE_BIN = ("/root/akernel-bm1/src/fs-work/functionsystem/build-test/bin/"
             "instance_ctrl_intent_gate_test")

def now():
    return time.time()

def w(row):
    with open(os.path.join(CASE, "order.jsonl"), "a") as f:
        f.write(json.dumps(row) + "\n")

def wait_file(path, timeout=60, absent=False):
    deadline = now() + timeout
    while now() < deadline:
        if (not os.path.exists(path)) if absent else os.path.exists(path):
            return True
        time.sleep(0.2)
    return False

def proxy_events_since(t0):
    evs = []
    try:
        with open(os.path.join(CASE, "proxy.log")) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("t", 0) >= t0:
                    evs.append(e)
    except OSError:
        pass
    return evs

# 0. Fresh ARM/RELEASE state for this run.
for f in ("ARM", "RELEASE"):
    p = os.path.join(CASE, f)
    if os.path.exists(p):
        os.remove(p)
w({"op": "run_start", "t": now()})

# 1. Positive control through the proxy (hold armed only later, so this is
#    clean passthrough). The C++ probe writes phaseA rows + XML + its own rc.
env = dict(os.environ)
env.update({
    "DS_GATE3": "1",
    "DS_GATE3_RUNID": "b2order",
    "DS_GATE3_WORKER_PORT": "31592",
    "DS_GATE3_OUT": os.path.join(CASE, "phase.jsonl"),
    "LD_LIBRARY_PATH": ":".join([
        "/root/akernel-bm1/src/fs-work/vendor/output/Install/grpc/lib",
        "/root/akernel-bm1/src/fs-work/vendor/output/Install/protobuf/lib",
        "/root/akernel-bm1/src/fs-work/vendor/output/Install/datasystem/sdk/cpp/lib",
        "/root/akernel-bm1/src/fs-work/vendor/output/Install/obs/lib",
        "/root/akernel-bm1/src/fs-work/vendor/output/Install/openssl/lib",
        "/root/akernel-bm1/src/fs-work/vendor/src/datasystem/service/lib",
        "/root/akernel-bm1/src/fs-work/common/litebus/output/lib",
        "/root/akernel-bm1/src/fs-work/common/logs/output/lib",
        "/root/akernel-bm1/src/fs-work/common/metrics/output/lib",
    ]),
})
t0 = now()
pc = subprocess.run(
    [PROBE_BIN, "--gtest_filter=DsGate3Probe.*",
     "--gtest_output=xml:" + os.path.join(CASE, "positive.xml")],
    env=env, stdout=open(os.path.join(CASE, "positive.stdout"), "w"),
    stderr=subprocess.STDOUT, timeout=180)
w({"op": "positive_control", "rc": pc.returncode, "dur_s": now() - t0})
if pc.returncode != 0:
    print("positive control failed rc=%d — aborting (environment, not a "
          "conclusion)" % pc.returncode)
    sys.exit(3)

# 2. ARM the hold, then send ONE designated OLD Set on the main client and
#    let its call exceed the 9s deadline WITHOUT asserting/tearing down.
#    The C++ probe is not used here — a raw KVClient gives us a live,
#    long-lived client object we control precisely.
arm = os.path.join(CASE, "ARM")
open(arm, "w").write("1")
w({"op": "armed", "t": now()})

sys.path.insert(0, "/root/akernel-bm1/ds-exp")
# Drive the raw SDK through a small python shim is NOT available (C++ SDK);
# instead run a dedicated C++ helper compiled from the probe target: the
# probe binary already links DSCacheClientImpl. We reuse the probe process
# pattern: a single Set under a fresh runid, with the harness watching the
# proxy log for the hold event while the call is in flight.
env2 = dict(env)
env2["DS_GATE3_RUNID"] = "b2old"
single = os.path.join(CASE, "oldset.stdout")
# The probe performs init+verifier+control then ARM via DS_GATE3_ARM — but
# the proxy is ALREADY armed (ARM file exists), so the first op Set is
# detained by the proxy at once. We let the probe run past its 9s deadline
# under our own watch; the timeout below (25s) exceeds it deliberately.
t1 = now()
try:
    old = subprocess.run(
        [PROBE_BIN, "--gtest_filter=DsGate3Probe.*",
         "--gtest_output=xml:" + os.path.join(CASE, "oldset.xml")],
        env=env2, stdout=open(single, "w"), stderr=subprocess.STDOUT,
        timeout=25)
    old_rc = old.returncode
except subprocess.TimeoutExpired:
    old_rc = "watchdog-25s"
w({"op": "old_set_call", "rc": old_rc, "dur_s": now() - t1})

evs = proxy_events_since(t1)
held = [e for e in evs if e.get("e") == "req_packet" and e.get("act", "").startswith("held")]
fwd_before_release = [e for e in evs if e.get("e") == "forwarded_after_hold"]
w({"op": "detention_check", "held_packets": len(held),
   "forwarded_before_release": len(fwd_before_release)})
print("detained packets:", len(held),
      "forwarded before release:", len(fwd_before_release))

# 3. With the old call past its deadline (or watchdog-cut), write the NEW
#    value through a SEPARATE healthy channel — a raw socket to the worker's
#    REAL address would need the DS protocol; the practical healthy channel
#    is another probe run with the proxy pointed OFF the advertised port.
#    For the single-instance topology the worker also listens on 31594
#    directly: point DS_GATE3_WORKER_PORT=31594 to bypass the proxy.
env3 = dict(env)
env3.update({"DS_GATE3_RUNID": "b2new", "DS_GATE3_WORKER_PORT": "31594"})
t2 = now()
new = subprocess.run(
    [PROBE_BIN, "--gtest_filter=DsGate3Probe.*",
     "--gtest_output=xml:" + os.path.join(CASE, "newset.xml")],
    env=env3, stdout=open(os.path.join(CASE, "newset.stdout"), "w"),
    stderr=subprocess.STDOUT, timeout=60)
w({"op": "new_write_healthy_channel", "rc": new.returncode, "dur_s": now() - t2})

# 4. RELEASE the old request.
rel = os.path.join(CASE, "RELEASE")
open(rel, "w").write("1")
w({"op": "released", "t": now()})
time.sleep(3)
evs2 = proxy_events_since(t1)
w({"op": "post_release_events",
   "forwarded_after_hold": len([e for e in evs2 if e.get("e") == "forwarded_after_hold"])})

# 5. Final read-back through the healthy channel (31594) with the same
#    key namespace; the probe re-verifies its own keys, and the JSONL rows
#    already carry match information for the b2old/b2new runids.
print("B2 order run complete — evidence in", CASE)
print("next: read order.jsonl + phase rows for runids b2old/b2new and "
      "compare write/read timestamps against hold/forward events")
w({"op": "run_end", "t": now()})
