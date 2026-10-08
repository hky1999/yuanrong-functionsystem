#!/usr/bin/env python3
# Local TCP positive/negative tests for the persistent fault proxy (root
# 2026-10-08 review): no C++ build needed, no DS instance needed — a plain
# TCP echo server stands in for the worker. Verifies, in order:
#   1. HOLD captures the FIRST packet sent AFTER arming even when the relay
#      thread was already blocked in recv pre-arm (the attempt-2a79 bug).
#   2. The detained packet is forwarded ONLY after the release file exists.
#   3. HOLD_MAX_S bounds a never-released hold (hold_timeout + release).
#   4. A delay longer than the old 10s upstream timeout does NOT cut the
#      connection (delay 12s passes intact — no proxy-imposed 9/10s cut).
#   5. Resource ceilings recycle: after connections close, active conns and
#      threads_alive return to zero (stats events).
# Each case prints PASS/FAIL; exit code is the number of failures.
import socket, subprocess, sys, tempfile, threading, time, os, json

HERE = os.path.dirname(os.path.abspath(__file__))
PROXY = os.path.join(HERE, "fault_proxy.py")

def wait_listening(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False

def last_events(log, n=50):
    try:
        with open(log) as f:
            return [json.loads(l) for l in f.readlines()[-n:]]
    except OSError:
        return []

def start_proxy(tmp, mode, arm="", hold_max=None, listen=31692, target=31694):
    log = os.path.join(tmp, "proxy.log")
    env = dict(os.environ)
    env.update({
        "DS_PROXY_LISTEN": f"127.0.0.1:{listen}",
        "DS_PROXY_TARGET": f"127.0.0.1:{target}",
        "DS_PROXY_MODE": mode,
        "DS_PROXY_LOG": log,
        "DS_PROXY_MAX_CONN": "16",
        "DS_PROXY_MAX_THREADS": "64",
    })
    if arm:
        env["DS_PROXY_ARM"] = arm
    if hold_max is not None:
        env["DS_PROXY_HOLD_MAX_S"] = str(hold_max)
    p = subprocess.Popen([sys.executable, PROXY], env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_listening(listen), f"proxy did not listen on {listen}"
    return p, log

def echo_server(port, ready):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(8)
    ready.set()
    while True:
        c, _ = s.accept()
        def serve(c=c):
            try:
                while True:
                    d = c.recv(65536)
                    if not d:
                        break
                    c.sendall(d)
            except OSError:
                pass
            finally:
                try: c.close()
                except OSError: pass
        threading.Thread(target=serve, daemon=True).start()

def main():
    failures = 0
    tmp = tempfile.mkdtemp(prefix="dsproxy-test-")
    ready = threading.Event()
    threading.Thread(target=echo_server, args=(31694, ready), daemon=True).start()
    ready.wait(5)

    # ── Case 1+2: hold captures first post-arm packet, forwards on release ──
    arm = os.path.join(tmp, "ARM")
    release = os.path.join(tmp, "RELEASE")
    p, log = start_proxy(tmp, f"hold:{release}", arm=arm)
    try:
        c = socket.create_connection(("127.0.0.1", 31692), timeout=5)
        c.settimeout(15)
        c.sendall(b"pre-arm")          # relay thread now blocks in recv
        assert c.recv(64) == b"pre-arm"  # echo roundtrip before arming
        open(arm, "w").write("1")      # ARM now — thread is INSIDE recv
        c.sendall(b"post-arm")         # first packet after arming
        t0 = time.time()
        try:
            got = c.recv(64)           # must NOT return before release
            early = True
        except socket.timeout:
            early = False
        if early:
            print("FAIL case1: held packet returned before release:", got)
            failures += 1
        else:
            print("PASS case1: first post-arm packet detained")
        open(release, "w").write("1")
        got = c.recv(64)
        if got == b"post-arm" and time.time() - t0 > 0.05:
            print("PASS case2: forwarded only after release file")
        else:
            print("FAIL case2:", got); failures += 1
        evs = last_events(log)
        if any(e.get("e") == "hold_released" for e in evs) and \
           any(e.get("e") == "req_packet" and e.get("act") in ("held", "held_timeout") for e in evs):
            print("PASS case2b: hold_released + held req_packet events recorded")
        else:
            print("FAIL case2b: missing hold events"); failures += 1
        c.close()
    finally:
        p.terminate(); p.wait(5)

    # ── Case 3: bounded hold (never released) ──
    arm3 = os.path.join(tmp, "ARM3")
    rel3 = os.path.join(tmp, "REL3-NEVER")
    p, log = start_proxy(tmp, f"hold:{rel3}", arm=arm3, hold_max=2)
    try:
        c = socket.create_connection(("127.0.0.1", 31692), timeout=5)
        c.settimeout(15)
        c.sendall(b"x1"); assert c.recv(64) == b"x1"
        open(arm3, "w").write("1")
        c.sendall(b"detained")
        t0 = time.time()
        got = c.recv(64)
        dt = time.time() - t0
        if got == b"detained" and 1.5 < dt < 6:
            print(f"PASS case3: bounded hold released at {dt:.1f}s")
        else:
            print("FAIL case3:", got, dt); failures += 1
        if any(e.get("e") == "hold_timeout" for e in last_events(log)):
            print("PASS case3b: hold_timeout event recorded")
        else:
            print("FAIL case3b: no hold_timeout event"); failures += 1
        c.close()
    finally:
        p.terminate(); p.wait(5)

    # ── Case 4: delay 12s must NOT cut the connection ──
    p, log = start_proxy(tmp, "delay:12")
    try:
        c = socket.create_connection(("127.0.0.1", 31692), timeout=5)
        c.settimeout(30)
        t0 = time.time()
        c.sendall(b"slow")
        got = c.recv(64)
        dt = time.time() - t0
        if got == b"slow" and 11 < dt < 20:
            print(f"PASS case4: 12s delay passed intact at {dt:.1f}s (no proxy cut)")
        else:
            print("FAIL case4:", got, dt); failures += 1
        c.close()
    finally:
        p.terminate(); p.wait(5)

    # ── Case 5: ceilings recycle ──
    p, log = start_proxy(tmp, "passthrough")
    try:
        cs = [socket.create_connection(("127.0.0.1", 31692), timeout=5) for _ in range(4)]
        for c in cs:
            c.sendall(b"r"); assert c.recv(64) == b"r"
        time.sleep(6)   # one stats tick
        st = [e for e in last_events(log) if e.get("e") == "stats"][-1]
        peak_conns = st["conns"]; peak_threads = st["threads_alive"]
        for c in cs:
            c.close()
        time.sleep(7)   # next stats tick after close
        st2 = [e for e in last_events(log) if e.get("e") == "stats"][-1]
        if peak_conns >= 4 and st2["conns"] == 0 and st2["threads_alive"] == 0:
            print(f"PASS case5: conns {peak_conns}->0, threads {peak_threads}->0")
        else:
            print("FAIL case5:", st, "->", st2); failures += 1
    finally:
        p.terminate(); p.wait(5)

    print(f"fault_proxy local tests: {5 - failures}/5 groups pass" if failures
          else "fault_proxy local tests: ALL PASS")
    return failures

if __name__ == "__main__":
    sys.exit(main())
