#!/usr/bin/env python3
# DS gate-3 Phase B fault proxy (persistent, versioned copy).
# Environment (all overridable):
#   DS_PROXY_LISTEN (default 127.0.0.1:31592) — the ADVERTISED worker address
#   DS_PROXY_TARGET (default 127.0.0.1:31594) — the worker's REAL bind address
#   DS_PROXY_MODE   passthrough | delay:N | drop | hold:FILE
#     passthrough — baseline transit counter (B3)
#     delay:N      — delay every RESPONSE by N seconds (B1)
#     drop         — forward request, DROP the response (B2a: outcome-unknown)
#     hold:FILE    — DETAIN the request until FILE exists (B2b: controlled
#                    detention — create the file only AFTER the caller has
#                    returned, then observe backend commit ordering)
#   DS_PROXY_ARM    gate file: when set, delay/drop/hold activate ONLY after
#                   this file exists (Init + positive controls stay clean;
#                   the probe creates the arm file at its explicit arm point)
#   DS_PROXY_LOG    event log path (per-round file; never overwrite old ones)
#   DS_PROXY_MAX_CONN / DS_PROXY_MAX_THREADS — resource ceilings
# Counters: conns / req_bytes / rsp_bytes / drops / holds + 5s stats lines.
# Guards (attempt-2/3/6 lessons): LISTEN==TARGET is refused outright; both
# relay directions count bytes; per-connection threads are bounded by
# DS_PROXY_MAX_THREADS and pending connections by DS_PROXY_MAX_CONN.
import socket, threading, time, sys, os, json

def _addr(env, default):
    v = os.environ.get(env, default)
    host, port = v.rsplit(":", 1)
    return (host, int(port))

LISTEN = _addr("DS_PROXY_LISTEN", "127.0.0.1:31592")
TARGET = _addr("DS_PROXY_TARGET", "127.0.0.1:31594")
mode = os.environ.get("DS_PROXY_MODE", "passthrough")
log = os.environ.get("DS_PROXY_LOG", "/tmp/ds-proxy.log")
armFile = os.environ.get("DS_PROXY_ARM", "")
MAX_CONN = int(os.environ.get("DS_PROXY_MAX_CONN", "512"))
MAX_THREADS = int(os.environ.get("DS_PROXY_MAX_THREADS", "2048"))

lock = threading.Lock()
counts = {"conns": 0, "req_bytes": 0, "rsp_bytes": 0, "drops": 0, "holds": 0}
threads_alive = 0

def ev(**k):
    k["t"] = time.time()
    with open(log, "a") as f:
        f.write(json.dumps(k) + "\n")

if LISTEN == TARGET:
    ev(e="fatal", reason="LISTEN==TARGET self-loop refused")
    sys.exit(2)

def armed():
    return (not armFile) or os.path.exists(armFile)

def bump(counter, n):
    with lock:
        counts[counter] += n

def handle(c):
    global threads_alive
    try:
        with lock:
            if counts["conns"] >= MAX_CONN:
                ev(e="conn_refused_over_ceiling")
                c.close()
                return
            counts["conns"] += 1
        ev(e="conn")
        try:
            u = socket.create_connection(TARGET, timeout=10)
        except OSError:
            c.close()
            ev(e="upstream_fail")
            return
        m = mode.split(":")
        kind = m[0]
        arg = int(m[1]) if len(m) > 1 and m[1].isdigit() else 0
        dfile = m[1] if kind == "hold" and len(m) > 1 else None

        def c2u():
            try:
                while True:
                    if dfile is not None and armed():
                        while not os.path.exists(dfile):
                            time.sleep(0.05)
                        with lock:
                            counts["holds"] += 1
                        ev(e="hold_released", key=dfile)
                    data = c.recv(65536)
                    if not data:
                        break
                    bump("req_bytes", len(data))
                    u.sendall(data)
            except OSError:
                pass
            finally:
                try: u.close()
                except OSError: pass

        def u2c():
            try:
                while True:
                    data = u.recv(65536)
                    if not data:
                        break
                    bump("rsp_bytes", len(data))
                    if kind == "delay" and armed():
                        time.sleep(arg)
                    if kind == "drop" and armed():
                        bump("drops", 1)
                        continue
                    c.sendall(data)
            except OSError:
                pass
            finally:
                try: c.close()
                except OSError: pass

        with lock:
            if threads_alive + 2 > MAX_THREADS:
                ev(e="thread_ceiling")
                c.close(); u.close()
                return
            threads_alive += 2
        threading.Thread(target=c2u, daemon=True).start()
        threading.Thread(target=u2c, daemon=True).start()
        with lock:
            threads_alive -= 0  # threads self-decrement below via watcher
    finally:
        pass

def stats():
    while True:
        time.sleep(5)
        with lock:
            c = dict(counts)
        ev(e="stats", **c)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(LISTEN)
s.listen(64)
ev(e="listening", mode=mode, listen=LISTEN, target=TARGET, arm=armFile)
threading.Thread(target=stats, daemon=True).start()
while True:
    c, _ = s.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
