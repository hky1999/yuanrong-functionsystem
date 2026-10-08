#!/usr/bin/env python3
# DS gate-3 Phase B fault proxy (persistent, versioned copy).
# Environment (all overridable):
#   DS_PROXY_LISTEN (default 127.0.0.1:31592) — the ADVERTISED worker address
#   DS_PROXY_TARGET (default 127.0.0.1:31594) — the worker's REAL bind address
#   DS_PROXY_MODE   passthrough | delay:N | drop | hold:FILE
#     passthrough — baseline transit counter (B3)
#     delay:N      — delay every RESPONSE by N seconds (B1)
#     drop         — forward request, DROP the response (B2a: outcome-unknown)
#     hold:FILE    — DETAIN each request until FILE exists (B2b: controlled
#                    detention — create the file only AFTER the caller has
#                    returned), then forward and record the release
#   DS_PROXY_ARM    gate file: when set, delay/drop/hold activate ONLY after
#                   this file exists. The check runs AFTER recv and BEFORE
#                   the fault acts, so the FIRST packet sent after arming is
#                   captured even when the relay thread was already blocked
#                   in recv pre-arm.
#   DS_PROXY_LOG        event log path (per-round file; never overwrite)
#   DS_PROXY_MAX_CONN   active-connection ceiling (default 512)
#   DS_PROXY_MAX_THREADS active-relay-thread ceiling (default 2048)
#   DS_PROXY_HOLD_MAX_S bounded hold release (default 300s; a detained
#                       request releases anyway and logs hold_timeout — the
#                       proxy never wedges a request forever)
# Counters: conns(active) / total_conns / req_bytes / rsp_bytes / drops /
# holds / hold_timeouts + 5s stats lines. Per-packet fault decisions are
# evented (held/forwarded/dropped/delayed) so one injection maps to one op.
# Proxy I/O protection is EXPLICIT and separate from fault effects: the
# upstream connect has a 10s CONNECT timeout only; after connect the socket
# is set blocking (no read timeout) so a delayed response is never cut off
# by the proxy itself.
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
HOLD_MAX_S = float(os.environ.get("DS_PROXY_HOLD_MAX_S", "300"))

lock = threading.Lock()
counts = {"conns": 0, "total_conns": 0, "req_bytes": 0, "rsp_bytes": 0,
          "drops": 0, "holds": 0, "hold_timeouts": 0}
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

def thread_enter():
    with lock:
        globals()["threads_alive"] += 1

def thread_exit():
    with lock:
        globals()["threads_alive"] -= 1

def hold_if_armed(direction):
    """Blocks while the hold gate is armed and the release file is absent.
    Returns 'held' (released normally), 'held_timeout' (bounded release) or
    'forwarded' (no fault this packet). Called AFTER recv, BEFORE send."""
    if direction != "req" or mode.split(":")[0] != "hold" or not armed():
        return "forwarded"
    dfile = mode.split(":")[1] if len(mode.split(":")) > 1 else ""
    ev(e="hold_begin", key=dfile)
    deadline = time.time() + HOLD_MAX_S
    while not os.path.exists(dfile):
        if time.time() >= deadline:
            bump("hold_timeouts", 1)
            ev(e="hold_timeout", key=dfile, max_s=HOLD_MAX_S)
            return "held_timeout"
        time.sleep(0.05)
    bump("holds", 1)
    ev(e="hold_released", key=dfile)
    return "held"

def handle(c):
    global threads_alive
    active_dirs = [0]  # live relay directions for this connection
    dir_lock = threading.Lock()
    def dir_done():
        with dir_lock:
            active_dirs[0] -= 1
            if active_dirs[0] == 0:
                bump("conns", -1)
    try:
        with lock:
            if counts["conns"] >= MAX_CONN:
                # NOTE: no bump()/ev() under the lock — both re-acquire it.
                c.close()
                locked_refuse = True
            else:
                counts["conns"] += 1
                counts["total_conns"] += 1
                locked_refuse = False
        if locked_refuse:
            ev(e="conn_refused_over_ceiling")
            return
        ev(e="conn")
        try:
            # 10s applies to the CONNECT only (proxy self-protection); the
            # relay sockets below are fully blocking so fault delays are
            # never truncated by the proxy.
            u = socket.create_connection(TARGET, timeout=10)
        except OSError:
            c.close()
            bump("conns", -1)
            ev(e="upstream_fail")
            return
        u.settimeout(None)
        c.settimeout(None)
        m = mode.split(":")
        kind = m[0]
        arg = int(m[1]) if len(m) > 1 and m[1].isdigit() else 0

        with lock:
            if threads_alive + 2 > MAX_THREADS:
                over = True
            else:
                globals()["threads_alive"] += 2
                over = False
        if over:
            ev(e="thread_ceiling")
            c.close(); u.close()
            bump("conns", -1)
            return

        def c2u():
            try:
                while True:
                    data = c.recv(65536)
                    if not data:
                        break
                    bump("req_bytes", len(data))
                    verdict = hold_if_armed("req")
                    ev(e="req_packet", bytes=len(data), act=verdict)
                    u.sendall(data)
                    if verdict in ("held", "held_timeout"):
                        ev(e="forward_ok_after_hold", bytes=len(data))
            except OSError:
                pass
            finally:
                try: u.shutdown(socket.SHUT_RDWR)
                except OSError: pass
                try: u.close()
                except OSError: pass
                thread_exit()
                dir_done()

        def u2c():
            try:
                while True:
                    data = u.recv(65536)
                    if not data:
                        break
                    bump("rsp_bytes", len(data))
                    if kind == "delay" and armed():
                        time.sleep(arg)
                        ev(e="delayed_rsp", bytes=len(data), s=arg)
                    if kind == "drop" and armed():
                        bump("drops", 1)
                        ev(e="dropped_rsp", bytes=len(data))
                        continue
                    c.sendall(data)
            except OSError:
                pass
            finally:
                try: c.shutdown(socket.SHUT_RDWR)
                except OSError: pass
                try: c.close()
                except OSError: pass
                thread_exit()
                dir_done()

        with dir_lock:
            active_dirs[0] = 2
        threading.Thread(target=c2u, daemon=True).start()
        threading.Thread(target=u2c, daemon=True).start()
    finally:
        pass

def stats():
    while True:
        time.sleep(5)
        with lock:
            c = dict(counts); c["threads_alive"] = threads_alive
        ev(e="stats", **c)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(LISTEN)
s.listen(64)
ev(e="listening", mode=mode, listen=LISTEN, target=TARGET, arm=armFile,
   hold_max_s=HOLD_MAX_S)
threading.Thread(target=stats, daemon=True).start()
while True:
    c, _ = s.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
