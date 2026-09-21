"""边界面板：连接池被 close 掉之后，正阻塞在等槽上的请求线程会怎样。

九格：一个等待者与三个等待者各自能不能收尾、醒过来时有没有偷偷发真请求、
旧队列关完之后是不是空的、pool_timeout 到点抛什么与等够多久、
正常释放一条连接能不能把等待者接上、close 之后再发请求的既有下场。

用法（仓库根目录）：timeout 300 env PYTHONPATH=src .venv/bin/python repro/close-wake-panel.py
"""
import queue
import socket
import sys
import threading
import time

from urllib3 import HTTPConnectionPool, PoolManager
from urllib3.exceptions import ClosedPoolError, EmptyPoolError

RESULTS = []
RESP = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"


def check(name, ok, note=""):
    RESULTS.append((name, bool(ok)))
    print("%-4s %-58s %s" % ("PASS" if ok else "FAIL", name, note))


class Server:
    """Answers one request per accepted connection; optional per-connection delay."""

    def __init__(self, delay=0.0):
        self.l = socket.socket()
        self.l.bind(("127.0.0.1", 0))
        self.l.listen(16)
        self.port = self.l.getsockname()[1]
        self.accepted = 0
        self.delay = delay
        self.stop = False
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        while not self.stop:
            try:
                c, _ = self.l.accept()
            except OSError:
                return
            self.accepted += 1
            with c:
                try:
                    c.recv(65536)
                    if self.delay:
                        time.sleep(self.delay)
                    c.sendall(RESP)
                except OSError:
                    pass

    def close(self):
        self.stop = True
        try:
            self.l.close()
        except OSError:
            pass


def waiter(pool, out, key, **kw):
    try:
        r = pool.urlopen("GET", "/", **kw)
        out[key] = ("ok", r.status)
    except BaseException as e:  # noqa: BLE001
        out[key] = ("raise", type(e).__name__)


# ------------------------------------------------------------------ W1..W4
srv = Server()
pool = HTTPConnectionPool("127.0.0.1", srv.port, maxsize=1, block=True, timeout=5)
held = pool.urlopen("GET", "/", preload_content=False, release_conn=False)
time.sleep(0.2)
out = {}
n_conns_before = pool.num_connections
accepted_before = srv.accepted
old_queue = pool.pool
t1 = threading.Thread(target=waiter, args=(pool, out, "w1"), kwargs={"pool_timeout": None}, daemon=True)
t1.start()
time.sleep(0.5)
check("W0 precondition: waiter is parked in the queue", out.get("w1") is None, repr(out.get("w1")))
t0 = time.monotonic()
pool.close()
t1.join(timeout=2.0)
el = time.monotonic() - t0
w1_ok = (not t1.is_alive()) and out.get("w1") == ("raise", "ClosedPoolError")
check("W1 blocked waiter ends in ClosedPoolError after close", w1_ok, "alive=%s result=%r took=%.2fs" % (t1.is_alive(), out.get("w1"), el))

same_conns = pool.num_connections == n_conns_before and srv.accepted == accepted_before
check("W2 woken waiter opened no connection", same_conns, "num_connections %d->%d, accepted %d->%d" % (n_conns_before, pool.num_connections, accepted_before, srv.accepted))

try:
    leftover = old_queue.get(block=False)
    qnote = "old queue still holds %r" % (leftover,)
    empty_after_close = False
except queue.Empty:
    empty_after_close = True
    qnote = "empty"
check("W3 old queue ends up empty (既有断言钉着)", empty_after_close, qnote)

# W4: several waiters on a maxsize=1 pool must all be retired
srv4 = Server()
p4 = HTTPConnectionPool("127.0.0.1", srv4.port, maxsize=1, block=True, timeout=5)
p4.urlopen("GET", "/", preload_content=False, release_conn=False)
time.sleep(0.2)
res4 = {}
threads = []
for i in range(3):
    t = threading.Thread(target=waiter, args=(p4, res4, "t%d" % i), kwargs={"pool_timeout": None}, daemon=True)
    t.start()
    threads.append(t)
time.sleep(0.5)
p4.close()
for t in threads:
    t.join(timeout=3.0)
alive = sum(1 for t in threads if t.is_alive())
w4_ok = alive == 0 and all(res4.get("t%d" % i) == ("raise", "ClosedPoolError") for i in range(3))
check("W4 all 3 blocked waiters retired by one close", w4_ok, "alive=%d results=%r" % (alive, res4))

# ------------------------------------------------------------------ W5 pool_timeout honoured
srv5 = Server()
p5 = HTTPConnectionPool("127.0.0.1", srv5.port, maxsize=1, block=True, timeout=5)
p5.urlopen("GET", "/", preload_content=False, release_conn=False)
time.sleep(0.2)
out5 = {}
t5 = threading.Thread(target=waiter, args=(p5, out5, "w5"), kwargs={"pool_timeout": 0.4}, daemon=True)
t5.start()
t5.join(timeout=5.0)
w5_ok = out5.get("w5") == ("raise", "EmptyPoolError") and not t5.is_alive()
check("W5 pool_timeout=0.4 -> EmptyPoolError (guard)", w5_ok, repr(out5.get("w5")))
p5.close()

# ------------------------------------------------------------------ W6 release still wakes a waiter
srv6 = Server()
p6 = HTTPConnectionPool("127.0.0.1", srv6.port, maxsize=1, block=True, timeout=5)
held6 = p6.urlopen("GET", "/", preload_content=False, release_conn=False)
time.sleep(0.2)
out6 = {}
t6 = threading.Thread(target=waiter, args=(p6, out6, "w6"), kwargs={"pool_timeout": 5}, daemon=True)
t6.start()
time.sleep(0.4)
held6.release_conn()
t6.join(timeout=6.0)
check("W6 a released connection still serves the waiter", out6.get("w6") == ("ok", 200), repr(out6.get("w6")))
p6.close()

# ------------------------------------------------------------------ W7 deadline not cut short
srv7 = Server()
p7 = HTTPConnectionPool("127.0.0.1", srv7.port, maxsize=1, block=True, timeout=5)
p7.urlopen("GET", "/", preload_content=False, release_conn=False)
time.sleep(0.2)
out7 = {}
t7 = threading.Thread(target=waiter, args=(p7, out7, "w7"), kwargs={"pool_timeout": 1.5}, daemon=True)
t0 = time.monotonic()
t7.start()
t7.join(timeout=6.0)
el7 = time.monotonic() - t0
check(
    "W7 pool_timeout=1.5 waits the full 1.5s",
    out7.get("w7") == ("raise", "EmptyPoolError") and el7 >= 1.4,
    "result=%r waited=%.2fs" % (out7.get("w7"), el7),
)
p7.close()

# ------------------------------------------------------------------ W8 PoolManager.clear() retires waiters
# NOTE: no PoolManager.clear() cell on purpose.  PoolManager builds its container
# as RecentlyUsedContainer(num_pools) without a dispose_func
# (src/urllib3/poolmanager.py:225), so clear()/__exit__ only drop pool references
# and rely on weakref.finalize; its docstring even says "This will not affect
# in-flight connections".  Retiring a waiter through the manager is therefore not
# a contract this panel may assert.

# ------------------------------------------------------------------ W9 pool stays closed
try:
    pool._get_conn()
    w9 = False
    note9 = "no raise"
except ClosedPoolError:
    w9 = True
    note9 = "ClosedPoolError"
except BaseException as e:  # noqa: BLE001
    w9 = False
    note9 = type(e).__name__
check("W9 later _get_conn still raises ClosedPoolError (guard)", w9, note9)

for s in (srv, srv4, srv5, srv6, srv7):
    s.close()

print("\n%d/%d cells pass" % (sum(1 for _, ok in RESULTS if ok), len(RESULTS)))
sys.exit(0)
