# SPARK-53759: createSimpleWorker broken with Python 3.12+

## Summary

The `createSimpleWorker` codepath in PySpark crashes deterministically with
Python 3.12 or later. Windows **always** uses this path because `os.fork()`
is unavailable. On Linux, setting `spark.python.use.daemon=false` triggers
the same failure.

**Root cause identified**: `worker.py`'s `__main__` block is missing an explicit
`sock_file.flush()` after `main()` returns. The daemon path (`daemon.py`) has
this flush in a `finally` block; the simple-worker path does not. On Python 3.12+,
changed GC/finalization ordering causes the socket to close before the
`BufferedRWPair` write buffer is flushed, so buffered task results are lost.
The JVM sees `EOFException`.

**One-line fix verified** across Python 3.11, 3.12, 3.13 on Windows and Linux.

**Upstream fix**: [PR #54458](https://github.com/apache/spark/pull/54458) (merged
Feb 26, 2026). Available in `pyspark==4.2.0.dev3`. Not in any stable release yet
(3.5.8, 4.0.2, 4.1.1 are all affected).

## The Fix

In [`python/pyspark/worker.py`](https://github.com/apache/spark/blob/master/python/pyspark/worker.py),
the `__main__` block currently ends with:

```python
    main(sock_file, sock_file)
```

Replace with:

```python
    try:
        main(sock_file, sock_file)
    finally:
        try:
            sock_file.flush()
        except Exception:
            pass
```

This mirrors the existing pattern in [`daemon.py`'s `worker()` function](https://github.com/apache/spark/blob/master/python/pyspark/daemon.py#L91),
which already has `outfile.flush()` in a `finally` block (line ~95).

## Verification Matrix

Fix tested by patching `pyspark/worker.py` inside `pyspark.zip` (the bundled
copy the JVM loads via PYTHONPATH).

| Platform | Python | Unpatched | Patched |
|----------|--------|-----------|---------|
| Windows 11 (10.0.26200) | 3.11.9 | PASS | PASS (harmless) |
| Windows 11 (10.0.26200) | 3.12.10 | **FAIL** | PASS |
| Windows 11 (10.0.26200) | 3.13.3 | **FAIL** | PASS |
| Linux (Ubuntu 24.04 WSL) | 3.12.3 | **FAIL** | PASS |

All 5 test cases (spark_range, rdd_collect, rdd_map, createDataFrame, UDF)
pass on Python 3.13.3 Windows with the fix applied. PySpark 4.1.1, OpenJDK 17.

## Root Cause Analysis

### The two codepaths

`PythonWorkerFactory.scala` has two worker launch strategies:

```scala
if (useDaemon) {
  createThroughDaemon()     // POSIX: daemon.py + os.fork()
} else {
  createSimpleWorker(...)   // Windows: subprocess + TCP socket
}
```

### The daemon path has the flush

[`daemon.py`'s `worker()` function](https://github.com/apache/spark/blob/master/python/pyspark/daemon.py#L91)
wraps `worker_main()` in try/finally with an explicit flush:

```python
try:
    worker_main(infile, outfile)
except SystemExit as exc:
    exit_code = compute_real_exit_code(exc.code)
finally:
    try:
        outfile.flush()    # <-- ensures all buffered data is sent
    except Exception:
        ...
```

### The simple-worker path does NOT

[`worker.py`'s `__main__` block](https://github.com/apache/spark/blob/master/python/pyspark/worker.py)
calls `main()` without a flush:

```python
if __name__ == "__main__":
    ...
    (sock_file, _) = local_connect_and_auth(conn_info, auth_secret)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)   # <-- no flush after this; process exits
```

### Why it matters: buffered I/O

`local_connect_and_auth()` creates the socket file via
[`sock.makefile("rwb", 65536)`](https://docs.python.org/3/library/socket.html#socket.socket.makefile),
which returns a `BufferedRWPair` with a 64KB write buffer. When `main()` writes
task results, the data goes into this buffer. It only reaches the socket when:

1. The buffer fills up (64KB), or
2. `.flush()` is called explicitly, or
3. `BufferedRWPair.__del__()` flushes during object finalization

### What changed in Python 3.12

CPython 3.12 moved garbage collection to the eval breaker mechanism
([gh-97922](https://github.com/python/cpython/issues/97922)):

> "The Garbage Collector now runs only on the eval breaker mechanism of the
> Python bytecode evaluation loop instead of object allocations."
>
> — [What's New in Python 3.12](https://docs.python.org/3.12/whatsnew/3.12.html)

This changed the order in which objects are finalized during interpreter
shutdown. On Python 3.11, the `BufferedRWPair.__del__` happened to run before
the socket was torn down, so the implicit flush succeeded. On Python 3.12+, the
socket closes first, and `BufferedRWPair.__del__` fails with:

```
OSError: [WinError 10038] An operation was attempted on something that is not a socket
```

### Why relying on `__del__` is the bug

The Python documentation explicitly warns against this:

> "It is not guaranteed that `__del__()` methods are called for objects that
> still exist when the interpreter exits."
>
> — [Python Data Model: `object.__del__`](https://docs.python.org/3/reference/datamodel.html#object.__del__)

The daemon path correctly avoids this by flushing in a `finally` block. The
simple-worker path relies on the implicit flush in `__del__`, which is not
guaranteed to work.

### Why the fix belongs in PySpark

1. **CPython didn't break any contract.** Finalization order was never
   guaranteed. The daemon path already handles this correctly.
2. **The daemon path has the fix.** `daemon.py` line ~95 has
   `outfile.flush()` in a `finally` block. The simple-worker path is simply
   missing the same pattern.
3. **The bug is cross-platform.** It reproduces on Linux with
   `spark.python.use.daemon=false` (Python 3.12.3, Ubuntu 24.04).

## Affected Versions

| Python  | PySpark | Result |
|---------|---------|--------|
| 3.11.9  | 3.5.5   | PASS |
| 3.11.9  | 4.0.0   | PASS |
| 3.12.10 | 3.5.5   | **FAIL** |
| 3.12.10 | 4.0.0   | **FAIL** |
| 3.13.3  | 3.5.5   | **FAIL** |
| 3.13.3  | 4.0.0   | **FAIL** |

Not PySpark-version-dependent — same crash on 3.4.4, 3.5.5, 4.0.0.
Not py4j-version-dependent — same crash on 0.10.9.7 and 0.10.9.9.

## Reproduction

### Minimal reproducer

```python
from pyspark.sql import SparkSession
import sys, os

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SPARK-53759-repro") \
    .config("spark.ui.enabled", "false") \
    .config("spark.python.use.daemon", "false") \
    .getOrCreate()

# JVM-only — always works
assert spark.range(10).count() == 10

# Requires Python worker — crashes on Python 3.12+
df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
print(df.count())  # EOFException on 3.12+

spark.stop()
```

### Steps to reproduce

1. Python 3.12+ and Java 17+ on PATH
2. `pip install pyspark`
3. `python reproduce.py`
4. On Windows: crashes by default
5. On Linux: set `spark.python.use.daemon=false` to trigger

### Error signature

```
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
Caused by: java.io.EOFException
    at java.base/java.io.DataInputStream.readInt(DataInputStream.java:386)
    at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala)
```

## Investigation Notes

### Important: pyspark.zip

The JVM loads `worker.py` from `pyspark/python/lib/pyspark.zip`, NOT from
`site-packages/pyspark/worker.py`. Patches to the site-packages copy have no
effect. The PYTHONPATH set by `PythonWorkerFactory.scala` includes:

```
<spark_home>/python
<spark_home>/python/lib/pyspark.zip
<spark_home>/python/lib/py4j-*.zip
```

### Worker lifecycle (confirmed via instrumentation)

When instrumented with file-based logging inside the zip-bundled `worker.py`:

```
worker.py loaded, Python 3.13.3
calling connect, port=61830
connected, fileno=496
wrote PID
flushed, entering main()
main() returned normally    # <-- main() COMPLETES; worker doesn't crash
```

The worker completes its work successfully. The data loss occurs during process
exit when `BufferedRWPair.__del__` fails to flush the write buffer because the
underlying socket has already been closed by the OS.

## Key Source Files

- [`python/pyspark/worker.py`](https://github.com/apache/spark/blob/master/python/pyspark/worker.py) — `__main__` block (missing flush)
- [`python/pyspark/daemon.py`](https://github.com/apache/spark/blob/master/python/pyspark/daemon.py) — `worker()` function (has flush in finally)
- [`python/pyspark/util.py`](https://github.com/apache/spark/blob/master/python/pyspark/util.py) — `local_connect_and_auth()` (creates `BufferedRWPair` via `sock.makefile`)
- [`core/.../PythonWorkerFactory.scala`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala) — `createSimpleWorker()` (JVM side)

## References

- [Python Data Model: `object.__del__`](https://docs.python.org/3/reference/datamodel.html#object.__del__) — "It is not guaranteed that `__del__()` methods are called for objects that still exist when the interpreter exits."
- [What's New in Python 3.12](https://docs.python.org/3.12/whatsnew/3.12.html) — GC moved to eval breaker ([gh-97922](https://github.com/python/cpython/issues/97922))
- [Python `socket.makefile()`](https://docs.python.org/3/library/socket.html#socket.socket.makefile) — buffering semantics
- [ESRI Knowledge Base](https://support.esri.com/en-us/knowledge-base/pyspark-crashes-with-python-3-12-on-windows-000039267) — confirms the issue in production

## Workarounds (until fix reaches a stable release)

1. **Install pyspark 4.2.0.dev3** — `pip install --pre pyspark==4.2.0.dev3` (fix included)
2. **Use Python 3.11** — confirmed working across all PySpark versions
3. **Use WSL/Linux** — the daemon path works on all Python versions
4. **Patch pyspark.zip locally** — `python scripts/apply_fix.py --apply`
