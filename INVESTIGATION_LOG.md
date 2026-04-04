# SPARK-53759 Investigation Log

## Status: ROOT CAUSE IDENTIFIED + FIX VERIFIED

---

## Root Cause

The worker's `__main__` block in `pyspark/worker.py` calls `main(sock_file, sock_file)`
as the last statement. When `main()` returns, the process exits, and Python 3.12+
runs `BufferedRWPair.__del__()` on the socket file object during interpreter shutdown.
On Windows, this triggers `OSError: [WinError 10038] An operation was attempted on
something that is not a socket` because the underlying socket has already been closed
by the OS during process teardown.

The critical issue: `main()` writes results to `sock_file` but **never flushes** at
the end. The data sits in the `BufferedRWPair` write buffer. When Python 3.12+ runs
the `BufferedRWPair.__del__` during shutdown, it tries to flush the buffer to the
already-closed socket, causing the write to fail silently. The JVM, still reading
from the socket, sees `EOFException` because the buffered data was never actually
sent.

On Python 3.11, the cleanup ordering was different — the buffer was flushed before
the socket was torn down. Python 3.12 changed GC/finalization behavior (PEP 683
immortal objects, eval-breaker GC), which altered the cleanup order so the socket
closes before the buffer flushes.

## The Fix (one line)

Wrap `main()` in try/finally with an explicit flush:

```python
# Before (broken on Python 3.12+ Windows):
    main(sock_file, sock_file)

# After (fixed):
    try:
        main(sock_file, sock_file)
    finally:
        try:
            sock_file.flush()
        except Exception:
            pass
```

This ensures all buffered data is written to the socket before the process exits,
regardless of Python's finalization order.

## Verification

- Python 3.13.3 WITHOUT fix: 3/5 tests fail (rdd_map, createDataFrame, udf)
- Python 3.13.3 WITH fix: **5/5 tests pass**
- Python 3.11.9 (no fix needed): 5/5 tests pass

## Important Discovery: pyspark.zip

The JVM loads `worker.py` from **`pyspark/python/lib/pyspark.zip`**, NOT from
`site-packages/pyspark/worker.py`. Patches to the site-packages copy have no effect.
This delayed the investigation significantly — all earlier patches (fdopen, timeout,
keep_socket) were applied to the wrong file.

---

## Full Investigation Timeline

### Phase 1: Version Matrix
- Confirmed Python 3.11 works, 3.12+ crashes across PySpark 3.4.4/3.5.5/4.0.0
- Confirmed not PySpark-version-dependent, not py4j-version-dependent
- PySpark code byte-identical across Python versions

### Phase 2: Pure-Python Harness (diagnose.py)
- Worker completes full lifecycle in pure-Python mock (both 3.11 and 3.13)
- Python 3.13 shows `BufferedRWPair` / `WinError 10038` during cleanup
- Python 3.11 does not show this error
- Crash only occurs under real JVM orchestration

### Phase 3: Worker-Side Patches (WRONG FILE)
- Applied 3 patches to `site-packages/pyspark/worker.py`:
  - `os.fdopen(os.dup())` — no effect
  - `sock.settimeout(None)` — no effect
  - Keep socket reference alive — no effect
- **All failed because JVM loads from pyspark.zip, not site-packages**

### Phase 4: Worker Instrumentation (WRONG FILE)
- Added module-level logging to `site-packages/pyspark/worker.py`
- Cleared `.pyc` cache
- Log files never created
- **Revealed that JVM was loading a different copy of worker.py**

### Phase 5: Discovery of pyspark.zip
- Found `pyspark/python/lib/pyspark.zip` (3.5MB) containing bundled Python files
- JVM sets PYTHONPATH to include this zip, which takes precedence
- Created `patch_zip_worker.py` to patch the zip-bundled copy

### Phase 6: Zip-Based Instrumentation
- Fixed bug in logging code (used `os` before import, should be `_os_early`)
- Successfully captured worker logs from zip-bundled worker.py
- **Key finding: `main()` returned normally!** Worker didn't crash — it completed

### Phase 7: Fix Isolation
- Full instrumented version (with logging + flush) passed all tests
- Tested components individually:
  - `_sock` rename alone: FAIL
  - `try/finally flush` alone: **PASS**
  - Both together: **PASS**
- **The fix is: explicit flush after main()**

---

## Theories Ruled Out

| # | Theory | Why ruled out |
|---|--------|--------------|
| 1 | GC timing changes close socket prematurely | Fix is about flush, not socket lifetime |
| 2 | `sock.makefile()` vs `os.fdopen(os.dup())` | Not the cause (patched wrong file anyway) |
| 3 | Socket timeout leaks into task reads | Not the cause |
| 4 | Worker crashes during import | Worker imports fine; `main()` runs to completion |
| 5 | Worker crashes during `main()` | `main()` returns normally; issue is in cleanup |
| 6 | Worker dies before `main()` entry | False — `main()` is entered and completes |
