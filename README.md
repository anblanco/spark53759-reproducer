# SPARK-53759: createSimpleWorker broken with Python 3.12+

Reproducer, root cause analysis, and verified fix for
[SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759).

## Root cause

On Python 3.12+, the simple-worker (non-daemon) path crashes with
`EOFException` because the worker socket is not explicitly closed before the
process exits. Windows always uses this path; Linux/macOS use it when
`spark.python.use.daemon=false`.

Changed GC finalization ordering in CPython 3.12+
([gh-97922](https://github.com/python/cpython/issues/97922)) can close the
underlying raw socket before `BufferedRWPair.__del__` runs. When
`BufferedRWPair` finally tries to flush its write buffer during cleanup, the
socket is already dead — buffered task results are lost and the JVM sees
`EOFException`.

This affects **all** Python worker files, not just `worker.py`. The Python Data
Source API workers (introduced in Spark 4.0) have the same bare
`main(sock_file, sock_file)` without explicit close, and are equally vulnerable.

## The fix

Wrap `main()` in `try/finally` with an explicit `sock_file.close()` in every
worker file's `__main__` block:

```python
    try:
        main(sock_file, sock_file)
    finally:
        sock_file.close()
```

`close()` is semantically correct — it explicitly tears down the socket before
GC can interfere, and internally calls `flush()`. This matches how
[PR #54458](https://github.com/apache/spark/pull/54458) solved it on master
via a context manager with `close()` in the `finally` block.

**Note:** The JVM loads worker files from `pyspark/python/lib/pyspark.zip`, not
from `site-packages`. To test the fix locally, use `scripts/apply_fix.py` which
patches all worker files in the zip.

## Upstream fix

Fixed on master in [PR #54458](https://github.com/apache/spark/pull/54458)
(merged Feb 26, 2026) via a context manager refactoring. Backport for stable
branches proposed in [PR #55201](https://github.com/apache/spark/pull/55201).

The fix is in `pyspark==4.2.0.dev3` but **not in any stable release** yet.

| PySpark | Python 3.13 (Windows) | Fix present? |
|---------|----------------------|-------------|
| 3.5.5 | **FAIL** | No |
| 4.0.0 | **FAIL** | No |
| 4.1.1 | **FAIL** | No |
| 4.2.0.dev1 | **FAIL** | No |
| 4.2.0.dev2 | **FAIL** | No |
| 4.2.0.dev3 | PASS | **Yes** |

## Verification (manual patch)

| Platform | Python | Unpatched | Patched |
|----------|--------|-----------|---------|
| Windows 11 | 3.11.9 | PASS | PASS (harmless) |
| Windows 11 | 3.12.10 | **FAIL** | PASS |
| Windows 11 | 3.13.3 | **FAIL** | PASS |
| Linux (Ubuntu 24.04) | 3.12.3 | **FAIL** | PASS |

## Prerequisites

- **Python 3.12+** (the bug doesn't manifest on 3.11)
- **Java 17+** on PATH (`java -version` should work)
- **pip** (to install pyspark)
- **pyarrow** (required for Data Source API tests on PySpark 4.0+)

## Quick reproduction

```bash
pip install pyspark
python reproduce.py
```

## Running the tests

```bash
pip install pyspark pytest pyarrow
pytest test_spark53759.py -v
```

Expected on Python 3.12+ with `daemon=false`:
- `test_spark_range` and `test_rdd_collect` PASS (no worker needed)
- `test_rdd_map`, `test_create_dataframe`, `test_udf` **FAIL** (worker crashes)
- `test_datasource_read` **FAIL** on PySpark 4.0+ (data source worker crashes),
  SKIP on PySpark 3.x (DataSource API not available)

## Full version matrix

Uses [nox](https://nox.thea.codes/) to test across Python x PySpark combinations:

```bash
pip install nox
nox              # full matrix (3.11, 3.12, 3.13 x pyspark 3.5.5, 4.0.0)
nox -p 3.13      # single Python version
```

## Applying the fix locally

```bash
python scripts/apply_fix.py --apply     # patch pyspark.zip for the current interpreter
pytest test_spark53759.py -v             # all tests pass
python scripts/apply_fix.py --revert    # restore original
```

## Repo contents

| File | Description |
|------|-------------|
| `reproduce.py` | Minimal one-file reproducer |
| `test_spark53759.py` | Pytest suite: 6 tests from JVM-only to worker-dependent |
| `noxfile.py` | Test matrix across Python x PySpark versions via nox |
| `scripts/apply_fix.py` | Apply/revert the close fix to all workers in pyspark.zip |
| `scripts/diagnose.py` | Pure-Python mock of createSimpleWorker |
| `scripts/patch_zip.py` | Instrument worker.py inside pyspark.zip with logging |
| `scripts/wsl_patch_and_test.py` | Apply fix and test inside WSL Linux |
| `BUG_REPORT.md` | Full root cause analysis with citations |
| `INVESTIGATION_LOG.md` | What was tested and ruled out |

## Contributing upstream

Backport PR: `[SPARK-53759][PYTHON][4.1] Fix missing close in simple-worker path`

See [BUG_REPORT.md](BUG_REPORT.md) for the full analysis, references, and proposed change.
