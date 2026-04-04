# SPARK-53759: createSimpleWorker broken with Python 3.12+

Reproducer, root cause analysis, and verified fix for
[SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759).

## Root cause

`worker.py`'s `__main__` block is missing an explicit `sock_file.flush()` after
`main()` returns. The daemon path (`daemon.py`) has this flush in a `finally`
block; the simple-worker path does not.

On Python 3.12+, changed GC/finalization ordering
([gh-97922](https://github.com/python/cpython/issues/97922)) causes the socket
to close before `BufferedRWPair.__del__` can flush the write buffer. Buffered
task results are lost. The JVM sees `EOFException`.

## The fix (one line)

In `python/pyspark/worker.py`, replace:

```python
    main(sock_file, sock_file)
```

with:

```python
    try:
        main(sock_file, sock_file)
    finally:
        try:
            sock_file.flush()
        except Exception:
            pass
```

This mirrors [`daemon.py`'s `worker()` function](https://github.com/apache/spark/blob/master/python/pyspark/daemon.py#L91)
which already has `outfile.flush()` in a `finally` block.

**Note:** The JVM loads `worker.py` from `pyspark/python/lib/pyspark.zip`, not
from `site-packages`. To test the fix locally, use `scripts/apply_fix.py` which
patches the zip-bundled copy.

## Verification

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

## Quick reproduction

```bash
pip install pyspark
python reproduce.py
```

## Running the tests

```bash
pip install pyspark pytest
pytest test_spark53759.py -v
```

Expected on Python 3.12+ with `daemon=false`:
- `test_spark_range` and `test_rdd_collect` PASS (no worker needed)
- `test_rdd_map`, `test_create_dataframe`, `test_udf` **FAIL** (worker crashes)

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
pytest test_spark53759.py -v             # all 5 pass
python scripts/apply_fix.py --revert    # restore original
```

## Repo contents

| File | Description |
|------|-------------|
| `reproduce.py` | Minimal one-file reproducer |
| `test_spark53759.py` | Pytest suite: 5 tests from JVM-only to worker-dependent |
| `noxfile.py` | Test matrix across Python x PySpark versions via nox |
| `scripts/apply_fix.py` | Apply/revert the flush fix to pyspark.zip |
| `scripts/diagnose.py` | Pure-Python mock of createSimpleWorker |
| `scripts/patch_zip.py` | Instrument worker.py inside pyspark.zip with logging |
| `scripts/wsl_patch_and_test.py` | Apply fix and test inside WSL Linux |
| `BUG_REPORT.md` | Full root cause analysis with citations |
| `INVESTIGATION_LOG.md` | What was tested and ruled out |

## Contributing upstream

PR format for apache/spark: `[SPARK-53759][PYTHON] Fix missing flush in simple-worker path`

See [BUG_REPORT.md](BUG_REPORT.md) for the full analysis, references, and proposed change.
