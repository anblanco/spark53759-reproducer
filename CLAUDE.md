# SPARK-53759 Reproducer

## Purpose

Evidence package for [SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759):
reproducer, root cause analysis, and verified fix. Target audience is PySpark
maintainers reviewing the upstream PR.

## Key finding

The simple-worker path is missing `sock_file.flush()` before close/exit.
On upstream master, this is in `worker_util.py`'s `get_sock_file_to_executor()`.
On pip-installed PySpark 3.5.5/4.0.0, the code is inline in `worker.py`'s
`__main__` block. The daemon path (`daemon.py`) already has this flush.
On Python 3.12+, changed finalization ordering causes buffered data loss.
One-line fix verified on Windows and Linux.

## Repo structure

```
reproduce.py              # minimal reproducer (pip install pyspark && python reproduce.py)
test_spark53759.py        # pytest suite (5 tests: 2 baseline, 3 worker-dependent)
noxfile.py                # test matrix via nox (Python x PySpark versions)
scripts/
  apply_fix.py            # apply/revert the flush fix to pyspark.zip
  diagnose.py             # pure-Python mock of createSimpleWorker
  patch_zip.py            # instrument worker.py inside pyspark.zip
  wsl_patch_and_test.py   # test the fix on Linux via WSL
  test_regression.py      # standalone red/green regression test
BUG_REPORT.md             # full root cause analysis with citations
INVESTIGATION_LOG.md      # what was tested and ruled out
TASK.md                   # original task description
```

## Running commands

```bash
# Quick reproduction
pip install pyspark
python reproduce.py

# Test suite
pip install pyspark pytest
pytest test_spark53759.py -v

# Full version matrix
pip install nox
nox                       # all combos
nox -p 3.13               # single Python version

# Apply/test the fix
python scripts/apply_fix.py --py 3.13 --apply
pytest test_spark53759.py -v
python scripts/apply_fix.py --py 3.13 --revert
```

## Important notes

- JVM loads worker.py from `pyspark/python/lib/pyspark.zip`, NOT site-packages.
  Patches to site-packages have no effect. Use `scripts/apply_fix.py`.
- `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` must be set on Windows.
  Scripts handle this via `os.environ.setdefault()`.
- `winutils.exe` / `HADOOP_HOME` is NOT needed for `local[1]` mode.
