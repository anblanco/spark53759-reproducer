"""
Standalone regression test for SPARK-53759.

Same test as added to the Spark repo's test_worker.py, but runnable against
pip-installed PySpark for quick red/green TDD verification.

Usage:
    python scripts/test_regression.py --py 3.13          # should FAIL (red)
    python scripts/apply_fix.py --py 3.13 --apply
    python scripts/test_regression.py --py 3.13          # should PASS (green)
    python scripts/apply_fix.py --py 3.13 --revert
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _resolve_python import resolve_python

TEST_CODE = '''
import os, sys, unittest
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark import SparkConf, SparkContext
from pyspark.testing.utils import PySparkTestCase


class SimpleWorkerFlushTest(PySparkTestCase):
    """SPARK-53759: simple-worker path must flush before close."""

    @classmethod
    def conf(cls):
        _conf = super().conf()
        _conf.set("spark.python.use.daemon", "false")
        _conf.set("spark.ui.enabled", "false")
        return _conf

    def test_simple_worker_basic_operation(self):
        rdd = self.sc.parallelize(range(100), 1)
        result = rdd.map(lambda x: x * 2).collect()
        self.assertEqual(result, [x * 2 for x in range(100)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
'''


def main():
    parser = argparse.ArgumentParser(description="SPARK-53759 regression test")
    parser.add_argument("--py", default=None)
    args = parser.parse_args()

    python_exe = resolve_python(args.py)
    print(f"Python: {python_exe}")

    # Write test to temp file and run it
    import tempfile
    test_file = Path(tempfile.gettempdir()) / "spark53759_regression_test.py"
    test_file.write_text(TEST_CODE)

    try:
        env = os.environ.copy()
        env["PYSPARK_PYTHON"] = python_exe
        env["PYSPARK_DRIVER_PYTHON"] = python_exe

        result = subprocess.run(
            [python_exe, str(test_file)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

        # Show just the key output
        for line in (result.stdout + result.stderr).splitlines():
            if any(kw in line for kw in [
                "test_simple", "OK", "FAIL", "ERROR", "Ran ",
                "EOFException", "crashed",
            ]):
                print(f"  {line}")

        if result.returncode == 0:
            print("\nRESULT: GREEN (test passed)")
        else:
            print("\nRESULT: RED (test failed)")

        return result.returncode
    finally:
        test_file.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main())
