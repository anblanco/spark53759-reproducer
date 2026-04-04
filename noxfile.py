"""
SPARK-53759 test matrix via nox.

Usage:
    nox                              # full matrix (3 Pythons x 2 PySpark versions)
    nox -p 3.13                      # single Python version
    nox -s "test(pyspark='3.5.5')"   # single PySpark version
"""
import nox

nox.options.error_on_missing_interpreters = False


@nox.session(python=["3.11", "3.12", "3.13"])
@nox.parametrize("pyspark", ["3.5.5", "4.0.0"])
def test(session, pyspark):
    """Run the SPARK-53759 reproducer test suite."""
    session.install(f"pyspark=={pyspark}", "pytest")
    session.run("pytest", "test_spark53759.py", "-v", *session.posargs)
