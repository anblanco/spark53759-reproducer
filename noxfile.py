"""
SPARK-53759 test matrix via nox.

Usage:
    nox                              # full matrix
    nox -p 3.13                      # single Python version
    nox -s "test(pyspark='3.5.5')"   # single PySpark version

Expected results:
    Python 3.11 + any version:       all pass (bug not present on 3.11)
    Python 3.12/3.13 + stable:       2 pass, 3 fail (worker crash)
    Python 3.12/3.13 + 4.2.0.dev3:   all pass (fix from PR #54458)
"""
import nox

nox.options.error_on_missing_interpreters = False

PYSPARK_STABLE = ["3.5.5", "4.0.0", "4.1.1"]
PYSPARK_PRERELEASE = ["4.2.0.dev1", "4.2.0.dev2", "4.2.0.dev3"]


@nox.session(python=["3.11", "3.12", "3.13"])
@nox.parametrize("pyspark", PYSPARK_STABLE + PYSPARK_PRERELEASE)
def test(session, pyspark):
    """Run the SPARK-53759 reproducer test suite."""
    if ".dev" in pyspark:
        session.install("--pre", f"pyspark=={pyspark}", "pytest")
    else:
        session.install(f"pyspark=={pyspark}", "pytest")
    session.run("pytest", "test_spark53759.py", "-v", *session.posargs)
