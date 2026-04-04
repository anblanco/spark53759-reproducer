"""
Apply the SPARK-53759 flush fix and test — designed to run inside WSL.

Confirms the bug reproduces on Linux (not Windows-specific) and that the
fix works cross-platform. Tested with Python 3.12.3 on Ubuntu 24.04 WSL.

Usage (from WSL):
    /tmp/sparktest/bin/python scripts/wsl_patch_and_test.py --apply
    /tmp/sparktest/bin/python scripts/wsl_patch_and_test.py --test
    /tmp/sparktest/bin/python scripts/wsl_patch_and_test.py --revert
"""
import argparse
import os
import shutil
import sys
import zipfile


def find_zip():
    import pyspark
    return os.path.join(os.path.dirname(pyspark.__file__), "python", "lib", "pyspark.zip")


def apply_fix():
    zp = find_zip()
    bk = zp + ".bak"
    if not os.path.exists(bk):
        shutil.copy2(zp, bk)
        print(f"Backed up: {bk}")

    with zipfile.ZipFile(zp) as z:
        names = z.namelist()
        contents = {n: z.read(n) for n in names}

    orig = contents["pyspark/worker.py"].decode()
    patched = orig.replace(
        "    main(sock_file, sock_file)",
        "    try:\n"
        "        main(sock_file, sock_file)\n"
        "    finally:\n"
        "        try:\n"
        "            sock_file.flush()\n"
        "        except Exception:\n"
        "            pass",
    )
    if patched == orig:
        print("Could not find patch target (already patched?)")
        return

    contents["pyspark/worker.py"] = patched.encode()
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for n in names:
            z.writestr(n, contents[n])
    print(f"Patched: {zp}")


def revert():
    zp = find_zip()
    bk = zp + ".bak"
    if os.path.exists(bk):
        shutil.copy2(bk, zp)
        print("Reverted")
    else:
        print("No backup")


def test():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("SPARK-53759-wsl")
        .config("spark.ui.enabled", "false")
        .config("spark.python.use.daemon", "false")
        .getOrCreate()
    )

    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {sys.platform}")
    print(f"Spark: {spark.version}")

    count = spark.range(10).count()
    print(f"spark.range(10).count() = {count}")

    try:
        df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
        print(f"createDataFrame().count() = {df.count()}")
        print("RESULT: PASS")
    except Exception as e:
        msg = str(e)
        if "crashed" in msg or "EOFException" in msg:
            print("RESULT: FAIL — SPARK-53759 confirmed")
        else:
            print(f"RESULT: FAIL — {type(e).__name__}")
    finally:
        spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    p.add_argument("--revert", action="store_true")
    p.add_argument("--test", action="store_true")
    a = p.parse_args()

    if a.apply:
        apply_fix()
    elif a.revert:
        revert()
    elif a.test:
        test()
