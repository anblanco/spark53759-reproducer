"""
Minimal reproducer for SPARK-53759.

On Windows: crashes by default (daemon is always disabled).
On Linux/macOS: set spark.python.use.daemon=false to trigger.

Requires: pip install pyspark (tested 3.5.x, 4.0.x)
Requires: Java 17+ on PATH
"""
import subprocess
import sys
import os

# Check Java is available before Spark tries to start
try:
    subprocess.run(["java", "-version"], capture_output=True, timeout=10, check=True)
except (FileNotFoundError, subprocess.CalledProcessError):
    print("ERROR: Java not found. Install Java 17+ and add it to PATH.")
    sys.exit(1)

from pyspark.sql import SparkSession

print(f"Python:   {sys.version}")
print(f"Platform: {sys.platform} ({os.name})")

# Ensure Spark uses the correct Python interpreter (critical on Windows)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SPARK-53759-repro") \
    .config("spark.ui.enabled", "false") \
    .config("spark.python.use.daemon", "false") \
    .getOrCreate()

import py4j
print(f"Spark:    {spark.version}")
print(f"py4j:     {py4j.__version__}")
print()

# JVM-only — always works
count = spark.range(10).count()
print(f"spark.range(10).count() = {count}  [JVM-only: OK]")

# Python worker — crashes on simple-worker path
try:
    df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
    print(f"createDataFrame().count() = {df.count()}  [worker: OK]")
    print("\nRESULT: PASS")
except Exception as e:
    msg = str(e)
    if "crashed" in msg or "EOFException" in msg or "failed to connect back" in msg or "SocketTimeoutException" in msg:
        print("createDataFrame() CRASHED  [worker: FAIL]")
        print("\nRESULT: FAIL — SPARK-53759 confirmed")
    else:
        print(f"createDataFrame() ERROR: {type(e).__name__}")
        print(f"\nRESULT: FAIL — unexpected: {msg[:300]}")
finally:
    spark.stop()
