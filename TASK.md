# SPARK-53759: Windows Reproducer & Bug Report

## What you are

You are Claude Code running on a Windows machine. Do NOT assume any Python
or Java versions are pre-installed. Download everything from official
upstream sources:

| Component  | Official Source | Method |
|------------|----------------|--------|
| Python     | https://www.python.org/ftp/python/ | Windows installer (.exe), silent install |
| Java (JDK) | https://www.oracle.com/java/technologies/downloads/ | Oracle JDK installer, silent install |
| PySpark    | https://pypi.org/project/pyspark/ | `pip install pyspark` (official Apache Spark Python package) |
| py4j       | Bundled inside PySpark | Also at https://pypi.org/project/py4j/ and https://www.py4j.org/ |
| Spark src  | https://github.com/apache/spark | Official Apache repo |
| py4j src   | https://github.com/py4j/py4j | Official repo |

Do NOT use Adoptium, Azul, Corretto, or any third-party JDK redistributor.
Do NOT use NuGet, conda, or pyenv for Python. Use the canonical sources only.

Your deliverables:
1. A git repo with reproducer, test harness, and automation
2. Actual test results from running the version matrix on this machine
3. A JIRA-ready bug report populated with real data
4. Shallow clones of apache/spark and py4j/py4j for future patching

---

## Background: what the bug is

PySpark has two codepaths for launching Python workers:

- **daemon path**: `daemon.py` uses `os.fork()`. POSIX only. Works.
- **simple-worker path**: `createSimpleWorker()` spawns
  `python -m pyspark.worker` as a subprocess via TCP loopback. **Broken.**

Windows ALWAYS uses the simple-worker path because `os.fork()` doesn't exist.
On Linux, `spark.python.use.daemon=false` triggers the same broken path.

Confirmed on Linux (Python 3.12, PySpark 3.5.5, OpenJDK 21): daemon=true
passes, daemon=false crashes deterministically:

```
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
Caused by: java.io.EOFException
  at PythonRunner$$anon$3.read(PythonRunner.scala:774)
```

`spark.range().count()` works (JVM-only). `createDataFrame()`, UDFs, anything
needing the Python worker crashes. The worker connects and authenticates but
dies before `worker.py`'s `main()` is entered.

---

## Step 1: Install Oracle JDK

Check if Java is installed: `java -version`

If not, download Oracle JDK 21 (LTS) from:
https://www.oracle.com/java/technologies/downloads/#jdk21-windows

Direct download links (Oracle No-Fee Terms and Conditions license):
- MSI: `https://download.oracle.com/java/21/latest/jdk-21_windows-x64_bin.msi`
- EXE: `https://download.oracle.com/java/21/latest/jdk-21_windows-x64_bin.exe`

Silent install:
```powershell
# MSI (requires admin):
msiexec /i jdk-21_windows-x64_bin.msi /quiet

# Or EXE:
.\jdk-21_windows-x64_bin.exe /s
```

Set JAVA_HOME:
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

If no admin rights, use the ZIP archive from the same Oracle page — extract
and point JAVA_HOME to the extracted directory.

Verify: `java -version` → should show `java version "21.x.x"`.

---

## Step 2: Install Python versions from python.org

Download official CPython installers and install silently into isolated dirs.

URL pattern: `https://www.python.org/ftp/python/<ver>/python-<ver>-amd64.exe`

Silent install (per-user, no admin):
```
python-<ver>-amd64.exe /quiet InstallAllUsers=0 Include_launcher=0 Include_test=0 TargetDir=<path>
```

### Versions

Browse https://www.python.org/ftp/python/ for latest patches. Try:

| Minor | Version | Installer URL |
|-------|---------|---------------|
| 3.11  | 3.11.12 | `https://www.python.org/ftp/python/3.11.12/python-3.11.12-amd64.exe` |
| 3.12  | 3.12.10 | `https://www.python.org/ftp/python/3.12.10/python-3.12.10-amd64.exe` |
| 3.13  | 3.13.5  | `https://www.python.org/ftp/python/3.13.5/python-3.13.5-amd64.exe` |
| 3.14  | 3.14.3  | `https://www.python.org/ftp/python/3.14.3/python-3.14.3-amd64.exe` |

If a URL 404s, check the FTP listing for that minor series.

### Script: install_pythons.ps1

For each version: download to `python-installers\`, install to
`python-installations\python-<ver>\`, verify `python.exe --version`.
Log failures and continue.

---

## Step 3: Create the repo

```powershell
mkdir spark53759-reproducer
cd spark53759-reproducer
git init
```

### Structure
```
spark53759-reproducer\
├── reproduce.py              # Minimal JIRA-pasteable reproducer
├── run_matrix.py             # Full test harness
├── run_all_versions.ps1      # Orchestrator: creates venvs, runs matrix
├── install_pythons.ps1       # Downloads Python from python.org
├── requirements.txt          # pyspark==3.5.5
├── .gitignore
├── results\                  # JSON outputs (gitignored except .gitkeep)
│   └── .gitkeep
├── python-installations\     # Pythons from python.org (gitignored)
├── python-installers\        # Downloaded .exe files (gitignored)
├── venvs\                    # Per-combo virtual environments (gitignored)
└── upstream\                 # Cloned source repos (gitignored)
```

### reproduce.py

Minimal one-file reproducer. Self-contained, no imports beyond pyspark.
This is what gets pasted into a JIRA comment.

```python
"""
Minimal reproducer for SPARK-53759.

On Windows: crashes by default (daemon is always disabled).
On Linux/macOS: set spark.python.use.daemon=false to trigger.

Requires: pip install pyspark (tested 3.5.x, 4.0.x)
Requires: Java 8, 11, 17, or 21 (tested with Oracle JDK 21)
"""
from pyspark.sql import SparkSession
import sys
import os

print(f"Python:   {sys.version}")
print(f"Platform: {sys.platform} ({os.name})")

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SPARK-53759-repro") \
    .config("spark.ui.enabled", "false") \
    .config("spark.python.use.daemon", "false") \
    .getOrCreate()

print(f"Spark:    {spark.version}")
print(f"py4j:     {__import__('py4j').__version__}")
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
    if "crashed" in msg or "EOFException" in msg:
        print("createDataFrame() CRASHED  [worker: FAIL]")
        print("\nRESULT: FAIL — SPARK-53759 confirmed")
    else:
        print(f"createDataFrame() ERROR: {type(e).__name__}")
        print(f"\nRESULT: FAIL — unexpected: {msg[:300]}")
finally:
    spark.stop()
```

### run_matrix.py

Takes `--label LABEL` for tagging output files.

Tests four operations with `daemon=false`:
1. `spark.range(10).count()` — JVM-only baseline
2. `spark.createDataFrame([("Alice",30)],["name","age"]).count()` — needs worker
3. UDF: `spark.range(5).withColumn("x", udf(...)("id")).collect()` — needs worker
4. Write temp file, `spark.read.text("file:///path")` — needs worker + I/O

On non-Windows (os.name != "nt"), also run all four with `daemon=true`.
On Windows, note that Spark ignores daemon=true (falls back to daemon=false).

If `create_dataframe` crashes, skip remaining worker tests.

JSON output to `results\<label>.json`:
```json
{
  "environment": {
    "python_version": "3.12.10 (tags/v3.12.10:...",
    "python_version_short": "3.12.10",
    "pyspark_version": "3.5.5",
    "py4j_version": "0.10.9.7",
    "java_version": "java version \"21.0.x\"",
    "platform": "Windows-10-...",
    "os_name": "nt"
  },
  "results": [
    {"name": "spark_range", "daemon": "false", "status": "PASS", ...},
    {"name": "create_dataframe", "daemon": "false", "status": "FAIL",
     "error": "Python worker crashed (EOFException)", ...}
  ]
}
```

### run_all_versions.ps1

Orchestrator. For each installed Python × PySpark version:

1. Create venv: `& <python.exe> -m venv venvs\<label>`
2. Install PySpark from PyPI: `& venvs\<label>\Scripts\python.exe -m pip install --quiet pyspark==<ver>`
   - PySpark is the official Apache Spark Python package on PyPI; it bundles py4j
   - If install fails (version incompatibility), log and skip
3. Run: `& venvs\<label>\Scripts\python.exe run_matrix.py --label <label>`

PySpark versions (from PyPI): **3.4.4, 3.5.5, 4.0.0**

Master summary table at end (read from all `results\*.json`):

```
Python     PySpark   py4j         createDataFrame (daemon=false)
---------  --------  -----------  --------------------------------
3.11.12    3.4.4     0.10.9.5     ???
3.11.12    3.5.5     0.10.9.7     ???
3.12.10    3.5.5     0.10.9.7     FAIL (worker crashed)
3.14.3     3.5.5     0.10.9.7     FAIL (worker crashed)
...
```

This table identifies the exact regression boundary.

### .gitignore

```
python-installations/
python-installers/
venvs/
upstream/
results/*.json
_test_input.txt
__pycache__/
*.pyc
```

---

## Step 4: Run it

```powershell
powershell -ExecutionPolicy Bypass -File install_pythons.ps1
powershell -ExecutionPolicy Bypass -File run_all_versions.ps1
```

Let both complete. The master summary answers:
- Does 3.11 + daemon=false crash? → was createSimpleWorker ever functional?
- Does PySpark 3.4 + daemon=false crash? → regression in PySpark or Python?
- Which py4j version boundary matters?

---

## Step 5: Clone upstream repos (official sources only)

```powershell
New-Item -ItemType Directory -Force -Path upstream | Out-Null
git clone --depth 1 https://github.com/apache/spark.git upstream\spark
git clone --depth 1 https://github.com/py4j/py4j.git upstream\py4j
```

These are the official Apache and py4j repositories. Don't modify — reference only.

---

## Step 6: Write BUG_REPORT.md

Use actual results from Step 4. Structure:

**Title**: `SPARK-53759: createSimpleWorker broken — affects all Windows users`

**Summary**: createSimpleWorker codepath crashes deterministically. Windows
always uses this path. Reproducible on Linux with daemon=false.

**Affected Versions**: Master summary table from Step 4.

**Environment**: Oracle JDK version, Python versions from python.org,
PySpark versions from PyPI. All official distributions.

**Reproduction**: Output of `reproduce.py` verbatim.

**Analysis**: createSimpleWorker broken, daemon path works, worker dies
between auth and main(). Key source files:
- `PythonWorkerFactory.scala` (createSimpleWorker vs createThroughDaemon)
- `worker.py` (__main__ block — simple-worker entry)
- `daemon.py` (worker() function — daemon entry, uses os.fork)
- `java_gateway.py` (local_connect_and_auth — TCP handshake)

**Open Questions**: Updated based on matrix results.

**Workarounds**: Python 3.11 (if matrix confirms), WSL/Linux, PySpark 3.4.

---

## Step 7: Commit

```powershell
git add -A
git commit -m "SPARK-53759 reproducer: version matrix and bug report

All components from official sources:
- Python from python.org
- Oracle JDK from oracle.com
- PySpark from PyPI (official Apache Spark distribution)

Tested Python 3.11-3.14 x PySpark 3.4-4.0 on Windows.
The createSimpleWorker codepath crashes deterministically.
Includes minimal reproducer, automated matrix, and draft bug report."
```

---

## What NOT to do

- Do NOT theorize about GC timing, FD lifecycle, or os.fdopen/os.dup — these
  were disproven. The worker never reaches main().
- Do NOT speculate beyond what the matrix proves.
- Do NOT modify the upstream clones yet.
- Do NOT use third-party redistributions of Java, Python, or PySpark.
- Do NOT assume the bug is Python-version-specific until the matrix confirms it.
