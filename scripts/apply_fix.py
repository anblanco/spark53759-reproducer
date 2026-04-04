"""
Apply the SPARK-53759 flush fix to pyspark.zip for any Python version.

The fix: wrap main() in try/finally with an explicit sock_file.flush().
This mirrors daemon.py's worker() function which already has this flush.

Verified results:
  - Python 3.11.9  Windows: PASS (harmless, already worked)
  - Python 3.12.10 Windows: PASS (fixes the crash)
  - Python 3.13.3  Windows: PASS (fixes the crash)
  - Python 3.12.3  Linux:   PASS (fixes the crash)

Usage:
    python scripts/apply_fix.py --py 3.12 --apply
    python scripts/apply_fix.py --py 3.12 --revert
"""
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _resolve_python import resolve_python


def find_zip(py):
    r = subprocess.run(
        [py, "-c", "import pyspark; print(pyspark.__file__)"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"pyspark not installed: {r.stderr.strip()}")
    return Path(r.stdout.strip()).parent / "python" / "lib" / "pyspark.zip"


OLD = (
    "    main(sock_file, sock_file)"
)

NEW = (
    "    try:\n"
    "        main(sock_file, sock_file)\n"
    "    finally:\n"
    "        # SPARK-53759: explicit flush before exit to prevent data loss\n"
    "        # from BufferedRWPair cleanup race on Python 3.12+ Windows.\n"
    "        # Mirrors the flush in daemon.py's worker() finally block.\n"
    "        try:\n"
    "            sock_file.flush()\n"
    "        except Exception:\n"
    "            pass"
)


def apply_patch(python_exe):
    zp = find_zip(python_exe)
    print(f"pyspark.zip: {zp}")
    bk = zp.with_suffix(".zip.bak")
    if not bk.exists():
        shutil.copy2(zp, bk)

    with zipfile.ZipFile(zp) as z:
        names = z.namelist()
        contents = {n: z.read(n) for n in names}

    orig = contents["pyspark/worker.py"].decode()
    if "SPARK-53759" in orig:
        print("Already patched")
        return True

    patched = orig.replace(OLD, NEW)
    if patched == orig:
        print("Could not find patch target — worker.py may have changed upstream")
        return False

    contents["pyspark/worker.py"] = patched.encode()
    tmp = zp.with_suffix(".zip.tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        for n in names:
            z.writestr(n, contents[n])
    tmp.replace(zp)
    print("Applied: try/finally flush after main()")
    return True


def revert_patch(python_exe):
    zp = find_zip(python_exe)
    bk = zp.with_suffix(".zip.bak")
    if bk.exists():
        shutil.copy2(bk, zp)
        print(f"Reverted: {zp}")
    else:
        print("No backup found")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--py", default=None,
                   help="Python version or path (default: current interpreter)")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--revert", action="store_true")
    a = p.parse_args()
    py = resolve_python(a.py)  # defaults to sys.executable when --py omitted
    if a.apply:
        apply_patch(py)
    elif a.revert:
        revert_patch(py)
