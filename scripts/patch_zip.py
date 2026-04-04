"""
Patch worker.py inside pyspark.zip with instrumentation logging.

CRITICAL DISCOVERY: The JVM loads worker.py from pyspark/python/lib/pyspark.zip,
NOT from site-packages/pyspark/worker.py. The JVM's PythonWorkerFactory sets
PYTHONPATH to include the zip, which takes precedence. This is why all earlier
patches to site-packages had no effect.

This script patches the zip-bundled copy with file-based logging so we can
observe the worker lifecycle under real JVM orchestration.

KEY FINDING: Instrumentation showed main() returns normally — the worker
completes its work. The data loss happens during process exit when
BufferedRWPair.__del__ fails to flush because the socket is already closed.

Usage:
    python scripts/patch_zip.py --py 3.13 --apply     # apply logging patch
    python scripts/patch_zip.py --py 3.13 --revert     # restore from backup
    python scripts/patch_zip.py --py 3.13 --show-logs  # display worker logs
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


def find_pyspark_zip(python_exe):
    """Find pyspark.zip for a given Python installation."""
    result = subprocess.run(
        [python_exe, "-c", "import pyspark; print(pyspark.__file__)"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pyspark not installed: {result.stderr}")
    pyspark_dir = Path(result.stdout.strip()).parent
    zip_path = pyspark_dir / "python" / "lib" / "pyspark.zip"
    if not zip_path.is_file():
        raise RuntimeError(f"pyspark.zip not found at {zip_path}")
    return zip_path


LOG_PREAMBLE = '''
# SPARK-53759 instrumentation — injected by patch_zip_worker.py
import os as _os_early, time as _time_early, tempfile as _tf_early, sys as _sys_early
_s53759_logpath = _os_early.path.join(_tf_early.gettempdir(), f"spark53759_worker_{_os_early.getpid()}.log")
def _s53759_log(msg):
    try:
        with open(_s53759_logpath, "a") as _f:
            _f.write(f"{_time_early.time():.3f} [{_os_early.getpid()}] {msg}\\n")
    except Exception as _e:
        _sys_early.stderr.write(f"SPARK53759 LOG FAILED: {_e}\\n")
_s53759_log(f"worker.py loaded, Python {_sys_early.version}")
'''


def apply_patch(zip_path):
    """Add logging instrumentation to worker.py inside the zip."""
    backup = zip_path.with_suffix(".zip.bak")
    if not backup.exists():
        shutil.copy2(zip_path, backup)
        print(f"Backed up: {backup}")

    # Read all files from zip
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        contents = {}
        for name in names:
            contents[name] = zf.read(name)

    # Patch worker.py
    original = contents["pyspark/worker.py"].decode("utf-8")

    if "_s53759_log" in original:
        print("Already patched!")
        return

    # Insert logging after docstring
    patched = original.replace(
        '"""\nWorker that receives input from Piped RDD.\n"""',
        '"""\nWorker that receives input from Piped RDD.\n"""' + LOG_PREAMBLE,
    )

    # Instrument __main__ block
    patched = patched.replace(
        "(sock_file, _) = local_connect_and_auth(conn_info, auth_secret)\n"
        "    # TODO: Remove the following two lines",
        '_s53759_log(f"calling connect, port={conn_info}")\n'
        "    (sock_file, _sock) = local_connect_and_auth(conn_info, auth_secret)\n"
        '    _s53759_log(f"connected, fileno={_sock.fileno()}")\n'
        "    # TODO: Remove the following two lines",
    )

    patched = patched.replace(
        "    write_int(os.getpid(), sock_file)\n"
        "    sock_file.flush()\n"
        "    main(sock_file, sock_file)",
        "    write_int(os.getpid(), sock_file)\n"
        '    _s53759_log("wrote PID")\n'
        "    sock_file.flush()\n"
        '    _s53759_log("flushed, entering main()")\n'
        "    try:\n"
        "        main(sock_file, sock_file)\n"
        '        _s53759_log("main() returned normally")\n'
        "    except SystemExit as _e:\n"
        '        _s53759_log(f"main() SystemExit({_e.code})")\n'
        "        raise\n"
        "    except Exception as _e:\n"
        "        import traceback as _tb\n"
        '        _s53759_log(f"main() EXCEPTION: {type(_e).__name__}: {_e}")\n'
        "        for _line in _tb.format_exc().splitlines():\n"
        '            _s53759_log(f"  {_line}")\n'
        "        raise\n"
        '    _s53759_log("after main() try/except block")\n'
        '    _s53759_log(f"sock_file closed={sock_file.closed if hasattr(sock_file, \'closed\') else \'?\'}")\n'
        '    _s53759_log(f"_sock fileno={_sock.fileno()}, timeout={_sock.gettimeout()}")\n'
        "    # Explicitly flush and close to prevent cleanup race\n"
        "    try:\n"
        "        sock_file.flush()\n"
        '        _s53759_log("final flush OK")\n'
        "    except Exception as _e:\n"
        '        _s53759_log(f"final flush FAILED: {type(_e).__name__}: {_e}")\n'
        "    import atexit\n"
        '    atexit.register(lambda: _s53759_log("atexit called"))',
    )

    if patched == original:
        print("WARNING: Could not find patch targets in worker.py")
        return

    contents["pyspark/worker.py"] = patched.encode("utf-8")
    print(f"Patched worker.py: {len(original)} -> {len(patched)} bytes")

    # Rebuild zip
    tmp_zip = zip_path.with_suffix(".zip.tmp")
    with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in names:
            zf.writestr(name, contents[name])

    tmp_zip.replace(zip_path)
    print(f"Rebuilt: {zip_path}")


def revert_patch(zip_path):
    """Restore pyspark.zip from backup."""
    backup = zip_path.with_suffix(".zip.bak")
    if not backup.exists():
        print("No backup found — nothing to revert")
        return
    shutil.copy2(backup, zip_path)
    print(f"Reverted from: {backup}")


def show_logs():
    """Display worker log files from temp directory."""
    tmp = tempfile.gettempdir()
    logs = sorted(Path(tmp).glob("spark53759_worker_*.log"))
    if not logs:
        print(f"No worker logs found in {tmp}")
        return
    for logfile in logs:
        print(f"=== {logfile.name} ===")
        print(logfile.read_text())
        print()


def clear_logs():
    """Delete worker log files from temp directory."""
    tmp = tempfile.gettempdir()
    for f in Path(tmp).glob("spark53759_worker_*.log"):
        f.unlink()
    print("Cleared worker logs")


def main():
    parser = argparse.ArgumentParser(description="Patch pyspark.zip worker.py")
    parser.add_argument("--py", default=None, help="Python version (e.g. '3.13')")
    parser.add_argument("--apply", action="store_true", help="Apply instrumentation patch")
    parser.add_argument("--revert", action="store_true", help="Revert to backup")
    parser.add_argument("--show-logs", action="store_true", help="Show worker logs")
    parser.add_argument("--clear-logs", action="store_true", help="Clear worker logs")
    args = parser.parse_args()

    if args.show_logs:
        show_logs()
        return

    if args.clear_logs:
        clear_logs()
        return

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from _resolve_python import resolve_python
    python_exe = resolve_python(args.py)
    zip_path = find_pyspark_zip(python_exe)
    print(f"Python: {python_exe}")
    print(f"pyspark.zip: {zip_path}")

    if args.apply:
        apply_patch(zip_path)
    elif args.revert:
        revert_patch(zip_path)
    else:
        parser.error("Use --apply, --revert, --show-logs, or --clear-logs")


if __name__ == "__main__":
    main()
