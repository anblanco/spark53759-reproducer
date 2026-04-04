"""
Diagnostic harness for SPARK-53759.

Mimics the JVM side of createSimpleWorker entirely in Python:
  1. Opens a server socket on loopback
  2. Spawns `python -m pyspark.worker` as a subprocess
  3. Accepts the connection, performs auth handshake
  4. Reads the worker PID
  5. Sends a minimal task payload (split_index = -1 to trigger clean exit)

KEY FINDING: This harness does NOT reproduce the crash. Both Python 3.11 and
3.13 complete the full worker lifecycle successfully here. The crash only occurs
under real JVM orchestration, because the JVM loads worker.py from pyspark.zip
(not site-packages), and the missing flush only matters when the JVM is reading
real task results from the socket.

This proved the issue is in process-exit cleanup, not in connection setup.

Usage:
    python scripts/diagnose.py --py 3.13
    python scripts/diagnose.py --all
"""
import argparse
import os
import socket
import struct
import subprocess
import sys
import threading
import time


def write_int(value, stream):
    stream.write(struct.pack("!i", value))


def read_int(stream):
    data = stream.read(4)
    if not data or len(data) < 4:
        raise EOFError(f"Expected 4 bytes, got {len(data) if data else 0}")
    return struct.unpack("!i", data)[0]


def write_with_length(data, stream):
    write_int(len(data), stream)
    stream.write(data)


def read_with_length(stream):
    length = read_int(stream)
    return stream.read(length)


def log(msg):
    elapsed = time.time() - START
    print(f"  [{elapsed:7.3f}s] {msg}")


START = time.time()


def run_diagnosis(python_exe, verbose=False):
    print(f"Python: {python_exe}")

    # Check Python version
    result = subprocess.run([python_exe, "--version"], capture_output=True, text=True)
    py_version = result.stdout.strip()
    print(f"Version: {py_version}")

    # Check pyspark is importable
    result = subprocess.run(
        [python_exe, "-c", "import pyspark; print(pyspark.__version__)"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"SKIP: pyspark not installed for {python_exe}")
        print(f"  stderr: {result.stderr.strip()}")
        return None
    ps_version = result.stdout.strip()
    print(f"PySpark: {ps_version}")
    print()

    # Step 1: Create server socket
    log("Creating server socket on 127.0.0.1...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    log(f"Listening on port {port}")

    # Step 2: Set up environment (mimicking PythonWorkerFactory.scala)
    auth_secret = "test-secret-for-diagnosis-12345"
    env = os.environ.copy()
    env["PYTHON_WORKER_FACTORY_PORT"] = str(port)
    env["PYTHON_WORKER_FACTORY_SECRET"] = auth_secret
    env["PYTHONUNBUFFERED"] = "YES"
    env["PYSPARK_PYTHON"] = python_exe
    env["PYSPARK_DRIVER_PYTHON"] = python_exe

    # Step 3: Spawn worker subprocess
    log("Spawning worker: python -m pyspark.worker")
    worker_proc = subprocess.Popen(
        [python_exe, "-m", "pyspark.worker"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    log(f"Worker PID (OS): {worker_proc.pid}")

    # Capture stderr in background thread
    stderr_lines = []

    def read_stderr():
        for line in worker_proc.stderr:
            text = line.decode("utf-8", errors="replace").rstrip()
            stderr_lines.append(text)
            if verbose:
                log(f"[worker stderr] {text}")

    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    stderr_thread.start()

    # Step 4: Accept connection from worker
    log("Waiting for worker to connect (10s timeout)...")
    server.settimeout(10.0)
    try:
        conn, addr = server.accept()
        log(f"Worker connected from {addr}")
    except socket.timeout:
        log("TIMEOUT: Worker never connected")
        worker_proc.kill()
        stderr_thread.join(timeout=2)
        if stderr_lines:
            print("\n  Worker stderr:")
            for line in stderr_lines[-20:]:
                print(f"    {line}")
        return "CONNECT_TIMEOUT"

    # Step 5: Auth handshake (JVM side = server, worker = client)
    # Worker sends: [length][secret_bytes]
    # JVM replies: [length]["ok"]
    log("Performing auth handshake...")
    conn_file = conn.makefile("rwb", 65536)
    try:
        # Read secret from worker
        secret_from_worker = read_with_length(conn_file)
        received_secret = secret_from_worker.decode("utf-8")
        if received_secret != auth_secret:
            log(f"AUTH FAIL: expected '{auth_secret}', got '{received_secret}'")
            return "AUTH_MISMATCH"
        log("Auth secret received and verified")

        # Send "ok" reply
        write_with_length(b"ok", conn_file)
        conn_file.flush()
        log("Auth reply 'ok' sent")
    except EOFError as e:
        log(f"AUTH FAIL: EOFError during handshake: {e}")
        worker_proc.kill()
        stderr_thread.join(timeout=2)
        if stderr_lines:
            print("\n  Worker stderr:")
            for line in stderr_lines[-20:]:
                print(f"    {line}")
        return "AUTH_EOF"
    except Exception as e:
        log(f"AUTH FAIL: {type(e).__name__}: {e}")
        return "AUTH_ERROR"

    # Step 6: Read worker PID
    log("Reading worker PID...")
    try:
        worker_pid = read_int(conn_file)
        log(f"Worker PID (reported): {worker_pid}")
    except EOFError:
        log("FAIL: EOFError reading worker PID — worker died after auth")
        worker_proc.kill()
        stderr_thread.join(timeout=2)
        if stderr_lines:
            print("\n  Worker stderr:")
            for line in stderr_lines[-20:]:
                print(f"    {line}")
        return "PID_EOF"

    # Step 7: Send minimal task payload
    # In real Spark, the task executor sends split_index first.
    # split_index = -1 triggers sys.exit(-1) in worker.main() — clean exit for tests.
    log("Sending split_index = -1 (clean exit signal)...")
    try:
        write_int(-1, conn_file)
        conn_file.flush()
        log("Task payload sent")
    except (BrokenPipeError, OSError) as e:
        log(f"FAIL: {type(e).__name__} sending task — worker already dead: {e}")
        worker_proc.kill()
        stderr_thread.join(timeout=2)
        if stderr_lines:
            print("\n  Worker stderr:")
            for line in stderr_lines[-20:]:
                print(f"    {line}")
        return "SEND_BROKEN_PIPE"

    # Step 8: Wait for worker to exit
    log("Waiting for worker to exit...")
    try:
        exit_code = worker_proc.wait(timeout=10)
        log(f"Worker exited with code {exit_code}")
    except subprocess.TimeoutExpired:
        log("Worker did not exit in 10s, killing...")
        worker_proc.kill()
        exit_code = -9

    # Collect stderr
    stderr_thread.join(timeout=2)

    # Report
    print()
    if exit_code == 0 or exit_code == -1 or exit_code == 255:
        # -1 maps to 255 on Windows (unsigned byte)
        log("RESULT: PASS — worker completed the full lifecycle")
        status = "PASS"
    else:
        log(f"RESULT: FAIL — worker exited with code {exit_code}")
        status = "FAIL"

    if stderr_lines and (verbose or status == "FAIL"):
        print("\n  Worker stderr (last 30 lines):")
        for line in stderr_lines[-30:]:
            print(f"    {line}")

    # Cleanup
    try:
        conn_file.close()
        conn.close()
    except Exception:
        pass
    server.close()

    return status


def main():
    parser = argparse.ArgumentParser(description="SPARK-53759 diagnostic harness")
    parser.add_argument(
        "--py", default=None,
        help="Python version shorthand (e.g. '3.13') or path to python.exe",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Show worker stderr in real-time")
    parser.add_argument(
        "--all", action="store_true",
        help="Test all Python installations in python-installations/",
    )
    args = parser.parse_args()

    sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__))))
    from _resolve_python import resolve_python

    if args.all:
        # Run against 3.11, 3.12, 3.13 via py launcher
        results = []
        for ver in ["3.11", "3.12", "3.13"]:
            try:
                exe = resolve_python(ver)
            except FileNotFoundError:
                print(f"Python {ver} not found, skipping")
                continue
            print(f"\n{'='*60}")
            status = run_diagnosis(exe, verbose=args.verbose)
            results.append((ver, status))
            print()

        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        for ver, status in results:
            print(f"  Python {ver:<10s} {status}")
    else:
        python_exe = resolve_python(args.py)
        run_diagnosis(python_exe, verbose=args.verbose)


if __name__ == "__main__":
    main()
