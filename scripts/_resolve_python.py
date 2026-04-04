"""
Resolve --py version shorthand to a Python executable path.

Uses the Windows py launcher (py -3.13) or accepts a full path.
Shared by investigation scripts in scripts/.
"""
import subprocess
import sys
from pathlib import Path


def resolve_python(version_hint):
    """Resolve '3.13' to a full Python executable path via py launcher."""
    if version_hint is None:
        return sys.executable

    # Full path?
    p = Path(version_hint)
    if p.is_file():
        return str(p)

    # Try py launcher (Windows)
    try:
        result = subprocess.run(
            ["py", f"-{version_hint}", "-c", "import sys; print(sys.executable)"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except FileNotFoundError:
        pass

    # Try pythonX.Y directly (Linux/macOS)
    try:
        result = subprocess.run(
            [f"python{version_hint}", "-c", "import sys; print(sys.executable)"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except FileNotFoundError:
        pass

    raise FileNotFoundError(
        f"Could not find Python {version_hint}. "
        f"Try: py -{version_hint} --version (Windows) or python{version_hint} --version (Linux)"
    )
