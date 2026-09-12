"""Fixtures shared by the test suite.

The Spark session is session-scoped: a JVM per test would dominate the run
time, and every Spark test treats the session as read-only. The tests skip
with a reason when there is no JVM to start.
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

# PySpark 3.5 on Windows cannot use `pyspark.daemon` (it forks), so each task
# runs `python -m pyspark.worker`. That worker writes its results into the
# 64 KiB buffer of `socket.makefile("rwb", ...)` and never flushes them --- it
# relies on the interpreter's exit-time flush, which on this machine runs after
# the socket is already gone. The JVM then reads nothing and every Python-side
# task dies with "Python worker exited unexpectedly (crashed)". Flushing at
# `atexit`, while the socket is still open, fixes it. PYTHON_WORKER_FACTORY_PORT
# is set only in worker processes, so the driver is left alone.
_WORKER_SITECUSTOMIZE = '''\
import os

if os.environ.get("PYTHON_WORKER_FACTORY_PORT"):
    import atexit
    import socket
    import weakref

    _socket_files = weakref.WeakSet()
    _make_file = socket.socket.makefile

    def makefile(self, *args, **kwargs):
        handle = _make_file(self, *args, **kwargs)
        _socket_files.add(handle)
        return handle

    socket.socket.makefile = makefile

    @atexit.register
    def _flush_socket_files():
        for handle in list(_socket_files):
            try:
                handle.flush()
            except Exception:
                pass
'''


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        bin_dir = Path(java_home) / "bin"
        if (bin_dir / "java.exe").exists() or (bin_dir / "java").exists():
            return True
    return shutil.which("java") is not None


def _install_worker_flush_fix() -> str | None:
    """Put the worker `sitecustomize` on PYTHONPATH. Returns the directory."""
    if not sys.platform.startswith("win"):
        return None
    path = tempfile.mkdtemp(prefix="pyspark-worker-fix-")
    (Path(path) / "sitecustomize.py").write_text(
        _WORKER_SITECUSTOMIZE, encoding="utf-8", newline="\n"
    )
    existing = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = path + (os.pathsep + existing if existing else "")
    return path


@pytest.fixture(scope="session")
def spark():
    if not _java_available():
        pytest.skip("no JVM found: set JAVA_HOME or put java on PATH")

    fix_dir = _install_worker_flush_fix()
    # Without this the worker is looked up as bare "python", which on Windows
    # is the Microsoft Store stub.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    from pyspark.sql import SparkSession

    session = (SparkSession.builder
               .master("local[2]")
               .appName("vn-rental-dsp-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
    if fix_dir:
        shutil.rmtree(fix_dir, ignore_errors=True)
