"""sparklint: a fast, Spark-aware linter for PySpark code (Rust binary)."""

from __future__ import annotations

import os
import sys
import sysconfig

__all__ = ["find_sparklint_bin"]


def find_sparklint_bin() -> str:
    """Return the path to the bundled ``sparklint`` binary."""
    exe = "sparklint" + (sysconfig.get_config_var("EXE") or "")

    candidates = [
        os.path.join(sysconfig.get_path("scripts"), exe),
    ]
    if sys.version_info >= (3, 10):
        user_scheme = sysconfig.get_preferred_scheme("user")
    elif os.name == "nt":
        user_scheme = "nt_user"
    elif sys.platform == "darwin" and sys._framework:  # type: ignore[attr-defined]
        user_scheme = "osx_framework_user"
    else:
        user_scheme = "posix_user"
    candidates.append(os.path.join(sysconfig.get_path("scripts", scheme=user_scheme), exe))
    # `pip install --target` / unusual layouts: <prefix>/bin next to the package.
    pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates.append(os.path.join(pkg_root, "bin", exe))
    candidates.append(os.path.join(os.path.dirname(pkg_root), "bin", exe))

    for path in candidates:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "sparklint binary not found; searched: " + ", ".join(candidates)
    )
