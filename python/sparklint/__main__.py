"""``python -m sparklint`` — run the bundled binary."""

from __future__ import annotations

import os
import subprocess
import sys

from sparklint import find_sparklint_bin


def _run() -> None:
    sparklint = find_sparklint_bin()
    if sys.platform == "win32":
        completed = subprocess.run([sparklint, *sys.argv[1:]])
        sys.exit(completed.returncode)
    os.execvp(sparklint, [sparklint, *sys.argv[1:]])


if __name__ == "__main__":
    _run()
