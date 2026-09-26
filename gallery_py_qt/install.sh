#!/usr/bin/env bash
# Set up gallery_py_qt on Linux or macOS.
#
#   ./gallery_py_qt/install.sh          # runtime dependencies only
#   ./gallery_py_qt/install.sh --dev    # also pytest, for the test suite
#
# Safe to re-run: an existing .venv is reused and brought up to date.
set -euo pipefail

# Locate the repository from this script's own path, so the directory the user
# runs it from does not matter.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(dirname "$here")"
venv="$repo/.venv"

reqs="$here/requirements.txt"
dev=0
if [ "${1:-}" = "--dev" ]; then
    reqs="$here/requirements-dev.txt"
    dev=1
fi

step() { printf '\n==> %s\n' "$1"; }

# -- 1. find Python 3.10+ -----------------------------------------------------
step "Looking for Python 3.10 or newer"
py=""
for cand in python3.13 python3.12 python3.11 python3.10 python3; do
    command -v "$cand" >/dev/null 2>&1 || continue
    if "$cand" -c 'import sys; sys.exit(0 if sys.version_info[:2] >= (3,10) else 1)'; then
        py="$cand"
        echo "    found $("$cand" -V)"
        break
    fi
done
if [ -z "$py" ]; then
    echo "No Python 3.10+ found. Install it and re-run." >&2
    exit 1
fi

# -- 2. Qt's system libraries (Linux only) ------------------------------------
# PySide6 bundles Qt itself but not these; without them the import fails with
# a bare "libEGL.so.1: cannot open shared object file".
if [ "$(uname -s)" = "Linux" ] && command -v apt-get >/dev/null 2>&1; then
    # Read the cache ONCE into a variable rather than piping it per library:
    # `grep -q` exits at the first match, and the resulting SIGPIPE on ldconfig
    # is turned into a pipeline failure by `pipefail`, which would report every
    # library as missing even when all of them are installed.
    cache="$(ldconfig -p 2>/dev/null || true)"
    missing=0
    for lib in libEGL.so.1 libGL.so.1 libxkbcommon.so.0; do
        case "$cache" in *"$lib"*) ;; *) missing=1 ;; esac
    done
    if [ "$missing" = 1 ]; then
        step "Installing Qt's system libraries"
        sudo=""
        [ "$(id -u)" -eq 0 ] || sudo="sudo"
        # An update failure is usually an unrelated third-party repository;
        # the install below is what actually matters, so don't abort on it.
        $sudo apt-get update || echo "    (apt update reported errors; continuing)"
        $sudo apt-get install -y --no-install-recommends \
            libegl1 libgl1 libxkbcommon0 libdbus-1-3 \
            libpulse0 libfontconfig1 libxcb-cursor0
    fi
fi

# -- 3. virtual environment ---------------------------------------------------
if [ -x "$venv/bin/python" ]; then
    step "Reusing the environment at $venv"
else
    step "Creating a virtual environment at $venv"
    "$py" -m venv "$venv"
fi

# -- 4. dependencies ----------------------------------------------------------
# NOTE: the requirements.txt in the REPOSITORY ROOT is a freeze of an unrelated
# environment and must not be used here. The gallery's own list is this one.
step "Installing $(basename "$reqs")"
"$venv/bin/python" -m pip install --upgrade pip --quiet
"$venv/bin/python" -m pip install -r "$reqs"

# -- 5. verify ----------------------------------------------------------------
# Importing is the real test: a wheel can install cleanly and still fail to
# load when a system library is missing.
step "Verifying"
"$venv/bin/python" -c \
    "import PySide6, PIL, cv2, piexif; print('    all four packages import cleanly')"

printf '\nDone. Start the gallery with:\n\n    %s %s\n\n' \
    "$venv/bin/python" "$repo/gallery_py_qt.py"
if [ "$dev" = 1 ]; then
    printf 'Run the tests with:\n\n    QT_QPA_PLATFORM=offscreen %s -m pytest tests -q\n\n' \
        "$venv/bin/python"
fi
