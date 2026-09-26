#!/usr/bin/env python3
"""Launcher for the Qt gallery:  python gallery_py_qt.py [FOLDER] [--no-restore]

Equivalent to `python -m gallery_py_qt`, but runnable as a loose script.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gallery_py_qt.app import main

if __name__ == "__main__":
    sys.exit(main())
