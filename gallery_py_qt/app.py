"""Application bootstrap: argument parsing, crash handler, event loop."""
from __future__ import annotations
import argparse
import os
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="gallery_py_qt",
        description="Vertical media gallery (PySide6) \u2014 image/video viewer.")
    parser.add_argument("folder", nargs="?", default=None,
                        help="folder to open on launch")
    args = parser.parse_args(argv)

    if __package__ in (None, ""):
        sys.path.insert(0, os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))

    from gallery_py_qt import crash
    crash.install()

    from PySide6.QtWidgets import QApplication
    from gallery_py_qt.main_window import MainWindow
    from gallery_py_qt.engine import prefs

    app = QApplication(sys.argv)
    app.setApplicationName("Gallery-Py-Qt")

    win = MainWindow()
    win.show()

    target = None
    if args.folder and os.path.isdir(args.folder):
        target = args.folder
    if target:
        from PySide6.QtCore import QTimer
        QTimer.singleShot(150, lambda: win.open_folder(target))

    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
