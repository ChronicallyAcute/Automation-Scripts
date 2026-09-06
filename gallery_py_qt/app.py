"""Application bootstrap: argument parsing, crash handler, event loop."""
from __future__ import annotations
import argparse
import os
import sys

# Quieten the media backends BEFORE anything imports cv2 or Qt Multimedia:
# both read these at plugin-load time, so setting them later has no effect.
# Damaged or unusual files ("moov atom not found", "co located POCs
# unavailable", "Referenced QT chapter track not found") make FFmpeg chatty on
# stderr even though the app handles them; these keep the console readable.
os.environ.setdefault("OPENCV_LOG_LEVEL", "ERROR")
os.environ.setdefault("OPENCV_FFMPEG_LOGLEVEL", "-8")   # AV_LOG_QUIET
os.environ.setdefault("QT_LOGGING_RULES", "qt.multimedia.*=false")


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

    # Confirm the troubleshooting switches took effect.  Without this, silence
    # is ambiguous — "no stalls were detected" and "the variable never reached
    # the app" look identical.  (GALLERY_VIDEO_DIAG was replaced by
    # GALLERY_DIAG, which is announced after the QApplication exists.)
    if os.environ.get("GALLERY_LOOP") == "manual":
        print("[video] loop mode: MANUAL (restart on end-of-media)",
              file=sys.stderr)
    if os.environ.get("GALLERY_NO_GL") == "1":
        print("[video] GPU viewport DISABLED — video tiles use the raster path",
              file=sys.stderr)
    else:
        print("[video] GPU viewport enabled (set GALLERY_NO_GL=1 to disable)",
              file=sys.stderr)

    from PySide6.QtWidgets import QApplication
    from gallery_py_qt.main_window import MainWindow
    from gallery_py_qt.engine import prefs

    app = QApplication(sys.argv)
    app.setApplicationName("Gallery-Py-Qt")

    # GALLERY_DIAG=1 (or a ms threshold) watches the GUI thread and records
    # what was running whenever the event loop stalls.  Written to a file, not
    # stderr: PowerShell wraps native stderr in error records, which made shell
    # redirection unreliable.
    from gallery_py_qt import diag as _diag
    _thresh = _diag.enabled()
    if _thresh:
        _diag.install(app, _thresh)
        print(f"[diag] ENABLED — stalls over {_thresh}ms are logged to:\n"
              f"       {_diag.LOG_PATH}", file=sys.stderr)

    win = MainWindow()
    win.show()

    from PySide6.QtCore import QTimer
    # First-run feature guide, shown after the window has painted.
    QTimer.singleShot(0, win.maybe_show_welcome)

    target = None
    if args.folder and os.path.isdir(args.folder):
        target = args.folder
    if target:
        QTimer.singleShot(150, lambda: win.open_folder(target))

    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
