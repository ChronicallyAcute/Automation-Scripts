"""Crash logging: route uncaught Python and Qt errors to a log file."""
from __future__ import annotations
import datetime, sys, traceback

from . import config


def log_crash(kind: str, exc: BaseException | None = None, extra: str = "") -> None:
    try:
        stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        body = ""
        if exc is not None:
            body = "".join(traceback.format_exception(type(exc), exc,
                                                       exc.__traceback__))
        with open(config.CRASH_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*70}\n[{stamp}] {kind}\n{extra}\n{body}")
        print(f"[gallery-py-qt] {kind}: {exc or extra}  (logged to {config.CRASH_LOG})",
              file=sys.stderr)
    except Exception:
        pass


def install() -> None:
    def _hook(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        log_crash("uncaught exception", exc_value)
    sys.excepthook = _hook

    try:
        from PySide6 import QtCore

        noise = ("ffmpeg", "hwaccel", "d3d11", "av1", "pixel format",
                 "get current frame", "vaapi", "dxva", "cuvid")

        def _qt_handler(mode, ctx, message):
            msg = str(message)
            low = msg.lower()
            cat = ""
            try:
                cat = (ctx.category or "")
            except Exception:
                pass
            if mode == QtCore.QtMsgType.QtFatalMsg:
                log_crash("qt fatal", extra=msg)
                return
            if mode == QtCore.QtMsgType.QtCriticalMsg:
                if cat.startswith("qt.multimedia") or any(k in low for k in noise):
                    return
                log_crash("qt critical", extra=msg)
        QtCore.qInstallMessageHandler(_qt_handler)
    except Exception:
        pass
