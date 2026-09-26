"""GUI-thread stall watchdog: names what was blocking, writes to a file."""
from __future__ import annotations
import os
import time

import pytest
from PySide6.QtCore import QTimer

from gallery_py_qt import diag


@pytest.fixture(autouse=True)
def _log(tmp_path, monkeypatch):
    monkeypatch.setattr(diag, "LOG_PATH", str(tmp_path / "diag.log"))
    yield


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("GALLERY_DIAG", raising=False)
    assert diag.enabled() is None


def test_enabled_forms(monkeypatch):
    monkeypatch.setenv("GALLERY_DIAG", "1")
    assert diag.enabled() == 120                 # default threshold
    monkeypatch.setenv("GALLERY_DIAG", "250")
    assert diag.enabled() == 250                 # explicit ms
    monkeypatch.setenv("GALLERY_DIAG", "5")
    assert diag.enabled() == 30                  # floored, never absurd
    monkeypatch.setenv("GALLERY_DIAG", "junk")
    assert diag.enabled() == 120                 # unparseable → default


def test_write_appends_timestamped_lines():
    diag.write("hello")
    diag.write("world")
    text = open(diag.LOG_PATH, encoding="utf-8").read()
    assert "hello" in text and "world" in text
    assert text.count("[") >= 2                  # one stamp per entry


def test_watchdog_records_the_blocking_function(qapp):
    diag.install(qapp, threshold_ms=100)

    def a_slow_slot():
        time.sleep(0.35)                         # block the event loop

    QTimer.singleShot(50, a_slow_slot)
    deadline = time.time() + 5
    while time.time() < deadline:
        qapp.processEvents()
        time.sleep(0.01)
        text = open(diag.LOG_PATH, encoding="utf-8").read()
        if "GUI THREAD BLOCKED" in text:
            break
    text = open(diag.LOG_PATH, encoding="utf-8").read()
    assert "GUI THREAD BLOCKED" in text
    assert "a_slow_slot" in text                 # the culprit is named


def test_healthy_loop_records_no_stall(qapp):
    diag.install(qapp, threshold_ms=400)
    end = time.time() + 1.0
    while time.time() < end:
        qapp.processEvents()
        time.sleep(0.01)
    text = open(diag.LOG_PATH, encoding="utf-8").read()
    assert "GUI THREAD BLOCKED" not in text


def test_watchdog_stops_on_uninstall(qapp):
    """A watchdog left running into teardown reads frames of threads that are
    being destroyed — the long-standing intermittent crash in this suite."""
    from gallery_py_qt import diag
    diag.install(qapp, 120)
    assert diag.is_running()
    diag.uninstall()
    assert not diag.is_running()


def test_install_replaces_rather_than_stacks(qapp):
    from gallery_py_qt import diag
    import threading
    before = sum(1 for t in threading.enumerate() if t.name == "stall-watchdog")
    for _ in range(3):
        diag.install(qapp, 120)
    after = sum(1 for t in threading.enumerate()
                if t.name == "stall-watchdog" and t.is_alive())
    diag.uninstall()
    assert after <= before + 1, "each install must retire the previous watchdog"


def test_uninstall_is_safe_when_nothing_is_running(qapp):
    from gallery_py_qt import diag
    diag.uninstall()
    diag.uninstall()
    assert not diag.is_running()
