"""Tiles start staggered so their loop points don't collide.

Measured on real hardware: three tiles holding equal-length clips reached
end-of-media within 1-2ms of each other, every decoder seeked back to zero at
the same instant, and playback stalled 400-970ms across every tile at once.
Offsetting the starts spreads those seeks apart.
"""
from __future__ import annotations

from PIL import Image

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


class _FakePlayer:
    def __init__(self):
        self.plays = 0
    def play(self): self.plays += 1
    def duration(self): return 5000
    def __getattr__(self, _n): return lambda *a, **k: None


def _vid(tmp_path, name="c.mp4"):
    p = str(tmp_path / name)
    open(p, "wb").write(b"\x00" * 64)
    return p


def _img(tmp_path, name="a.jpg"):
    p = str(tmp_path / name)
    Image.new("RGB", (16, 16)).save(p)
    return p


def test_first_slot_starts_immediately(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s._player = _FakePlayer()
    s.show_item(0, _vid(tmp_path))
    assert s._player.plays == 1          # slot 0 has no delay
    s.deleteLater()


def test_later_slots_are_delayed(qapp, tmp_path):
    s = _Slot(2, Favorites())
    s._player = _FakePlayer()
    s.show_item(0, _vid(tmp_path))
    assert s._player.plays == 0          # waits for its offset
    assert s._PLAY_STAGGER_MS > 0
    s.deleteLater()


def test_delayed_start_fires_for_the_current_media(qapp, tmp_path):
    import time
    s = _Slot(1, Favorites())
    s._player = _FakePlayer()
    s.show_item(0, _vid(tmp_path))
    deadline = time.time() + 3
    while time.time() < deadline and s._player.plays == 0:
        qapp.processEvents()
        time.sleep(0.01)
    assert s._player.plays == 1
    s.deleteLater()


def test_destroying_a_slot_cancels_its_pending_start(qapp, tmp_path):
    """The crash this guards: a bare QTimer.singleShot(lambda: ...self...)
    outlived the slot, so an import that tore tiles down left a callback that
    touched an already-deleted C++ QMediaPlayer and killed the process.  A
    slot-owned timer dies with the slot."""
    import time
    from PySide6.QtCore import QTimer
    s = _Slot(4, Favorites())          # a late slot => long stagger delay
    s.show_item(0, _vid(tmp_path))
    assert s._play_timer.isActive()    # a start is pending
    # The timer is parented to the slot, so it goes when the slot does.
    assert s._play_timer.parent() is s
    s.deleteLater()
    qapp.processEvents()
    end = time.time() + 2.0            # outlive the stagger delay
    while time.time() < end:
        qapp.processEvents()
        time.sleep(0.01)               # must not crash the process


def test_pending_start_is_cancelled_by_new_media(qapp, tmp_path):
    import time
    s = _Slot(3, Favorites())
    s._player = _FakePlayer()
    s.show_item(0, _vid(tmp_path, "one.mp4"))
    s.show_item(1, _img(tmp_path))        # moved on before its turn
    end = time.time() + 1.5
    while time.time() < end:
        qapp.processEvents()
        time.sleep(0.01)
    assert s._player.plays == 0           # never started the abandoned clip
    s.deleteLater()


def test_clearing_cancels_a_pending_start(qapp, tmp_path):
    import time
    s = _Slot(3, Favorites())
    s._player = _FakePlayer()
    s.show_item(0, _vid(tmp_path))
    s.clear()
    end = time.time() + 1.5
    while time.time() < end:
        qapp.processEvents()
        time.sleep(0.01)
    assert s._player.plays == 0
    s.deleteLater()


# -- looping strategy ---------------------------------------------------------
def test_native_looping_by_default(qapp, monkeypatch):
    from PySide6.QtMultimedia import QMediaPlayer
    monkeypatch.delenv("GALLERY_LOOP", raising=False)
    s = _Slot(0, Favorites())
    assert s._manual_loop is False
    # EndOfMedia must NOT restart by hand: that raced Qt's own loop.
    s._player = _FakePlayer()
    s._is_video = True
    s._on_status(QMediaPlayer.MediaStatus.EndOfMedia)
    assert s._player.plays == 0
    s.deleteLater()


def test_manual_looping_restarts_on_end(qapp, monkeypatch):
    from PySide6.QtMultimedia import QMediaPlayer
    monkeypatch.setenv("GALLERY_LOOP", "manual")
    s = _Slot(0, Favorites())
    assert s._manual_loop is True
    s._player = _FakePlayer()
    s._is_video = True
    s._on_status(QMediaPlayer.MediaStatus.EndOfMedia)
    assert s._player.plays == 1          # exactly one restart, never two
    s.deleteLater()
