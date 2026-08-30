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
