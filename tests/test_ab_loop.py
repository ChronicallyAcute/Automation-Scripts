"""A–B video loop: right-click the scrubber to loop between two timepoints."""
from __future__ import annotations

import pytest
from PySide6.QtCore import Qt, QPointF, QEvent
from PySide6.QtGui import QMouseEvent
from PIL import Image

from gallery_py_qt.seekbar import SeekBar
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


class _FakePlayer:
    """Just enough player: real position/duration, everything else a no-op so
    show_item()/show_row()'s stop()/setSource()/play() calls don't blow up."""
    def __init__(self, dur=10000):
        self._pos = 0
        self._dur = dur
    def position(self):
        return self._pos
    def duration(self):
        return self._dur
    def setPosition(self, ms):
        self._pos = int(ms)
    def __getattr__(self, _name):
        return lambda *a, **k: None


# -- SeekBar state machine -----------------------------------------------------
def test_right_click_cycles_a_b_clear(qapp):
    sb = SeekBar()
    sb.set_duration(10000)
    seen = []
    sb.loopChanged.connect(lambda a, b: seen.append((round(a, 2), round(b, 2))))

    sb._cycle_loop(0.2)                       # A
    assert sb.loop_points() is None           # not complete until B
    assert seen[-1] == (0.2, -1.0)

    sb._cycle_loop(0.8)                        # B
    assert sb.loop_points() == pytest.approx((0.2, 0.8))
    assert seen[-1] == (0.2, 0.8)

    sb._cycle_loop(0.5)                        # clear
    assert sb.loop_points() is None
    assert seen[-1] == (-1.0, -1.0)


def test_b_before_a_is_swapped(qapp):
    sb = SeekBar()
    sb._cycle_loop(0.7)
    sb._cycle_loop(0.3)
    assert sb.loop_points() == pytest.approx((0.3, 0.7))


def test_clear_loop_is_silent(qapp):
    sb = SeekBar()
    sb._cycle_loop(0.2)
    sb._cycle_loop(0.8)
    fired = []
    sb.loopChanged.connect(lambda a, b: fired.append((a, b)))
    sb.clear_loop()
    assert fired == []                        # programmatic clear doesn't emit
    assert sb.loop_points() is None


def test_right_mouse_press_sets_a_point(qapp):
    sb = SeekBar()
    sb.resize(200, 12)
    sb.set_duration(10000)
    pos = QPointF(100, 6)                      # middle → ~0.5
    ev = QMouseEvent(QEvent.Type.MouseButtonPress, pos, pos,
                     Qt.MouseButton.RightButton, Qt.MouseButton.RightButton,
                     Qt.KeyboardModifier.NoModifier)
    sb.mousePressEvent(ev)
    a = sb._loop_a
    assert a is not None and 0.45 < a < 0.55


def test_left_click_still_seeks_not_loops(qapp):
    sb = SeekBar()
    sb.resize(200, 12)
    sb.set_duration(10000)
    seeks = []
    sb.seeked.connect(seeks.append)
    pos = QPointF(50, 6)
    ev = QMouseEvent(QEvent.Type.MouseButtonPress, pos, pos,
                     Qt.MouseButton.LeftButton, Qt.MouseButton.LeftButton,
                     Qt.KeyboardModifier.NoModifier)
    sb.mousePressEvent(ev)
    assert seeks and sb.loop_points() is None


def test_paint_with_loop_does_not_crash(qapp):
    sb = SeekBar()
    sb.resize(200, 12)
    sb._cycle_loop(0.2)
    sb.repaint()                               # only A
    sb._cycle_loop(0.8)
    sb.repaint()                               # A + band
    sb.clear_loop()
    sb.repaint()


# -- multiview slot enforcement ------------------------------------------------
@pytest.fixture
def slot(qapp):
    s = _Slot(0, Favorites())
    s._player = _FakePlayer(10000)
    s._dur_ms = 10000
    return s


def test_slot_loops_back_to_a_at_b(slot):
    slot._on_loop_changed(0.2, 0.8)
    assert (slot._loop_a_ms, slot._loop_b_ms) == (2000, 8000)
    # Before B: no snap.
    slot._player.setPosition(5000)
    slot._on_pos(5000)
    assert slot._player.position() == 5000
    # Reaching B: snap back to A.
    slot._player.setPosition(8100)
    slot._on_pos(8100)
    assert slot._player.position() == 2000


def test_slot_clear_stops_looping(slot):
    slot._on_loop_changed(0.2, 0.8)
    slot._on_loop_changed(-1.0, -1.0)
    assert slot._loop_a_ms is None and slot._loop_b_ms is None
    slot._player.setPosition(9000)
    slot._on_pos(9000)
    assert slot._player.position() == 9000     # played through, no snap


def test_partial_loop_a_only_does_not_enforce(slot):
    slot._on_loop_changed(0.3, -1.0)           # only A set
    assert slot._loop_a_ms == 3000 and slot._loop_b_ms is None
    slot._player.setPosition(9000)
    slot._on_pos(9000)
    assert slot._player.position() == 9000


def test_new_item_resets_loop(slot, tmp_path):
    slot._on_loop_changed(0.2, 0.8)
    assert slot._loop_a_ms is not None
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    slot.show_item(0, p)
    assert slot._loop_a_ms is None and slot._loop_b_ms is None
    assert slot._scrub.loop_points() is None


def test_setting_loop_while_past_b_snaps_immediately(slot):
    slot._player.setPosition(9000)
    slot._on_loop_changed(0.2, 0.8)
    assert slot._player.position() == 2000     # yanked into the span at once


# -- lightbox enforcement ------------------------------------------------------
@pytest.fixture
def lightbox(qapp, tmp_path):
    from gallery_py_qt.lightbox import Lightbox
    from gallery_py_qt.model import GalleryModel
    from gallery_py_qt.loader import ThumbnailLoader
    m = GalleryModel(Favorites(), ThumbnailLoader())
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (32, 24)).save(p)
    m.set_paths([p])
    m.set_dims({p: (32, 24)})
    lb = Lightbox(m, Favorites())
    lb._player = _FakePlayer(10000)
    lb._dur_ms = 10000
    yield lb
    lb.close()


def test_lightbox_loops_back_to_a_at_b(lightbox):
    lightbox._on_loop_changed(0.25, 0.75)
    assert (lightbox._loop_a_ms, lightbox._loop_b_ms) == (2500, 7500)
    lightbox._on_pos(3000)
    assert lightbox._player.position() == 0    # a notification only, no snap yet
    lightbox._player.setPosition(7600)
    lightbox._on_pos(7600)
    assert lightbox._player.position() == 2500
    assert "Looping" in lightbox._time.toolTip()


def test_lightbox_show_row_resets_loop(lightbox):
    lightbox._loop_a_ms, lightbox._loop_b_ms = 1000, 2000
    lightbox.show_row(0)
    assert lightbox._loop_a_ms is None and lightbox._loop_b_ms is None
    assert lightbox._scrub.loop_points() is None
    assert lightbox._time.toolTip() == ""
