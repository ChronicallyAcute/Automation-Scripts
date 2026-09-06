"""Reposition sliders for media that overflows a multi-view tile (Fill / zoom),
for both images and video."""
from __future__ import annotations

import pytest
from PySide6.QtCore import QSizeF
from PySide6.QtGui import QColor, QImage, QPixmap

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _AspectLabel, _Slot


def _gradient(w, h):
    """A left→right red gradient so the shown crop is identifiable."""
    img = QImage(w, h, QImage.Format.Format_RGB32)
    for x in range(w):
        col = QColor(min(255, x), 0, 0)
        for y in range(h):
            img.setPixelColor(x, y, col)
    return QPixmap.fromImage(img)


# -- _AspectLabel offset -------------------------------------------------------
def test_overflow_detection(qapp):
    lbl = _AspectLabel()
    lbl.resize(100, 100)
    lbl.set_source(_gradient(200, 100))
    lbl.set_fill(True)                       # cover → overflows horizontally
    assert lbl.overflow() == (True, False)
    lbl.set_fill(False)
    lbl.set_zoom(1.0)                        # contain → fits
    assert lbl.overflow() == (False, False)


def test_zoom_over_100_overflows_both_axes(qapp):
    lbl = _AspectLabel()
    lbl.resize(100, 100)
    lbl.set_source(_gradient(100, 100))
    lbl.set_zoom(1.5)
    assert lbl.overflow() == (True, True)


def test_offset_pans_the_visible_crop(qapp):
    lbl = _AspectLabel()
    lbl.resize(100, 100)
    lbl.set_fill(True)
    lbl.set_source(_gradient(200, 100))
    lbl.set_offset(0.0, 0.5)                 # left edge
    left = lbl.pixmap().toImage().pixelColor(0, 50).red()
    lbl.set_offset(1.0, 0.5)                 # right edge
    right = lbl.pixmap().toImage().pixelColor(0, 50).red()
    assert left == 0 and right > left        # crop window moved rightwards


def test_offset_clamped(qapp):
    lbl = _AspectLabel()
    lbl.resize(100, 100)
    lbl.set_fill(True)
    lbl.set_source(_gradient(200, 100))
    lbl.set_offset(5.0, -3.0)                # out of range
    assert lbl._ox == 1.0 and lbl._oy == 0.0


# -- slot: images --------------------------------------------------------------
@pytest.fixture
def slot(qapp):
    s = _Slot(0, Favorites())
    s.resize(100, 100)
    return s


def test_slot_media_overflow_image(slot):
    slot._is_video = False
    slot._img.resize(100, 100)
    slot._img.set_fill(True)
    slot._img.set_source(_gradient(200, 100))
    assert slot._media_overflow() == (True, False)


def test_slot_set_pan_updates_image_offset(slot):
    slot._is_video = False
    slot._set_pan(0.2, 0.8)
    assert slot._ox == pytest.approx(0.2) and slot._oy == pytest.approx(0.8)
    assert slot._img._ox == pytest.approx(0.2) and slot._img._oy == pytest.approx(0.8)


def test_reset_pan_recentres(slot):
    slot._set_pan(0.1, 0.9)
    slot._reset_pan()
    assert slot._ox == 0.5 and slot._oy == 0.5


def test_show_item_recentres_pan(slot, tmp_path):
    from PIL import Image
    slot._set_pan(0.0, 1.0)
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (16, 16)).save(p)
    slot.show_item(0, p)
    assert slot._ox == 0.5 and slot._oy == 0.5


# -- slot: video ---------------------------------------------------------------
def test_slot_video_pan_moves_item(slot):
    slot._is_video = True
    slot._gview.resize(100, 100)
    slot._video_item.nativeSize = lambda: QSizeF(200, 100)   # wide video
    slot.set_fill(True)
    slot._fit_video()
    assert (slot._vid_ovx, slot._vid_ovy) == (True, False)
    slot._set_pan(0.0, 0.5)
    left_x = slot._video_item.pos().x()
    slot._set_pan(1.0, 0.5)
    right_x = slot._video_item.pos().x()
    assert left_x != right_x                 # the video frame was repositioned


# -- arrow-button wiring -------------------------------------------------------
def test_arrows_nudge_offset(slot):
    slot._reset_pan()
    slot._pan_right.click()
    assert slot._ox == pytest.approx(0.5 + slot._PAN_STEP)
    slot._pan_left.click()
    slot._pan_left.click()
    assert slot._ox == pytest.approx(0.5 - slot._PAN_STEP)
    slot._pan_up.click()
    assert slot._oy == pytest.approx(0.5 - slot._PAN_STEP)
    slot._pan_down.click()
    slot._pan_down.click()
    assert slot._oy == pytest.approx(0.5 + slot._PAN_STEP)


def test_arrows_clamp_at_edges(slot):
    slot._reset_pan()
    for _ in range(20):
        slot._pan_right.click()
    assert slot._ox == pytest.approx(1.0)
    for _ in range(20):
        slot._pan_up.click()
    assert slot._oy == pytest.approx(0.0)


def test_pan_arrows_shown_only_on_overflow_and_hover(slot):
    slot._is_video = False
    slot._img.resize(100, 100)
    slot._img.set_fill(True)
    slot._img.set_source(_gradient(200, 100))   # overflows horizontally only
    slot._btnbar.show()                          # simulate hover
    slot._position_overlays()
    assert not slot._pan_left.isHidden() and not slot._pan_right.isHidden()
    assert slot._pan_up.isHidden() and slot._pan_down.isHidden()
    slot._btnbar.hide()
    slot._position_overlays()
    assert all(b.isHidden() for b in slot._pan_btns)
