"""Per-tile ± resize buttons and the auto-hiding multi-view volume slider."""
from __future__ import annotations

from PIL import Image

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


def _img(tmp_path, name):
    p = str(tmp_path / name)
    Image.new("RGB", (8, 8)).save(p)
    return p


class _FakeAudio:
    """Stand in for QAudioOutput so tests never open a real audio device."""
    def setVolume(self, *_): pass
    def setMuted(self, *_): pass


# -- per-tile zoom ------------------------------------------------------------
def test_tile_zoom_buttons_adjust_only_this_slot(qapp, tmp_path):
    s = _Slot(0, Favorites())
    assert s._zoom == 1.0
    s._tile_zoom_in_btn.click()
    assert round(s._zoom, 2) == 1.10
    s._tile_zoom_out_btn.click()
    s._tile_zoom_out_btn.click()
    assert round(s._zoom, 2) == 0.90


def test_tile_zoom_clamps_and_disables_at_limits(qapp):
    s = _Slot(0, Favorites())
    for _ in range(30):
        s._tile_zoom(-s._TILE_ZOOM_STEP)
    assert round(s._zoom, 2) == s._TILE_ZOOM_MIN
    assert not s._tile_zoom_out_btn.isEnabled()
    assert s._tile_zoom_in_btn.isEnabled()
    for _ in range(60):
        s._tile_zoom(+s._TILE_ZOOM_STEP)
    assert round(s._zoom, 2) == s._TILE_ZOOM_MAX
    assert not s._tile_zoom_in_btn.isEnabled()


def test_global_zoom_resyncs_tile_buttons(qapp):
    s = _Slot(0, Favorites())
    s.set_zoom(2.0)                       # as MultiView's global ± would
    assert not s._tile_zoom_in_btn.isEnabled()
    assert s._tile_zoom_out_btn.isEnabled()


# -- volume slider auto-hide --------------------------------------------------
def test_volume_slider_autohides_after_change(qapp):
    s = _Slot(0, Favorites())
    s._audio = _FakeAudio()               # avoid opening a real device headless
    s._is_video = True
    s._toggle_mute()                      # unmute → popup shown, stays put
    assert not s._vol_popup.isHidden()
    assert not s._vol_hide_timer.isActive()

    s._vol_slider.setValue(40)            # a change arms the auto-hide
    assert s._vol_hide_timer.isActive()
    s._vol_hide_timer.timeout.emit()      # simulate the timer firing
    qapp.processEvents()
    assert s._vol_popup.isHidden()


def test_volume_no_autohide_while_muted(qapp):
    s = _Slot(0, Favorites())
    s._audio = _FakeAudio()
    s._muted = True
    s._on_vol_changed(30)                 # e.g. a programmatic value set
    assert not s._vol_hide_timer.isActive()
