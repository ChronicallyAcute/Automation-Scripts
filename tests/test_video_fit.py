"""Mid-playback jitter: don't reconfigure the video surface for nothing.

Resizing a QGraphicsVideoItem makes the media backend rebuild its output
surface, which stutters visibly while a clip is playing.  _fit_video ran on
every layout pass — hover in/out, panning, tag refresh, the chrome auto-hide,
zoom — and re-applied the SAME geometry each time.  On a 3x2 page of videos
that was a burst of needless reconfigurations a couple of seconds into
playback, which is what the jitter was.
"""
from __future__ import annotations

from PySide6.QtCore import QSizeF

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


class _CountingItem:
    """Stands in for QGraphicsVideoItem, counting surface reconfigurations."""
    def __init__(self, w=512.0, h=768.0):
        self.resizes = 0
        self.moves = 0
        self._native = QSizeF(w, h)
        self._size = QSizeF(0, 0)
        self._pos = (0.0, 0.0)

    def nativeSize(self): return self._native
    def setSize(self, sz): self.resizes += 1; self._size = sz
    def setPos(self, x, y): self.moves += 1; self._pos = (x, y)
    def __getattr__(self, _n): return lambda *a, **k: None


def _slot(qapp, w=400, h=300):
    s = _Slot(0, Favorites())
    s._is_video = True
    s._video_item = _CountingItem()
    s._gview.resize(w, h)
    return s


def test_repeated_fit_with_no_change_does_not_resize(qapp):
    s = _slot(qapp)
    s._fit_video()
    assert s._video_item.resizes == 1          # first fit applies
    for _ in range(25):                        # hover / pan / tag churn
        s._fit_video()
    assert s._video_item.resizes == 1          # …and never again
    s.deleteLater()


def test_a_real_size_change_still_applies(qapp):
    s = _slot(qapp)
    s._fit_video()
    before = s._video_item.resizes
    s._gview.resize(800, 600)                  # e.g. the chrome auto-hid
    s._fit_video()
    assert s._video_item.resizes == before + 1
    s.deleteLater()


def test_zoom_and_pan_changes_still_apply(qapp):
    s = _slot(qapp)
    s._fit_video()
    n = s._video_item.resizes
    s.set_zoom(1.5)                            # zoom changes the display size
    assert s._video_item.resizes > n
    n = s._video_item.resizes
    s._set_pan(0.0, 0.0)                       # pan changes position
    assert s._video_item.moves > 0
    s.deleteLater()


def test_new_media_forces_a_refit(qapp, tmp_path):
    s = _slot(qapp)
    s._fit_video()
    assert s._last_vid_geom is not None
    s.clear()
    assert s._last_vid_geom is None             # cache dropped with the media
    s.deleteLater()


# -- per-frame transport updates ----------------------------------------------
def test_time_label_only_repaints_once_a_second(qapp):
    s = _slot(qapp)
    s._player = type("P", (), {"setPosition": lambda *a: None})()
    painted = []
    s._time.setText = lambda t: painted.append(t)
    for ms in range(0, 1000, 50):               # 20 updates inside one second
        s._on_pos(ms)
    assert len(painted) == 1
    s._on_pos(1000)                             # crossing into the next second
    assert len(painted) == 2
    s.deleteLater()
