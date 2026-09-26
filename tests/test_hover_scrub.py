"""Video hover-scrub: mouse-x over a video preview seeks its timeline."""
from __future__ import annotations

import pytest
from PySide6.QtCore import QEvent, QPoint, QRect

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.gallery_view import GalleryView


class _StubPool:
    """Records scrub/resume/play calls so we can assert the wiring without a
    real media backend (unavailable headless)."""
    def __init__(self):
        self.scrubs = []
        self.resumes = []
        self.played = set()

    def play(self, path):
        self.played.add(path)

    def scrub(self, path, frac):
        self.scrubs.append((path, round(frac, 3)))
        return True                       # pretend the video is loaded

    def resume(self, path):
        self.resumes.append(path)

    def stop_all(self):
        pass


@pytest.fixture
def view(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    paths = [str(tmp_path / "clip.mp4"), str(tmp_path / "pic.jpg")]
    for p in paths:
        open(p, "wb").write(b"\x00")
    m.set_paths(paths)
    m.set_dims({p: (400, 300) for p in paths})
    gv = GalleryView()
    gv.setModel(m)
    gv.set_columns(2)
    gv.resize(820, 400)
    gv.show()
    qapp.processEvents()
    gv._relayout()
    gv._vid_pool = _StubPool()
    return gv, paths


def _cell_center_x_frac(gv, row, frac):
    x, y, w, h = gv._cells[row]
    sy = gv.verticalScrollBar().value()
    return QPoint(int(x + frac * w), int(y - sy + h / 2))


def test_hover_over_video_scrubs(view):
    gv, paths = view
    vid_row = 0 if paths[0].endswith(".mp4") else 1
    gv._update_hover(_cell_center_x_frac(gv, vid_row, 0.25))
    gv._update_hover(_cell_center_x_frac(gv, vid_row, 0.75))
    assert gv._vid_pool.scrubs                     # scrub was called
    # frac tracks mouse-x (roughly 0.25 then 0.75)
    fracs = [f for _p, f in gv._vid_pool.scrubs]
    assert fracs[0] < 0.4 and fracs[-1] > 0.6
    assert gv._scrub_path == paths[vid_row]


def test_hover_over_image_does_not_scrub(view):
    gv, paths = view
    img_row = 1 if paths[1].endswith(".jpg") else 0
    gv._update_hover(_cell_center_x_frac(gv, img_row, 0.5))
    assert gv._vid_pool.scrubs == []
    assert gv._scrub_path is None


def test_leaving_video_resumes_playback(view):
    gv, paths = view
    vid_row = 0 if paths[0].endswith(".mp4") else 1
    gv._update_hover(_cell_center_x_frac(gv, vid_row, 0.5))
    assert gv._scrub_path is not None
    gv.leaveEvent(QEvent(QEvent.Type.Leave))
    assert paths[vid_row] in gv._vid_pool.resumes
    assert gv._scrub_path is None


def test_suspend_clears_scrub(view):
    gv, paths = view
    vid_row = 0 if paths[0].endswith(".mp4") else 1
    gv._update_hover(_cell_center_x_frac(gv, vid_row, 0.5))
    gv.suspend_video_previews()
    assert gv._scrub_path is None


def test_scrub_seeks_to_duration_fraction(qapp):
    """Unit-check the pool's scrub math with a fake player."""
    from gallery_py_qt.gallery_view import _VideoPreviewPool
    from PySide6.QtMultimedia import QMediaPlayer

    class FakePlayer:
        def __init__(self):
            self._pos = None
            self._state = QMediaPlayer.PlaybackState.PlayingState
        def duration(self):
            return 10000
        def playbackState(self):
            return self._state
        def pause(self):
            self._state = QMediaPlayer.PlaybackState.PausedState
        def setPosition(self, ms):
            self._pos = ms

    pool = _VideoPreviewPool(lambda *_: None)
    fp = FakePlayer()
    pool._used["v"] = (fp, object())
    assert pool.scrub("v", 0.3) is True
    assert fp._pos == 3000 and fp._state == QMediaPlayer.PlaybackState.PausedState
    assert pool.scrub("missing", 0.5) is False
