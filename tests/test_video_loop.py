"""Short looping clips must not stutter, and a hiccup must not blacklist a file.

Two bugs this pins down:
  * the players set setLoops(Infinite) AND manually seeked to 0 on EndOfMedia,
    so two restarts raced at every loop boundary — a visible lag on a 5-second
    clip, and the extra seek landed off a keyframe (the h264 "co located POCs
    unavailable" complaints);
  * any errorOccurred tore the video down and marked the file permanently
    unplayable, so one transient decode glitch also killed its thumbnail and
    dimensions.
"""
from __future__ import annotations

from PIL import Image
from PySide6.QtMultimedia import QMediaPlayer

from gallery_py_qt.engine import media
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.lightbox import Lightbox
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import _Slot

ERR = QMediaPlayer.Error.ResourceError


class _FakePlayer:
    """Records seeks/plays so a loop boundary can be asserted on."""
    def __init__(self, duration=5000):
        self._duration = duration
        self.seeks, self.plays = [], 0
        self.loops = None
        self.source_cleared = False

    def duration(self): return self._duration
    def setPosition(self, ms): self.seeks.append(ms)
    def play(self): self.plays += 1
    def stop(self): pass
    def setSource(self, *_): self.source_cleared = True
    def setLoops(self, n): self.loops = n
    def playbackState(self): return QMediaPlayer.PlaybackState.PlayingState
    def __getattr__(self, _n): return lambda *a, **k: None


class _FakeSink:
    """Minimal stand-in for QVideoSink: only disconnect() is exercised."""
    class _Sig:
        def disconnect(self): pass
        def connect(self, *_): pass
    videoFrameChanged = _Sig()


def _vid(tmp_path, name="clip.mp4"):
    p = str(tmp_path / name)
    open(p, "wb").write(b"\x00" * 64)
    return p


# -- 1. no double restart at the loop point -----------------------------------
def test_slot_does_not_seek_on_end_of_media(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s._player = _FakePlayer()
    s._is_video = True
    s._path = _vid(tmp_path)
    s._on_status(QMediaPlayer.MediaStatus.EndOfMedia)
    assert s._player.seeks == []          # native looping handles it
    assert s._player.plays == 0
    s.deleteLater()


def test_slot_still_fails_on_invalid_media(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s._player = _FakePlayer(duration=0)
    s._is_video = True
    s._path = _vid(tmp_path)
    s._on_status(QMediaPlayer.MediaStatus.InvalidMedia)
    assert not s._is_video                # fell back to the "can't play" note
    s.deleteLater()


def _lb(tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    p = str(tmp_path / "i0.jpg")
    Image.new("RGB", (16, 16)).save(p)
    m.set_paths([p])
    return Lightbox(m, Favorites())


def test_lightbox_does_not_seek_on_end_of_media(qapp, tmp_path):
    lb = _lb(tmp_path)
    lb._ensure_player()
    lb._player = _FakePlayer()
    lb._slideshow_on = False
    lb._on_media_status(QMediaPlayer.MediaStatus.EndOfMedia)
    assert lb._player.seeks == []
    lb.close()


def test_slideshow_still_advances_on_end_of_media(qapp, tmp_path):
    lb = _lb(tmp_path)
    lb._ensure_player()
    lb._player = _FakePlayer()
    lb._slideshow_on = True
    advanced = []
    lb._slide_advance = lambda: advanced.append(True)
    lb._on_media_status(QMediaPlayer.MediaStatus.EndOfMedia)
    assert advanced == [True]             # the one case that must still act
    lb.close()


# -- 2. transient errors don't blacklist --------------------------------------
def test_error_after_playback_started_is_transient(qapp, tmp_path):
    path = _vid(tmp_path)
    s = _Slot(0, Favorites())
    s._player = _FakePlayer(duration=5000)   # it played: duration is known
    s._is_video = True
    s._path = path
    s._on_media_error(ERR, "co located POCs unavailable")
    assert s._is_video                       # tile keeps playing
    assert not media.is_bad_video(path)      # and stays usable elsewhere
    s.deleteLater()


def test_error_before_any_playback_marks_bad(qapp, tmp_path):
    path = _vid(tmp_path)
    s = _Slot(0, Favorites())
    s._player = _FakePlayer(duration=0)      # never got going
    s._is_video = True
    s._path = path
    s._dur_ms = 0
    s._on_media_error(ERR, "moov atom not found")
    assert not s._is_video
    assert media.is_bad_video(path)
    s.deleteLater()


def test_hover_pool_keeps_a_file_that_already_played(qapp, tmp_path):
    from gallery_py_qt.gallery_view import _VideoPreviewPool
    path = _vid(tmp_path)
    pool = _VideoPreviewPool(lambda *a: None)
    player = _FakePlayer(duration=5000)
    pool._used[path] = (player, _FakeSink())
    pool._on_error(player, ERR)
    assert not media.is_bad_video(path)      # transient: still usable
    assert path not in pool._used            # but the preview was released


def test_hover_pool_blacklists_a_file_that_never_played(qapp, tmp_path):
    from gallery_py_qt.gallery_view import _VideoPreviewPool
    path = _vid(tmp_path)
    pool = _VideoPreviewPool(lambda *a: None)
    player = _FakePlayer(duration=0)
    pool._used[path] = (player, _FakeSink())
    pool._on_error(player, ERR)
    assert media.is_bad_video(path)
