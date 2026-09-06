"""Animated GIFs must play (not show a single static frame) in both viewers."""
from __future__ import annotations

import pytest
from PySide6.QtGui import QPixmap

from gallery_py_qt.engine import media
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.gifplayer import GifPlayer
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import _Slot


# -- media type helpers --------------------------------------------------------
def test_is_gif_and_not_video():
    assert media.is_gif("/x/a.gif") and media.is_gif("/x/A.GIF")
    assert not media.is_gif("/x/a.png")
    assert not media.is_video("/x/a.gif")     # gif is an image type, not video


# -- GifPlayer -----------------------------------------------------------------
def test_gifplayer_pushes_frames(qapp, make_gif):
    gif = make_gif("anim.gif", frames=4)
    got = []
    gp = GifPlayer()
    assert gp.play(gif, lambda pm, first: got.append((pm, first))) is True
    assert gp.is_playing() and gp.path() == gif
    assert got and got[0][1] is True                  # first frame flagged, sync
    assert not got[0][0].isNull()
    gp.stop()
    assert not gp.is_playing() and gp.path() == ""


def test_gifplayer_rejects_non_animation(qapp, tmp_path):
    bogus = str(tmp_path / "not.gif")
    open(bogus, "wb").write(b"not really a gif")
    gp = GifPlayer()
    assert gp.play(bogus, lambda *a: None) is False    # caller falls back
    assert not gp.is_playing()


def test_gifplayer_pause_resume(qapp, make_gif):
    gp = GifPlayer()
    gp.play(make_gif("a.gif"), lambda *a: None)
    assert not gp.is_paused()
    gp.toggle_pause()
    assert gp.is_paused()
    gp.toggle_pause()
    assert not gp.is_paused()
    gp.stop()


def test_gifplayer_replay_same_path_is_noop(qapp, make_gif):
    gif = make_gif("a.gif")
    calls = []
    gp = GifPlayer()
    gp.play(gif, lambda *a: calls.append(1))
    mv1 = gp._movie
    gp.play(gif, lambda *a: calls.append(1))          # same path again
    assert gp._movie is mv1                            # kept the running movie
    gp.stop()


# -- multiview slot ------------------------------------------------------------
@pytest.fixture
def slot(qapp):
    return _Slot(0, Favorites())


def test_slot_animates_gif(slot, make_gif):
    gif = make_gif("anim.gif", frames=4)
    slot.show_item(0, gif)
    assert slot._gif.is_playing() and slot._gif.path() == gif
    assert not slot._is_video
    assert not slot._pm.isNull()                       # a frame reached _img
    assert slot._stack.currentIndex() == 0             # image page


def test_slot_switching_media_stops_gif(slot, make_gif, tmp_path):
    from PIL import Image
    slot.show_item(0, make_gif("anim.gif"))
    assert slot._gif.is_playing()
    still = str(tmp_path / "still.jpg")
    Image.new("RGB", (16, 16)).save(still)
    slot.show_item(1, still)
    assert not slot._gif.is_playing()                  # switched to a static image


def test_slot_clear_stops_gif(slot, make_gif):
    slot.show_item(0, make_gif("anim.gif"))
    assert slot._gif.is_playing()
    slot.clear()
    assert not slot._gif.is_playing()


def test_slot_static_gif_falls_back(slot, tmp_path):
    """A file with a .gif name that isn't a decodable animation still shows."""
    bogus = str(tmp_path / "broken.gif")
    open(bogus, "wb").write(b"not really an image at all")
    slot.show_item(0, bogus)
    assert not slot._gif.is_playing()                  # fell through to decode


# -- lightbox ------------------------------------------------------------------
@pytest.fixture
def lightbox(qapp, tmp_path, make_gif):
    from gallery_py_qt.lightbox import Lightbox
    m = GalleryModel(Favorites(), ThumbnailLoader())
    gif = make_gif("anim.gif", frames=4)
    from PIL import Image
    still = str(tmp_path / "still.jpg")
    Image.new("RGB", (32, 24)).save(still)
    m.set_paths([gif, still])
    m.set_dims({gif: (64, 48), still: (32, 24)})
    lb = Lightbox(m, Favorites())
    yield lb, gif, still
    lb.close()


def test_lightbox_animates_gif(lightbox):
    lb, gif, _still = lightbox
    lb.show_row(lb._model.row_for_path(gif))           # model may sort — find it
    assert lb._gif.is_playing() and lb._gif.path() == gif
    assert lb._stack.currentIndex() == 0               # image view
    assert not lb._transport.isVisible()               # no video transport
    assert not lb._img._item.pixmap().isNull()         # a frame is displayed


def test_lightbox_leaving_gif_stops_it(lightbox):
    lb, gif, still = lightbox
    lb.show_row(lb._model.row_for_path(gif))
    assert lb._gif.is_playing()
    lb.show_row(lb._model.row_for_path(still))          # navigate to a still
    assert not lb._gif.is_playing()


def test_lightbox_space_pauses_gif(lightbox):
    lb, gif, _still = lightbox
    lb.show_row(lb._model.row_for_path(gif))
    lb._toggle_play()
    assert lb._gif.is_paused()
    lb._toggle_play()
    assert not lb._gif.is_paused()
