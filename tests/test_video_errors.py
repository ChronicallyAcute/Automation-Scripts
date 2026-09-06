"""A corrupt/unplayable video degrades gracefully instead of hanging the pane.

Both the multi-view tile and the full-screen viewer connect QMediaPlayer's
errorOccurred; on a real error they release the source (so the FFmpeg backend
stops retrying), remember the file as bad, and show a note.
"""
from __future__ import annotations

import pytest
from PIL import Image

from PySide6.QtMultimedia import QMediaPlayer

from gallery_py_qt import config
from gallery_py_qt.engine import media
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot
from gallery_py_qt.lightbox import Lightbox
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


ERR = QMediaPlayer.Error.ResourceError
NOERR = QMediaPlayer.Error.NoError


# -- multi-view tile ----------------------------------------------------------
def test_slot_media_error_falls_back(qapp, tmp_path):
    path = str(tmp_path / "broken.mp4")
    open(path, "wb").write(b"\x00" * 32)
    s = _Slot(0, Favorites())
    s._is_video = True
    s._path = path
    s._stack.setCurrentIndex(1)
    s._on_media_error(ERR, "moov atom not found")
    assert not s._is_video
    assert s._stack.currentIndex() == 0            # switched off the video page
    assert media.is_bad_video(path)                # remembered as unplayable


def test_slot_noerror_is_ignored(qapp, tmp_path):
    path = str(tmp_path / "ok.mp4")
    open(path, "wb").write(b"\x00" * 32)
    s = _Slot(0, Favorites())
    s._is_video = True
    s._path = path
    s._on_media_error(NOERR)                       # spurious NoError signal
    assert s._is_video                             # untouched
    assert not media.is_bad_video(path)


# -- full-screen viewer -------------------------------------------------------
def test_lightbox_media_error_shows_note(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    vid = str(tmp_path / "clip.mp4")
    open(vid, "wb").write(b"\x00" * 32)
    m.set_paths([vid])
    box = Lightbox(m, Favorites())
    box._row = 0
    box._ensure_player()
    box._stack.setCurrentIndex(1)
    box._on_media_error(ERR, "moov atom not found")
    assert media.is_bad_video(vid)
    assert not box._loading_lbl.isHidden()
    assert "can't be played" in box._loading_lbl.text()
    box.close()
