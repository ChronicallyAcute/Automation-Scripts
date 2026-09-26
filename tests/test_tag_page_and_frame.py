"""Tag-the-whole-page in multi-view, and saving a video frame as an image."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt.engine import media, tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import MultiView


def _mv(qapp, tmp_path, n=4):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"i{i}.jpg") for i in range(n)]
    for p in ps:
        Image.new("RGB", (32, 24)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (32, 24) for p in ps})
    v = MultiView(m, Favorites())
    v.open(0)
    return v, ps


# -- tag the whole page -------------------------------------------------------
def test_page_paths_lists_visible_tiles(qapp, tmp_path):
    v, ps = _mv(qapp, tmp_path)
    shown = v.page_paths()
    assert shown and set(shown) <= set(ps)
    assert len(shown) == len(set(shown))          # distinct
    v.deleteLater()


def test_tag_page_applies_to_every_tile(qapp, tmp_path):
    v, _ = _mv(qapp, tmp_path)
    shown = v.page_paths()
    t = tags.get_tags()[0]
    v._tag_page(t, True)
    assert all(t in tags.tags_for(p) for p in shown)
    v.deleteLater()


def test_tag_page_removes_when_all_tagged(qapp, tmp_path):
    v, _ = _mv(qapp, tmp_path)
    shown = v.page_paths()
    t = tags.get_tags()[0]
    v._tag_page(t, True)
    v._tag_page(t, False)
    assert all(t not in tags.tags_for(p) for p in shown)
    v.deleteLater()


def test_tagall_menu_offers_remove_once_all_tagged(qapp, tmp_path):
    v, _ = _mv(qapp, tmp_path)
    t = tags.get_tags()[0]
    v._build_tagall_menu()
    assert any(f"Apply “{t}”" in a.text() for a in v._tagall_menu.actions())
    v._tag_page(t, True)
    v._build_tagall_menu()
    assert any(f"Remove “{t}”" in a.text() for a in v._tagall_menu.actions())
    v.deleteLater()


# -- save a video frame -------------------------------------------------------
def test_save_frame_on_a_bad_video_returns_false(tmp_path):
    bad = str(tmp_path / "broken.mp4")
    open(bad, "wb").write(b"\x00" * 64)
    assert media.frame_at_ms(bad, 0) is None
    assert not media.save_frame(bad, 0, str(tmp_path / "out.png"))


def test_save_frame_ignores_non_video(tmp_path):
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    assert media.frame_at_ms(p, 0) is None


@pytest.mark.skipif(not media.HAS_CV2, reason="opencv not installed")
def test_save_frame_writes_a_real_image(tmp_path):
    """Build a tiny video with OpenCV, then extract a frame back out of it."""
    import cv2, numpy as np
    src = str(tmp_path / "clip.mp4")
    w, h = 64, 48
    writer = cv2.VideoWriter(src, cv2.VideoWriter_fourcc(*"mp4v"), 10.0, (w, h))
    if not writer.isOpened():
        pytest.skip("no mp4 encoder available")
    for _ in range(10):
        writer.write(np.full((h, w, 3), 128, dtype=np.uint8))
    writer.release()

    dest = str(tmp_path / "frame.png")
    assert media.save_frame(src, 0, dest)
    assert os.path.getsize(dest) > 0
    with Image.open(dest) as im:
        assert im.size == (w, h)          # full source resolution, not scaled
