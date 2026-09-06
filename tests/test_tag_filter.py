"""Filter/browse the gallery by tag — model predicate + toolbar wiring."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


@pytest.fixture
def tagged_model(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    paths = [str(tmp_path / f"i{i}.jpg") for i in range(4)]
    for p in paths:
        Image.new("RGB", (32, 32)).save(p)
    m.set_paths(paths)
    m.set_dims({p: (100, 100) for p in paths})
    tags.toggle_tag(paths[0], "BT")
    tags.toggle_tag(paths[1], "BT")
    tags.toggle_tag(paths[1], "WAM")
    tags.toggle_tag(paths[2], "WAM")
    # paths[3] untagged
    return m, paths


def _visible(m):
    return {os.path.basename(m.path_at(i)) for i in range(m.rowCount())}


def test_no_tag_filter_shows_all(tagged_model):
    m, _ = tagged_model
    m.set_filter(True, True, "", tag_filter=set())
    assert _visible(m) == {"i0.jpg", "i1.jpg", "i2.jpg", "i3.jpg"}


def test_single_tag_filter(tagged_model):
    m, _ = tagged_model
    m.set_filter(True, True, "", tag_filter={"BT"})
    assert _visible(m) == {"i0.jpg", "i1.jpg"}


def test_any_match_union(tagged_model):
    m, _ = tagged_model
    m.set_filter(True, True, "", tag_filter={"BT", "WAM"}, tag_match_all=False)
    assert _visible(m) == {"i0.jpg", "i1.jpg", "i2.jpg"}


def test_all_match_intersection(tagged_model):
    m, _ = tagged_model
    m.set_filter(True, True, "", tag_filter={"BT", "WAM"}, tag_match_all=True)
    assert _visible(m) == {"i1.jpg"}          # only i1 has both


def test_tag_filter_combines_with_media_class(tagged_model, tmp_path):
    m, paths = tagged_model
    gif = str(tmp_path / "g.gif")
    Image.new("P", (16, 16)).save(gif)
    m.set_paths(paths + [gif])
    m.set_dims({gif: (100, 100)})
    tags.toggle_tag(gif, "BT")
    m.set_filter(images=True, videos=True, query="", gifs=False,
                 tag_filter={"BT"})
    assert _visible(m) == {"i0.jpg", "i1.jpg"}   # gif hidden by class filter


def test_toolbar_tag_filter_wiring(qapp, tmp_path):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    w.resize(1000, 700)
    w.show()
    qapp.processEvents()
    m = w._model
    paths = [str(tmp_path / f"i{i}.jpg") for i in range(3)]
    for p in paths:
        Image.new("RGB", (32, 32)).save(p)
    m.set_paths(paths)
    m.set_dims({p: (100, 100) for p in paths})
    tags.toggle_tag(paths[0], "Az")
    w._tag_filter_actions["Az"].setChecked(True)   # triggers _apply_filter
    qapp.processEvents()
    assert _visible(m) == {"i0.jpg"}
    assert "1" in w._tag_menu_btn.text()           # count badge
    w._clear_tag_filter()
    qapp.processEvents()
    assert m.rowCount() == 3 and w._tag_menu_btn.text() == "Tags ▾"
