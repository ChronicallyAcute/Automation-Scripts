"""Search query language: parser + model integration."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt.engine.query import compile_query
from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


# -- parser -------------------------------------------------------------------
def _info(**kw):
    base = {"name": "", "w": 0, "h": 0, "type": "image", "fav": False,
            "tags": set()}
    base.update(kw)
    return base


def test_empty_query_is_none():
    assert compile_query("") is None
    assert compile_query("   ") is None


def test_substring():
    p = compile_query("beach")
    assert p(_info(name="my_beach.jpg"))
    assert not p(_info(name="mountain.jpg"))


def test_multiple_substrings_and():
    p = compile_query("sea sunset")
    assert p(_info(name="sea_sunset.jpg"))
    assert not p(_info(name="sea_only.jpg"))


def test_tag():
    p = compile_query("tag:BT")
    assert p(_info(tags={"bt"}))
    assert not p(_info(tags={"wam"}))


def test_fav():
    assert compile_query("fav:yes")(_info(fav=True))
    assert not compile_query("fav:yes")(_info(fav=False))
    assert compile_query("fav:no")(_info(fav=False))


def test_type():
    assert compile_query("type:video")(_info(type="video"))
    assert compile_query("type:vid")(_info(type="video"))
    assert not compile_query("type:image")(_info(type="gif"))


def test_numeric_dims():
    assert compile_query("w>1920")(_info(w=3840))
    assert not compile_query("w>1920")(_info(w=1280))
    assert compile_query("h<=1080")(_info(h=1080))
    assert not compile_query("w>1920")(_info(w=0))    # unknown never matches


def test_combined_and():
    p = compile_query("beach tag:WAM type:gif w>=500")
    assert p(_info(name="beach.gif", tags={"wam"}, type="gif", w=800))
    assert not p(_info(name="beach.gif", tags={"wam"}, type="gif", w=100))


def test_unknown_key_falls_back_to_substring():
    p = compile_query("foo:bar")
    assert p(_info(name="foo:bar_x.jpg"))       # treated as substring


# -- model integration --------------------------------------------------------
@pytest.fixture
def model(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    specs = {"a.jpg": (3840, 2160), "b.jpg": (800, 600), "c.gif": (500, 500)}
    paths = [str(tmp_path / n) for n in specs]
    for p, n in zip(paths, specs):
        Image.new("RGB", (16, 16)).save(p)
    m.set_paths(paths)
    m.set_dims({str(tmp_path / n): d for n, d in specs.items()})
    tags.toggle_tag(str(tmp_path / "a.jpg"), "BT")
    m._favs._paths.add(str(tmp_path / "b.jpg"))
    return m, tmp_path


def _names(m):
    return {os.path.basename(m.path_at(i)) for i in range(m.rowCount())}


def test_model_query_dims(model):
    m, _ = model
    m.set_filter(True, True, "w>1920", gifs=True)
    assert _names(m) == {"a.jpg"}


def test_model_query_tag(model):
    m, _ = model
    m.set_filter(True, True, "tag:BT", gifs=True)
    assert _names(m) == {"a.jpg"}


def test_model_query_fav(model):
    m, _ = model
    m.set_filter(True, True, "fav:yes", gifs=True)
    assert _names(m) == {"b.jpg"}


def test_model_query_type_gif(model):
    m, _ = model
    m.set_filter(True, True, "type:gif", gifs=True)
    assert _names(m) == {"c.gif"}


def test_model_query_combined_with_toolbar(model):
    m, _ = model
    # query says gif, but the GIF class toggle is off -> nothing
    m.set_filter(images=True, videos=True, query="type:gif", gifs=False)
    assert _names(m) == set()
