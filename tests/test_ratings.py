"""Star ratings: store, query, sort, lightbox UI, settings dialog."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt.engine import ratings
from gallery_py_qt.engine.query import compile_query
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


# -- store --------------------------------------------------------------------
def test_set_and_get(make_image):
    p = make_image("a.jpg")
    assert ratings.rating_of(p) == 0
    assert ratings.set_rating(p, 4) == 4
    assert ratings.rating_of(p) == 4


def test_clamped():
    assert ratings.set_rating("x", 9) == ratings.MAX_STARS
    assert ratings.set_rating("x", -3) == 0


def test_zero_removes(make_image):
    p = make_image("a.jpg")
    ratings.set_rating(p, 3)
    ratings.set_rating(p, 0)
    assert ratings.rating_of(p) == 0
    assert p not in ratings._load()


def test_cycle(make_image):
    p = make_image("a.jpg")
    assert ratings.cycle_rating(p, 3) == 3
    assert ratings.cycle_rating(p, 3) == 0     # same star clears
    assert ratings.cycle_rating(p, 5) == 5


def test_persists(make_image):
    p = make_image("a.jpg")
    ratings.set_rating(p, 2)
    ratings._store = None                       # force reload from disk
    assert ratings.rating_of(p) == 2


# -- query --------------------------------------------------------------------
def test_query_rating():
    p = compile_query("rating>=3")
    assert p({"rating": 4})
    assert not p({"rating": 2})
    assert compile_query("rating=0")({"rating": 0})    # unrated matches


# -- model sort + filter ------------------------------------------------------
@pytest.fixture
def model(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"i{i}.jpg") for i in range(3)]
    for p in ps:
        Image.new("RGB", (16, 16)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (100, 100) for p in ps})
    ratings.set_rating(ps[0], 2)
    ratings.set_rating(ps[1], 5)
    return m, ps


def test_sort_by_rating(model):
    m, ps = model
    m.set_sort_chain(["rating", "name"])
    assert os.path.basename(m.path_at(0)) == "i1.jpg"      # 5 stars first
    assert os.path.basename(m.path_at(1)) == "i0.jpg"      # 2 stars


def test_filter_by_rating_query(model):
    m, _ = model
    m.set_filter(True, True, "rating>=4")
    assert {os.path.basename(m.path_at(i)) for i in range(m.rowCount())} \
        == {"i1.jpg"}


# -- lightbox UI --------------------------------------------------------------
def test_lightbox_stars(qapp, tmp_path):
    from gallery_py_qt.lightbox import Lightbox
    m = GalleryModel(Favorites(), ThumbnailLoader())
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (64, 48)).save(p)
    m.set_paths([p])
    m.set_dims({p: (64, 48)})
    lb = Lightbox(m, Favorites())
    lb.show()
    lb.show_row(0)
    qapp.processEvents()
    assert len(lb._star_btns) == 5
    lb._set_rating(3)
    assert ratings.rating_of(p) == 3
    assert [b.text() for b in lb._star_btns] == ["★", "★", "★", "☆", "☆"]
    lb._set_rating(3)                            # click same -> clear
    assert ratings.rating_of(p) == 0
    lb.close()


# -- settings dialog ----------------------------------------------------------
def test_settings_dialog_result(qapp):
    from gallery_py_qt.settings_dialog import SettingsDialog
    dlg = SettingsDialog({"theme": "dark", "link_mode": "copy",
                          "low_io_mode": None, "trash_purge_days": 0})
    dlg._select(dlg._link, "symlink")
    dlg._purge.setValue(30)
    out = dlg.result_prefs()
    assert out["link_mode"] == "symlink"
    assert out["trash_purge_days"] == 30
    assert out["low_io_mode"] is None
