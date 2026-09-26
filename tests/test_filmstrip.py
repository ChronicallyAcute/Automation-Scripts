"""Lightbox filmstrip: a thumbnail rail for jumping around the set."""
from __future__ import annotations
import time

from PIL import Image

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.lightbox import Lightbox
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


def _mk_lb(tmp_path, n=5):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"i{i}.jpg") for i in range(n)]
    for p in ps:
        Image.new("RGB", (16, 16)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (16, 16) for p in ps})
    return Lightbox(m, Favorites()), ps


def test_strip_hidden_until_toggled(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    assert lb._strip.isHidden()
    assert lb._strip_model.rowCount() == 0     # not even built yet
    lb.close()


def test_toggle_builds_and_shows_strip(qapp, tmp_path):
    lb, ps = _mk_lb(tmp_path)
    lb.show_row(0)
    lb._strip_btn.setChecked(True)
    lb._toggle_strip()
    assert not lb._strip.isHidden()
    assert lb._strip_model.rowCount() == len(ps)
    lb.close()


def test_clicking_a_thumb_navigates(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(0)
    lb._strip_btn.setChecked(True)
    lb._toggle_strip()
    idx = lb._strip_model.index(3, 0)
    lb._on_strip_clicked(idx)
    assert lb._row == 3
    lb.close()


def test_navigation_syncs_strip_selection(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(0)
    lb._strip_btn.setChecked(True)
    lb._toggle_strip()
    lb.next()                                   # 0 → 1
    assert lb._strip.currentIndex().row() == 1
    lb.close()


def test_keyboard_toggle_works(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(0)
    lb._kb_toggle_strip()
    assert lb._strip_btn.isChecked() and not lb._strip.isHidden()
    lb._kb_toggle_strip()
    assert not lb._strip_btn.isChecked() and lb._strip.isHidden()
    lb.close()


def test_thumbnails_populate(qapp, tmp_path):
    lb, ps = _mk_lb(tmp_path, n=3)
    lb.show_row(0)
    lb._strip_btn.setChecked(True)
    lb._toggle_strip()
    for _ in range(200):
        qapp.processEvents()
        time.sleep(0.005)
        if all(not lb._strip_items[p].icon().isNull() for p in ps):
            break
    assert all(not lb._strip_items[p].icon().isNull() for p in ps)
    lb.close()
