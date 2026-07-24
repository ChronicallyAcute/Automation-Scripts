"""GalleryView masonry layout + hit-testing, and MultiView orientation logic."""
from __future__ import annotations
import os
import random

import pytest
from PySide6.QtCore import QPoint
from PySide6.QtWidgets import QStackedWidget

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.gallery_view import GalleryView
from gallery_py_qt.multiview import MultiView


def _populate(model, tmp_path, specs):
    paths = [str(tmp_path / n) for n in specs]
    for p in paths:
        open(p, "wb").write(b"x")
    model.set_paths(paths)
    model.set_dims({str(tmp_path / n): d for n, d in specs.items()})
    return paths


# -- GalleryView masonry ------------------------------------------------------
def test_hit_testing_matches_bruteforce(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    specs = {f"i{i}.jpg": (400, 200 + (i % 5) * 120) for i in range(40)}
    _populate(m, tmp_path, specs)
    gv = GalleryView()
    gv.setModel(m)
    gv.set_columns(3)
    gv.resize(900, 700)
    gv.show()
    qapp.processEvents()
    gv._relayout()

    def brute(px, py):
        sy = gv.verticalScrollBar().value()
        Y = py + sy
        return [i for i, (x, y, w, h) in enumerate(gv._cells)
                if x <= px < x + w and y <= Y < y + h]

    rng = random.Random(1)
    for _ in range(200):
        gv.verticalScrollBar().setValue(
            rng.randint(0, max(0, gv.verticalScrollBar().maximum())))
        px, py = rng.randint(0, 899), rng.randint(0, 699)
        assert gv._row_at(QPoint(px, py)) == (brute(px, py) or [-1])[0]


def test_visible_cell_culling_matches_bruteforce(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    _populate(m, tmp_path, {f"i{i}.jpg": (400, 300) for i in range(60)})
    gv = GalleryView()
    gv.setModel(m)
    gv.set_columns(4)
    gv.resize(1000, 800)
    gv.show()
    qapp.processEvents()
    gv._relayout()

    def brute():
        sy = gv.verticalScrollBar().value()
        vh = gv.viewport().height()
        return sorted(i for i, (x, y, w, h) in enumerate(gv._cells)
                      if y + h > sy and y < sy + vh)

    rng = random.Random(2)
    for _ in range(30):
        gv.verticalScrollBar().setValue(
            rng.randint(0, max(0, gv.verticalScrollBar().maximum())))
        assert sorted(gv._visible_cell_rows()) == brute()


# -- MultiView orientation / layout -------------------------------------------
def _open_mv(qapp, model, start=0):
    mv = MultiView(model, Favorites())
    st = QStackedWidget()
    st.addWidget(mv)
    st.setCurrentWidget(mv)
    st.resize(1400, 900)
    st.show()
    qapp.processEvents()
    mv.open(start)
    qapp.processEvents()
    return mv, st


def test_portrait_media_uses_3x1(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    _populate(m, tmp_path, {f"v{i}.jpg": (600, 1000) for i in range(6)})
    mv, _st = _open_mv(qapp, m)
    assert mv._layout_slots == 3


def test_landscape_media_uses_2x2(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    _populate(m, tmp_path, {f"h{i}.jpg": (1400, 700) for i in range(4)})
    mv, _st = _open_mv(qapp, m)
    assert mv._layout_slots == 4


def test_forced_layout_resets_on_reopen(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    _populate(m, tmp_path, {f"v{i}.jpg": (600, 1000) for i in range(6)})
    mv, st = _open_mv(qapp, m)
    mv._cycle_layout()          # Auto -> 3x1
    mv._cycle_layout()          # 3x1 -> 2x2 (forced)
    assert mv._layout_slots == 4
    mv.open(0)                  # reopening resets to Auto
    qapp.processEvents()
    assert mv._forced_layout is None and mv._layout_slots == 3


def test_justified_tiles_hug_media_aspect(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    specs = {"v0.jpg": (600, 1000), "v1.jpg": (620, 1000), "v2.jpg": (560, 1000)}
    _populate(m, tmp_path, specs)
    mv, _st = _open_mv(qapp, m)
    mv._img_pool.waitForDone(4000)
    qapp.processEvents()
    for s in mv._active_slots():
        if not s._path:
            continue
        w, h = m.dim_at(s._path)
        g = s.geometry()
        assert abs(g.width() / g.height() - w / h) < 0.02


def test_tag_buttons_present_on_tiles(qapp, tmp_path):
    from gallery_py_qt.engine import tags
    m = GalleryModel(Favorites(), ThumbnailLoader())
    _populate(m, tmp_path, {f"v{i}.jpg": (600, 1000) for i in range(3)})
    mv, _st = _open_mv(qapp, m)
    assert sorted(mv._slots[0]._tag_btns) == sorted(tags.TAGS)
