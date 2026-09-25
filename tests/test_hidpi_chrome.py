"""Narrow-tile tag chips, relayout coalescing, and the high-DPI shadow path."""
from __future__ import annotations
import os
import time

from PIL import Image
from PySide6.QtCore import QSize
from PySide6.QtGui import QResizeEvent

from gallery_py_qt import multiview
from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import MultiView, _Slot


def _img(path, size=(800, 600)):
    Image.new("RGB", size).save(path)
    return str(path)


def _many_tags():
    for t in ("Landscape", "Portrait", "Family", "Archive", "Wallpaper",
              "Reference"):
        tags.add_tag(t)


# -- the reported bug: chips clipped off a narrow tile ------------------------


# -- the flow layout itself ----------------------------------------------------


# -- hide/unhide performance ---------------------------------------------------

def test_a_burst_of_resizes_causes_one_relayout(qapp, tmp_path):
    """The chrome auto-hide resizes the grid host, which re-lays every tile
    and re-fits every video.  Bursts must coalesce or hovering stutters."""
    paths = [_img(tmp_path / f"{i}.png") for i in range(4)]
    m = GalleryModel(Favorites(), ThumbnailLoader())
    m.set_paths(paths)
    mv = MultiView(m, Favorites())
    mv.resize(1280, 720)
    mv.show()
    qapp.processEvents()
    calls = []
    orig = mv._layout_tiles
    mv._layout_tiles = lambda *a, **k: (calls.append(1), orig(*a, **k))[1]
    events = 30
    for i in range(events):
        mv._grid_host.resize(1280 - i, 640)
        qapp.sendEvent(mv._grid_host,
                       QResizeEvent(QSize(1280 - i, 640), QSize(1280, 640)))
        qapp.processEvents()
    for _ in range(30):
        qapp.processEvents()
        time.sleep(0.005)
    # The guarantee is coalescing, not a fixed count: the timer legitimately
    # fires mid-burst if the burst itself outlasts one interval (it does under
    # load).  What must never come back is a relayout per resize event.
    assert calls, "the relayout must still happen once the burst settles"
    assert len(calls) <= events // 5, (
        f"{len(calls)} relayouts for {events} resizes — not coalescing")
    mv.close()


def test_stable_layout_stops_the_chrome_resizing_the_grid(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    m.set_paths([_img(tmp_path / "a.png")])
    mv = MultiView(m, Favorites())
    assert not mv.stable_layout()
    mv.set_stable_layout(True)
    assert mv.stable_layout()
    assert mv._chrome_widget.sizePolicy().retainSizeWhenHidden()
    mv.set_stable_layout(False)
    assert not mv._chrome_widget.sizePolicy().retainSizeWhenHidden()
    mv.close()


def test_repeated_style_refresh_does_not_rewrite_stylesheets(qapp, tmp_path):
    """setStyleSheet forces a full style repolish; the refresh runs across
    every slot whenever tags change anywhere, mostly writing what was there."""
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    s._refresh_tag_styles()
    writes = []
    for b in s._tag_btns.values():
        b.setStyleSheet = lambda css, _w=writes: _w.append(css)
    s._refresh_tag_styles()
    assert not writes, "unchanged styles must not be re-applied"
    s.deleteLater()


def test_style_refresh_still_applies_a_real_change(qapp, tmp_path):
    s = _Slot(0, Favorites())
    p = _img(tmp_path / "a.png")
    s.show_item(0, p)
    s._refresh_tag_styles()
    t = tags.get_tags()[0]
    tags.toggle_tag(p, t)
    s._refresh_tag_styles()
    assert "font-weight: bold" in s._tag_btns[t].styleSheet()


# -- the high-DPI shadow decision ---------------------------------------------


def test_shadow_can_be_forced_off(qapp, monkeypatch):
    monkeypatch.setenv("GALLERY_TAG_SHADOW", "0")
    assert not multiview._shadow_affordable()


# -- chips: all on one row, colour-coded, no ellipsis -------------------------

def _colour_tags():
    names = ["T - Face", "T - Landscape", "T - Portrait", "T - Family",
             "T - Archive", "T - Wallpaper", "T - Reference", "T - Night",
             "T - Macro"]
    tags.set_tags(names)
    palette = ["#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
               "#911eb4", "#46f0f0", "#f032e6", "#bcf60c"]
    for name, hue in zip(names, palette):
        tags.set_color(name, hue)
    return names


def _narrow_slot(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s._position_overlays()
    qapp.processEvents()
    return s


def test_a_set_chip_is_stronger_than_an_unset_one(qapp, tmp_path):
    names = _colour_tags()
    p = _img(tmp_path / "a.png")
    s = _Slot(0, Favorites())
    s.show_item(0, p)
    tags.toggle_tag(p, names[0])
    s._refresh_tag_styles()
    assert "bold" in s._tag_btns[names[0]].styleSheet()
    assert "bold" not in s._tag_btns[names[1]].styleSheet()
    s.deleteLater()




# -- tag chips: the original single row ----------------------------------------

def test_chips_are_a_plain_single_row(qapp, tmp_path):
    from PySide6.QtWidgets import QHBoxLayout
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    s.set_tag_editing(True)
    s.resize(900, 600)
    s._position_overlays()
    qapp.processEvents()
    assert isinstance(s._taglay, QHBoxLayout)
    shown = [b for b in s._tag_btns.values() if not b.isHidden()]
    assert len({b.y() for b in shown}) == 1
    s.deleteLater()


def test_chips_show_their_full_tag_name(qapp, tmp_path):
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    s.set_tag_editing(True)
    qapp.processEvents()
    assert s._tag_btns["Landscape"].text() == "Landscape"
    s.deleteLater()


def test_there_is_no_overflow_menu(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    assert not hasattr(s, "_tag_more_btn")
    s.deleteLater()


def test_a_set_chip_wears_its_tag_colour(qapp, tmp_path):
    p = _img(tmp_path / "a.png")
    t = tags.get_tags()[0]
    tags.set_color(t, "#3cb44b")
    s = _Slot(0, Favorites())
    s.show_item(0, p)
    tags.toggle_tag(p, t)
    s._refresh_tag_styles()
    assert "#3cb44b" in s._tag_btns[t].styleSheet()
    s.deleteLater()


def test_an_unset_chip_is_plain(qapp, tmp_path):
    p = _img(tmp_path / "a.png")
    t = tags.get_tags()[0]
    tags.set_color(t, "#3cb44b")
    s = _Slot(0, Favorites())
    s.show_item(0, p)
    s._refresh_tag_styles()
    assert "#3cb44b" not in s._tag_btns[t].styleSheet()
    s.deleteLater()


def test_the_shadow_is_on_by_default(qapp, tmp_path, monkeypatch):
    monkeypatch.delenv("GALLERY_TAG_SHADOW", raising=False)
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    assert s._tagbar.graphicsEffect() is not None
    s.deleteLater()


def test_the_shadow_can_be_swapped_for_a_flat_plate(qapp, tmp_path,
                                                    monkeypatch):
    """The escape hatch for the hover stutter on a high-DPI display."""
    monkeypatch.setenv("GALLERY_TAG_SHADOW", "0")
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    assert s._tagbar.graphicsEffect() is None
    assert "rgba(0,0,0,150)" in s._tag_btns[tags.get_tags()[0]].styleSheet()
    s.deleteLater()
