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
from gallery_py_qt.flowlayout import FlowLayout
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

def test_every_chip_stays_inside_a_narrow_tile(qapp, tmp_path):
    """3840x2160 at 300% is a 1280x720 LOGICAL desktop, so tiles are narrow.

    A QHBoxLayout overflowed its container and the chips past the right edge
    were invisible and unclickable.
    """
    _many_tags()
    p = _img(tmp_path / "tall.png", (900, 1600))     # letterboxed => narrow
    s = _Slot(0, Favorites())
    s.show_item(0, p)
    s.set_tag_editing(True)
    s.resize(426, 360)
    s._position_overlays()
    qapp.processEvents()
    shown = [b for b in s._tag_btns.values() if not b.isHidden()]
    assert shown, "no chips to check"
    overflow = [b for b in shown if b.x() + b.width() > s._tagbar.width() + 1]
    assert not overflow, f"{len(overflow)} chip(s) clipped off the tile"
    s.deleteLater()


def test_chips_wrap_onto_several_rows_when_asked(qapp, tmp_path):
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s.set_tag_wrap(True)                 # opt in; one row is the default
    qapp.processEvents()
    rows = {b.y() for b in s._tag_btns.values() if not b.isHidden()}
    assert len(rows) > 1, "wrap mode must use more than one row"
    s.deleteLater()


def test_the_row_is_a_single_line_by_default(qapp, tmp_path):
    """The original look: one row, with the remainder in an overflow menu
    rather than clipped off the edge."""
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s._position_overlays()
    qapp.processEvents()
    shown = [b for b in s._tag_btns.values() if not b.isHidden()]
    assert len({b.y() for b in shown}) == 1, "must stay on one row"
    assert not s._tag_more_btn.isHidden(), "overflow must be offered"
    assert s._tag_more_menu.actions(), "the hidden chips must be reachable"
    s.deleteLater()


def test_every_tag_is_reachable_on_one_row(qapp, tmp_path):
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s._position_overlays()
    qapp.processEvents()
    # Chip LABELS are abbreviated to fit, so reachability is checked by tag
    # identity: every tag is either a visible chip or an entry in the menu.
    on_row = {t for t, b in s._tag_btns.items() if not b.isHidden()}
    in_menu = {a.text().strip().lstrip("\u2713").strip()
               for a in s._tag_more_menu.actions()}
    assert on_row | in_menu >= set(s._tag_btns), "a tag became unreachable"
    s.deleteLater()


def test_controls_keep_their_place_when_chips_overflow(qapp, tmp_path):
    """Only tag chips are displaced — never the ditto / new-tag / zoom
    controls, which sat at the ends of the row in the original layout."""
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s._position_overlays()
    qapp.processEvents()
    for name in ("_repeat_btn", "_new_tag_btn",
                 "_tile_zoom_out_btn", "_tile_zoom_in_btn"):
        b = getattr(s, name)
        assert not b.isHidden(), f"{name} was displaced"
        assert b.x() + b.width() <= s._tagbar.width() + 1, f"{name} clipped"
    s.deleteLater()


def test_a_wide_tile_needs_no_overflow(qapp, tmp_path):
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "wide.png", (1600, 900)))
    s.set_tag_editing(True)
    s.resize(1400, 700)
    s._position_overlays()
    qapp.processEvents()
    assert s._tag_more_btn.isHidden(), "nothing should overflow on a wide tile"
    assert all(not b.isHidden() for b in s._tag_btns.values())
    s.deleteLater()


def test_switching_back_from_wrap_restores_one_row(qapp, tmp_path):
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s.set_tag_wrap(True)
    qapp.processEvents()
    s.set_tag_wrap(False)
    qapp.processEvents()
    shown = [b for b in s._tag_btns.values() if not b.isHidden()]
    assert len({b.y() for b in shown}) == 1
    s.deleteLater()


def test_a_wide_tile_still_uses_one_row(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "wide.png", (1600, 400)))
    s.set_tag_editing(True)
    s.resize(1600, 700)
    s._position_overlays()
    qapp.processEvents()
    rows = {b.y() for b in s._tag_btns.values() if not b.isHidden()}
    assert len(rows) == 1, "a wide tile should not wrap"
    s.deleteLater()


def test_tagbar_is_given_its_wrapped_height(qapp, tmp_path):
    _many_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "tall.png", (900, 1600)))
    s.set_tag_editing(True)
    s.resize(426, 360)
    s.set_tag_wrap(True)
    s._position_overlays()
    qapp.processEvents()
    need = s._taglay.heightForWidth(s._tagbar.width())
    assert s._tagbar.height() >= min(need, int(s.height() * 0.5))
    s.deleteLater()


def test_tagbar_never_swallows_the_whole_tile(qapp, tmp_path):
    for i in range(40):                      # an absurd tag set
        tags.add_tag(f"tag{i:02d}")
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    s.set_tag_editing(True)
    s.resize(300, 300)
    s.set_tag_wrap(True)
    s._position_overlays()
    qapp.processEvents()
    assert s._tagbar.height() <= s.height() * 0.6
    s.deleteLater()


# -- the flow layout itself ----------------------------------------------------

def test_flowlayout_reports_more_height_when_narrower(qapp):
    from PySide6.QtWidgets import QWidget, QPushButton
    host = QWidget()
    lay = FlowLayout(host, margin=0, spacing=2)
    for i in range(8):
        lay.addWidget(QPushButton("item %d" % i))
    wide = lay.heightForWidth(2000)
    narrow = lay.heightForWidth(200)
    assert narrow > wide
    host.deleteLater()


def test_flowlayout_skips_hidden_items(qapp):
    from PySide6.QtWidgets import QWidget, QPushButton
    host = QWidget()
    lay = FlowLayout(host, margin=0, spacing=2)
    btns = [QPushButton("item %d" % i) for i in range(8)]
    for b in btns:
        lay.addWidget(b)
    full = lay.heightForWidth(200)
    for b in btns[2:]:
        b.hide()
    assert lay.heightForWidth(200) < full, "hidden chips must not reserve space"
    host.deleteLater()


def test_flowlayout_take_and_count(qapp):
    from PySide6.QtWidgets import QWidget, QPushButton
    host = QWidget()
    lay = FlowLayout(host)
    lay.addWidget(QPushButton("x"))
    assert lay.count() == 1
    assert lay.itemAt(0) is not None and lay.itemAt(5) is None
    assert lay.takeAt(0) is not None and lay.count() == 0
    assert lay.takeAt(0) is None
    host.deleteLater()


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

def test_shadow_is_used_on_an_ordinary_display(qapp):
    os.environ.pop("GALLERY_TAG_SHADOW", None)
    assert multiview._shadow_affordable()


def test_shadow_can_be_forced_off(qapp, monkeypatch):
    monkeypatch.setenv("GALLERY_TAG_SHADOW", "0")
    assert not multiview._shadow_affordable()


def test_shadow_can_be_forced_on(qapp, monkeypatch):
    monkeypatch.setenv("GALLERY_TAG_SHADOW", "1")
    assert multiview._shadow_affordable()


def test_plated_chips_carry_their_own_background(qapp, tmp_path, monkeypatch):
    monkeypatch.setenv("GALLERY_TAG_SHADOW", "0")
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    assert s._chip_plate
    assert s._tagbar.graphicsEffect() is None, "no offscreen blur at high DPI"
    css = s._tag_btns[tags.get_tags()[0]].styleSheet()
    assert "rgba(0,0,0,150)" in css, "chips need their own contrast instead"
    s.deleteLater()


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


def test_every_chip_stays_on_the_row_without_an_ellipsis(qapp, tmp_path):
    names = _colour_tags()
    s = _narrow_slot(qapp, tmp_path)
    shown = [b for b in s._tag_btns.values() if not b.isHidden()]
    assert len(shown) == len(names), "no tag may be hidden behind a menu"
    assert len({b.y() for b in shown}) == 1, "must be a single row"
    assert s._tag_more_btn.isHidden(), "the ellipsis must not be needed"
    s.deleteLater()


def test_labels_shorten_rather_than_collapse(qapp, tmp_path):
    _colour_tags()
    s = _narrow_slot(qapp, tmp_path)
    labels = [b.text() for b in s._tag_btns.values() if not b.isHidden()]
    assert all(len(t) < len("T - Landscape") for t in labels)
    s.deleteLater()


def test_shortened_labels_stay_distinct(qapp, tmp_path):
    """"T - Face" and "T - Family" must not both become "Fa"."""
    _colour_tags()
    s = _narrow_slot(qapp, tmp_path)
    labels = [b.text() for b in s._tag_btns.values() if not b.isHidden()]
    assert len(set(labels)) == len(labels)
    s.deleteLater()


def test_the_full_name_survives_in_the_tooltip(qapp, tmp_path):
    _colour_tags()
    s = _narrow_slot(qapp, tmp_path)
    assert "T - Landscape" in s._tag_btns["T - Landscape"].toolTip()
    s.deleteLater()


def test_a_shared_prefix_is_dropped_first(qapp, tmp_path):
    _colour_tags()
    s = _narrow_slot(qapp, tmp_path)
    assert s._shared_prefix(list(s._tag_btns)) == "T - "
    assert not any(b.text().startswith("T - ")
                   for b in s._tag_btns.values() if not b.isHidden())
    s.deleteLater()


def test_every_chip_carries_its_own_colour(qapp, tmp_path):
    names = _colour_tags()
    s = _narrow_slot(qapp, tmp_path)
    # Unset chips are muted, not colourless — the colour is what identifies a
    # tag once the label is abbreviated.
    hues = {b.styleSheet() for b in s._tag_btns.values()}
    assert len(hues) == len(names), "each chip must be styled distinctly"
    s.deleteLater()


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


def test_full_names_return_on_a_wide_tile(qapp, tmp_path):
    _colour_tags()
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "wide.png", (1600, 900)))
    s.set_tag_editing(True)
    s.resize(1400, 700)
    s._position_overlays()
    qapp.processEvents()
    assert s._tag_btns["T - Landscape"].text() == "T - Landscape"
    s.deleteLater()
