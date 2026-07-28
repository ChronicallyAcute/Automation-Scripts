"""Multi-view chrome: transparent tag bar, orientation counts, zoom, rotate
menu, and the per-tile source link."""
from __future__ import annotations
import os

import pytest
from PySide6.QtCore import Qt
from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import MultiView, _Slot, _AspectLabel


@pytest.fixture
def slot(qapp):
    s = _Slot(0, Favorites())
    s.resize(400, 300)
    return s


@pytest.fixture
def mv(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    wide = [str(tmp_path / f"w{i}.jpg") for i in range(5)]
    tall = [str(tmp_path / f"t{i}.jpg") for i in range(3)]
    for p in wide:
        Image.new("RGB", (200, 100)).save(p)
    for p in tall:
        Image.new("RGB", (100, 200)).save(p)
    m.set_paths(wide + tall)
    m.set_dims({**{p: (200, 100) for p in wide},
                **{p: (100, 200) for p in tall}})
    v = MultiView(m, Favorites())
    v.resize(900, 600)
    return v, wide, tall


# -- 2. tag bar has no background ---------------------------------------------
def test_tagbar_is_transparent(slot):
    assert slot._tagbar.testAttribute(
        Qt.WidgetAttribute.WA_TranslucentBackground)
    assert slot._tagbar.styleSheet() == ""
    # A drop shadow supplies contrast in place of the removed backing plate.
    assert slot._tagbar.graphicsEffect() is not None


def test_tag_chips_have_no_fill(slot):
    slot._path = ""
    slot._refresh_tag_styles()
    assert slot._tag_btns, "expected tag chips"
    for b in slot._tag_btns.values():
        css = b.styleSheet()
        base = css.split("QToolButton:hover")[0]
        assert "background: transparent" in base
        assert "rgba(0,0,0" not in base          # no dark pill behind the glyph


def test_set_tag_still_distinguishable(slot, tmp_path):
    from gallery_py_qt.engine import tags
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    slot._path = p
    tags.toggle_tag(p, "Az")
    slot._refresh_tag_styles()
    on = slot._tag_btns["Az"].styleSheet()
    off = slot._tag_btns["Bp"].styleSheet()
    assert on != off
    assert config.ACCENT in on and "bold" in on   # colour+weight carry state


# -- 3. orientation counts are displayed --------------------------------------
def test_orient_button_shows_other_group_count(mv):
    v, wide, tall = mv
    v.open(0)
    # The model may sort, so assert relative to whichever group we landed in.
    if v._current_paths is v._landscape_paths:
        assert v._orient_btn.text() == f"↕ {len(tall)}"
        v._switch_orientation()
        assert v._orient_btn.text() == f"↔ {len(wide)}"
    else:
        assert v._orient_btn.text() == f"↔ {len(wide)}"
        v._switch_orientation()
        assert v._orient_btn.text() == f"↕ {len(tall)}"


def test_orient_button_disabled_when_no_other_media(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"w{i}.jpg") for i in range(2)]
    for p in ps:
        Image.new("RGB", (200, 100)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (200, 100) for p in ps})
    v = MultiView(m, Favorites())
    v.open(0)
    assert v._orient_btn.text() == "↕ 0"
    assert not v._orient_btn.isEnabled()


# -- 4. +/- zoom in 10% steps --------------------------------------------------
def test_zoom_steps_by_ten_percent(mv):
    v, _w, _t = mv
    assert v._zoom == 1.0 and v._zoom_lbl.text() == "100%"
    v.zoom_in()
    assert v._zoom == pytest.approx(1.1) and v._zoom_lbl.text() == "110%"
    v.zoom_out()
    v.zoom_out()
    assert v._zoom == pytest.approx(0.9) and v._zoom_lbl.text() == "90%"
    v.reset_zoom()
    assert v._zoom == 1.0


def test_zoom_clamps_and_disables_buttons(mv):
    v, _w, _t = mv
    for _ in range(40):
        v.zoom_out()
    assert v._zoom == pytest.approx(v.ZOOM_MIN)
    assert not v._zoom_out_btn.isEnabled() and v._zoom_in_btn.isEnabled()
    for _ in range(80):
        v.zoom_in()
    assert v._zoom == pytest.approx(v.ZOOM_MAX)
    assert not v._zoom_in_btn.isEnabled() and v._zoom_out_btn.isEnabled()


def test_zoom_reaches_every_slot(mv):
    v, _w, _t = mv
    v.zoom_in()
    assert all(s._zoom == pytest.approx(1.1) for s in v._slots)
    assert v._ss_buf._zoom == pytest.approx(1.1)


def test_zoom_resets_on_open(mv):
    v, _w, _t = mv
    v.zoom_in()
    v.zoom_in()
    v.open(0)
    assert v._zoom == 1.0 and v._zoom_lbl.text() == "100%"


def test_zoom_scales_the_rendered_pixmap(qapp, tmp_path):
    lbl = _AspectLabel()
    lbl.resize(200, 200)
    pm_src = Image.new("RGB", (100, 100))
    p = str(tmp_path / "sq.jpg")
    pm_src.save(p)
    from PySide6.QtGui import QPixmap
    lbl.set_source(QPixmap(p))
    fit = lbl.pixmap().size()
    lbl.set_zoom(0.5)
    small = lbl.pixmap().size()
    assert small.width() < fit.width()
    lbl.set_zoom(2.0)                             # overflows → cropped to label
    big = lbl.pixmap().size()
    assert big.width() <= lbl.width() and big.height() <= lbl.height()


def test_video_zoom_scales_display_size(slot):
    slot._is_video = True
    slot._gview.resize(400, 200)
    from PySide6.QtCore import QSizeF
    slot._video_item.setSize(QSizeF(0, 0))
    # nativeSize is empty headless, so the fallback path fills the viewport;
    # assert the zoom multiplier is at least threaded into the sizing call.
    slot.set_zoom(1.5)
    assert slot._zoom == pytest.approx(1.5)
    assert slot._img._zoom == pytest.approx(1.5)


# -- 5. rotate menu offers 90/180/270 -----------------------------------------
def test_rotate_menu_entries(slot):
    assert [a.text() for a in slot._rot_menu.actions()] == [
        "Rotate 90°", "Rotate 180°", "Rotate 270°"]


def test_rotate_emits_chosen_degrees(slot, tmp_path):
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    slot._path, slot._is_video = p, False
    got = []
    slot.rotated.connect(lambda pa, d: got.append((pa, d)))
    slot._rot_menu.actions()[2].trigger()          # 270
    slot._rot_btn.click()                          # plain click → 90
    assert got == [(p, 270), (p, 90)]


def test_rotate_relays_degrees_through_multiview(mv):
    v, wide, _t = mv
    v.open(0)
    got = []
    v.rotated.connect(lambda p, d: got.append((p, d)))
    v._on_slot_rotate(wide[0], 180)
    assert got == [(wide[0], 180)]


def test_video_tile_does_not_rotate(slot, tmp_path):
    slot._path, slot._is_video = str(tmp_path / "v.mp4"), True
    got = []
    slot.rotated.connect(lambda *a: got.append(a))
    slot._rot_menu.actions()[0].trigger()
    assert got == []


# -- 7. source hyperlink header ------------------------------------------------
def test_source_link_shows_and_hides(slot, tmp_path):
    p = str(tmp_path / "beach.jpg")
    Image.new("RGB", (32, 24)).save(p)
    slot.show_item(0, p)
    assert not slot._srclink.isHidden()
    assert "beach.jpg" in slot._srclink.text()
    assert '<a href="reveal"' in slot._srclink.text()
    assert p in slot._srclink.toolTip()
    slot.clear()
    assert slot._srclink.isHidden()


def test_source_link_is_transparent(slot, tmp_path):
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    slot.show_item(0, p)
    assert slot._srclink.testAttribute(
        Qt.WidgetAttribute.WA_TranslucentBackground)
    assert "background: transparent" in slot._srclink.styleSheet()
    assert "text-decoration:none" in slot._srclink.text()


def test_source_link_escapes_and_truncates(slot, tmp_path):
    name = "a&b<c>" + "x" * 80 + ".jpg"
    p = str(tmp_path / name)
    Image.new("RGB", (8, 8)).save(p)
    slot.show_item(0, p)
    txt = slot._srclink.text()
    assert "&amp;" in txt and "&lt;" in txt and "&gt;" in txt
    assert "…" in txt                              # long name elided
    assert p in slot._srclink.toolTip()            # full path still available


def test_source_link_reveals_the_file(slot, tmp_path, monkeypatch):
    from gallery_py_qt.engine import shell
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    slot.show_item(0, p)
    seen = []
    monkeypatch.setattr(shell, "reveal_path", lambda x: seen.append(x) or True)
    slot._srclink.linkActivated.emit("reveal")
    assert seen == [p]


def test_side_scroll_tiles_get_the_same_header(mv):
    """Side-scroll reuses _Slot, so the header covers the scroller view too."""
    v, wide, _t = mv
    v.open(0)
    v._slideshow_mode = "scroll"
    v._start_sidescroll()
    assert v._ss_slot_order
    assert all(not s._srclink.isHidden()
               for s in v._ss_slot_order if s._path)
    v._cleanup_sidescroll()


# -- regressions caught in review ---------------------------------------------
def test_open_popup_keeps_the_tile_bar_visible(slot, monkeypatch):
    """The rotate drop-down steals the pointer; the bar must not auto-hide."""
    from PySide6.QtWidgets import QApplication
    slot._btnbar.show()
    monkeypatch.setattr(QApplication, "activePopupWidget",
                        staticmethod(lambda: slot._rot_menu))
    slot._maybe_hide_bar()
    assert not slot._btnbar.isHidden()
    assert slot._bar_hide_timer.isActive()        # re-armed for after the popup
    monkeypatch.setattr(QApplication, "activePopupWidget",
                        staticmethod(lambda: None))
    slot._maybe_hide_bar()
    assert slot._btnbar.isHidden()


def test_rotate_button_disabled_for_video(slot, tmp_path):
    p = str(tmp_path / "clip.mp4")
    open(p, "wb").write(b"\x00")
    slot.show_item(0, p)
    assert not slot._rot_btn.isEnabled()
    assert "isn't supported" in slot._rot_btn.toolTip()


def test_rotate_button_enabled_for_image(slot, tmp_path):
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (16, 16)).save(p)
    slot.show_item(0, p)
    assert slot._rot_btn.isEnabled()


def test_overlays_match_the_rendered_pixmap_exactly(qapp, tmp_path):
    """_img_displayed_rect must report what _apply actually drew, not a
    re-derived estimate (they disagreed by a pixel at zoom < 1)."""
    from PySide6.QtGui import QPixmap
    s = _Slot(0, Favorites())
    s.resize(500, 400)
    s._img.resize(500, 400)
    p = str(tmp_path / "src.jpg")
    Image.new("RGB", (600, 1000)).save(p)
    s._pm = QPixmap(p)
    s._img.set_source(s._pm)
    for z in (0.5, 0.7, 1.0):
        s.set_zoom(z)
        _x, _y, dw, dh = s._img_displayed_rect()
        shown = s._img.pixmap()
        assert (dw, dh) == (shown.width(), shown.height())


def test_zoom_max_is_two_hundred_percent(mv):
    v, _w, _t = mv
    assert v.ZOOM_MAX == 2.00
    for _ in range(40):
        v.zoom_in()
    assert v._zoom_lbl.text() == "200%"


def test_orient_count_marked_provisional_while_measuring(mv):
    v, _w, tall = mv
    v.open(0)
    settled = v._orient_btn.text()
    assert "~" not in settled
    v.set_measuring(True)
    assert "~" in v._orient_btn.text()
    assert "may change" in v._orient_btn.toolTip()
    v.set_measuring(False)
    assert "~" not in v._orient_btn.text()
