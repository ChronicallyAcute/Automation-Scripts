"""UI decluttering: collapsed tag chips, folded toolbar, split rotate, auto-hide."""
from __future__ import annotations

from PIL import Image
from PySide6.QtWidgets import QToolButton

from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.lightbox import Lightbox
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.main_window import MainWindow
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import _Slot


def _img(tmp_path, name="a.jpg"):
    p = str(tmp_path / name)
    Image.new("RGB", (16, 16)).save(p)
    return p


# -- 1. per-tile tag chips collapse to just what's set ------------------------
def test_untagged_tile_shows_no_chips(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path))
    assert not s._tag_editing
    assert all(b.isHidden() for b in s._tag_btns.values())
    assert s._tagbar.isHidden()             # nothing to show → no shadow smudge
    s.deleteLater()


def test_collapsed_tile_shows_only_set_tags(qapp, tmp_path):
    p = _img(tmp_path)
    t = tags.get_tags()[0]
    s = _Slot(0, Favorites())
    s.show_item(0, p)
    tags.toggle_tag(p, t)
    s._refresh_tag_styles()
    shown = {name for name, b in s._tag_btns.items() if not b.isHidden()}
    assert shown == {t}                     # the other eight stay hidden
    # …and the editing affordances are not competing for attention.
    assert s._repeat_btn.isHidden()
    assert s._new_tag_btn.isHidden()
    assert s._tile_zoom_in_btn.isHidden()
    s.deleteLater()


def test_hover_expands_to_the_full_editable_row(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path))
    s.set_tag_editing(True)
    assert not any(b.isHidden() for b in s._tag_btns.values())
    assert not s._repeat_btn.isHidden() and not s._new_tag_btn.isHidden()
    assert not s._tile_zoom_out_btn.isHidden()
    assert not s._tile_zoom_in_btn.isHidden()
    s.set_tag_editing(False)
    assert all(b.isHidden() for b in s._tag_btns.values())
    s.deleteLater()


# -- 2. toolbar folds ---------------------------------------------------------
def test_sort_controls_moved_into_one_button(qapp):
    w = MainWindow()
    assert w._sort_btn.menu() is not None
    assert (w._sort_btn.popupMode()
            == QToolButton.ToolButtonPopupMode.InstantPopup)
    # The button states the setting it hides.
    assert w._sort.currentText() in w._sort_btn.text()
    assert "↑" in w._sort_btn.text() or "↓" in w._sort_btn.text()
    # The real combos still exist and still drive the model.
    assert w._sort2 is not None and w._dir_btn is not None
    w.close()


def test_sort_button_tracks_direction(qapp):
    w = MainWindow()
    assert "↑" in w._sort_btn.text()
    w._toggle_sort_dir()
    assert "↓" in w._sort_btn.text()
    w.close()


def test_overflow_menu_hosts_the_occasional_actions(qapp):
    w = MainWindow()
    hosted = {a.defaultWidget() for a in w._more_menu.actions()
              if hasattr(a, "defaultWidget") and a.defaultWidget() is not None}
    for btn in (w._trash_btn, w._dupes_btn, w._album_tags_btn,
                w._theme_btn, w._guide_btn):
        assert btn in hosted                 # embedded, not duplicated
    # Settings is a plain action in the same menu.
    assert any("Settings" in a.text() for a in w._more_menu.actions())
    w.close()


def test_media_toggles_stay_one_click(qapp):
    """Frequent controls must remain on the bar, not buried in a menu."""
    w = MainWindow()
    for b in (w._img_btn, w._gif_btn, w._vid_btn, w._favs_btn):
        assert b.isCheckable() and b.menu() is None
    assert w._img_btn.toolTip()              # abbreviated label, full tooltip
    w.close()


# -- 3. lightbox rotate collapses to one split button -------------------------
def _lb(tmp_path, n=2):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [_img(tmp_path, f"i{i}.jpg") for i in range(n)]
    m.set_paths(ps)
    m.set_dims({p: (16, 16) for p in ps})
    return Lightbox(m, Favorites()), ps


def test_rotate_is_one_button_with_a_menu(qapp, tmp_path):
    lb, _ = _lb(tmp_path)
    assert lb._rot_btn.menu() is not None
    assert [a.text() for a in lb._rot_menu.actions()] == [
        "Rotate 90°", "Rotate 135°", "Rotate 180°", "Rotate 225°", "Rotate 270°"]
    got = []
    lb.rotateRequested.connect(lambda d: got.append(d))
    lb._rot_btn.click()                      # plain click = 90°
    lb._rot_menu.actions()[2].trigger()      # menu = 180°
    assert got == [90, 180]
    lb.close()


# -- 4. one auto-hide rule ----------------------------------------------------
def test_lightbox_chrome_idles_out_and_wakes(qapp, tmp_path):
    lb, _ = _lb(tmp_path)
    lb.show_row(0)
    assert not lb._bar_widget.isHidden()
    lb._maybe_hide_chrome()                  # simulate the idle timer firing
    assert lb._bar_widget.isHidden()
    lb._wake_chrome()
    assert not lb._bar_widget.isHidden()
    lb.close()


def test_chrome_stays_while_help_is_open(qapp, tmp_path):
    lb, _ = _lb(tmp_path)
    lb.show_row(0)
    lb._toggle_help()
    assert not lb._help.isHidden()
    lb._maybe_hide_chrome()
    assert not lb._bar_widget.isHidden()     # never hide out from under help
    lb.close()


def test_navigating_wakes_the_chrome(qapp, tmp_path):
    lb, _ = _lb(tmp_path)
    lb.show_row(0)
    lb._maybe_hide_chrome()
    assert lb._bar_widget.isHidden()
    lb.next()                                # keyboard navigation is activity
    assert not lb._bar_widget.isHidden()
    lb.close()
