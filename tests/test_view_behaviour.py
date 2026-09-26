"""Autoplay scope, multi-view state, single-view chips, volume, slow media."""
from __future__ import annotations
import os
import time

import pytest
from PIL import Image

from gallery_py_qt.engine import media, tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


_seq = [0]


def _img(path, size=(400, 300)):
    """A DISTINCT image each time.

    Identical files would be byte-identical, and the gallery hides duplicate
    copies by default — 24 identical fixtures collapse to a single row, which
    is the feature working but makes a paging test meaningless.
    """
    os.makedirs(os.path.dirname(str(path)), exist_ok=True)
    _seq[0] += 1
    n = _seq[0]
    Image.new("RGB", size, (n * 7 % 256, n * 13 % 256, n * 29 % 256)).save(str(path))
    return str(path)


# -- autoplay only when fully in view -------------------------------------------

def _grid(qapp, tmp_path, n=40):
    from gallery_py_qt.gallery_view import GalleryView
    paths = [_img(tmp_path / f"{i:02}.png", (300, 400)) for i in range(n)]
    m = GalleryModel(Favorites(), ThumbnailLoader())
    m.set_paths(paths)
    v = GalleryView()
    v.setModel(m)
    v.set_columns(3)
    v.resize(800, 600)
    v.show()
    qapp.processEvents()
    v._relayout()
    return v


def test_partly_visible_rows_are_not_played(qapp, tmp_path):
    """A tile sliced by the viewport edge is barely watchable, and starting a
    decode for it is most of the cost of scrolling."""
    v = _grid(qapp, tmp_path)
    v.verticalScrollBar().setValue(150)
    qapp.processEvents()
    painted = set(v._visible_cell_rows())
    played = set(v._fully_visible_rows())
    assert played, "something must still play"
    assert played < painted, "the played set must be strictly smaller"
    v.close()


def test_fully_visible_rows_really_are_inside(qapp, tmp_path):
    v = _grid(qapp, tmp_path)
    v.verticalScrollBar().setValue(220)
    qapp.processEvents()
    top = v.verticalScrollBar().value()
    bottom = top + v.viewport().height()
    for row in v._fully_visible_rows():
        _cx, cy, _cw, ch = v._cells[row]
        assert cy >= top and cy + ch <= bottom
    v.close()


def test_at_the_very_top_the_first_row_plays(qapp, tmp_path):
    v = _grid(qapp, tmp_path)
    v.verticalScrollBar().setValue(0)
    qapp.processEvents()
    assert 0 in v._fully_visible_rows()
    v.close()


def test_visible_rows_is_still_the_painted_set(qapp, tmp_path):
    """Painting must not shrink with autoplay, or tiles would go blank."""
    v = _grid(qapp, tmp_path)
    v.verticalScrollBar().setValue(150)
    qapp.processEvents()
    assert v.visible_rows() == v._visible_cell_rows()
    v.close()


# -- multi-view keeps its place ---------------------------------------------------

def _window(qapp, tmp_path, n=24):
    from gallery_py_qt.main_window import MainWindow
    for i in range(n):
        _img(tmp_path / "m" / f"{i:02}.png")
    win = MainWindow()
    win.resize(1100, 760)
    win.show()
    qapp.processEvents()
    win.open_folder(str(tmp_path / "m"))
    for _ in range(400):
        qapp.processEvents()
        time.sleep(0.01)
        if win._model.rowCount() >= n:
            break
    return win


def _give_dims(qapp, win, size=(400, 300)):
    """Hand the model every item's dimensions.

    Multi-view partitions by dimensions, which normally arrive on a worker;
    until they do the orientation list holds only the peeked item and every
    page clamps to 0. Supplying them makes the test about paging, not timing.
    """
    win._model.set_dims({p: size for p in win._model.all_paths()})
    win._mv._on_model_dims_changed()
    qapp.processEvents()


def test_returning_to_multiview_keeps_the_page(qapp, tmp_path):
    win = _window(qapp, tmp_path)
    win._enter_multiview()
    qapp.processEvents()
    _give_dims(qapp, win)
    assert len(win._mv._current_paths) >= 10, "orientation list did not fill"
    win._mv._render(8)
    qapp.processEvents()
    assert win._mv._start == 8
    win._close_multiview()
    qapp.processEvents()
    win._enter_multiview()
    qapp.processEvents()
    assert win._mv._start == 8, "the page must not reset to the beginning"
    win.close()


def test_a_pinned_tile_still_holds_its_item_on_return(qapp, tmp_path):
    """The pin survived the round trip as an empty square: clear() wiped the
    slot's path and _render() skips pinned slots."""
    win = _window(qapp, tmp_path)
    win._enter_multiview()
    qapp.processEvents()
    _give_dims(qapp, win)
    slot = win._mv._active_slots()[0]
    slot._is_pinned = True
    want = slot._path
    assert want
    win._close_multiview()
    qapp.processEvents()
    win._enter_multiview()
    qapp.processEvents()
    back = win._mv._active_slots()[0]
    assert back.is_pinned and back._path == want
    win.close()


def test_a_fresh_gallery_entry_still_starts_where_asked(qapp, tmp_path):
    win = _window(qapp, tmp_path)
    win._open_multiview(5)
    qapp.processEvents()
    assert win._mv.has_position()
    win.close()


def test_a_pin_whose_file_vanished_is_dropped(qapp, tmp_path):
    win = _window(qapp, tmp_path)
    win._enter_multiview()
    qapp.processEvents()
    slot = win._mv._active_slots()[0]
    slot._is_pinned = True
    gone = slot._path
    win._close_multiview()
    qapp.processEvents()
    os.remove(gone)
    win._enter_multiview()
    qapp.processEvents()
    back = win._mv._active_slots()[0]
    assert not back.is_pinned or back._path != gone
    win.close()


# -- single-view gets the chips ---------------------------------------------------

def _lightbox(qapp, tmp_path, n=3):
    from gallery_py_qt.lightbox import Lightbox
    paths = [_img(tmp_path / "l" / f"{i}.png") for i in range(n)]
    m = GalleryModel(Favorites(), ThumbnailLoader())
    m.set_paths(paths)
    lb = Lightbox(m, Favorites())
    lb.resize(900, 700)
    lb.show()
    qapp.processEvents()
    return lb, paths


def test_single_view_has_a_chip_for_every_tag(qapp, tmp_path):
    lb, _paths = _lightbox(qapp, tmp_path)
    lb.show_row(0)
    qapp.processEvents()
    assert set(lb._tagbar._btns) == set(tags.get_tags())
    lb.close()


def test_a_chip_click_tags_the_shown_item(qapp, tmp_path):
    lb, paths = _lightbox(qapp, tmp_path)
    lb.show_row(0)
    qapp.processEvents()
    seen = []
    lb.tagsChanged.connect(seen.append)
    t = tags.get_tags()[0]
    lb._tagbar.toggle(t)
    assert t in tags.tags_for(paths[0])
    assert seen == [paths[0]]
    lb.close()


def test_navigating_rebinds_the_chips(qapp, tmp_path):
    lb, paths = _lightbox(qapp, tmp_path)
    lb.show_row(0)
    qapp.processEvents()
    lb.show_row(1)
    qapp.processEvents()
    assert lb._tagbar.path() == paths[1]
    lb.close()


def test_a_set_chip_is_stronger_than_an_unset_one(qapp, tmp_path):
    lb, paths = _lightbox(qapp, tmp_path)
    names = tags.get_tags()
    tags.set_color(names[0], "#e6194b")
    lb.show_row(0)
    tags.toggle_tag(paths[0], names[0])
    lb._tagbar.refresh()
    on = lb._tagbar._btns[names[0]].styleSheet()
    off = lb._tagbar._btns[names[1]].styleSheet()
    assert "#e6194b" in on and "bold" in on
    assert "bold" not in off
    assert "border-radius: 2px" in on and "background: transparent" in on
    lb.close()


def test_the_chips_follow_the_chrome(qapp, tmp_path):
    lb, _paths = _lightbox(qapp, tmp_path)
    lb.show_row(0)
    qapp.processEvents()
    lb._maybe_hide_chrome()
    assert lb._tagbar.isHidden()
    lb._wake_chrome()
    assert not lb._tagbar.isHidden()
    lb.close()


# -- the volume adjuster is chrome too --------------------------------------------

def test_the_volume_popup_hides_when_the_pointer_leaves(qapp, tmp_path):
    from gallery_py_qt.multiview import _Slot
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    s.resize(500, 400)
    qapp.processEvents()
    s._vol_popup.show()
    s._vol_hide_timer.start()
    s._maybe_hide_bar()
    assert s._vol_popup.isHidden()
    assert not s._vol_hide_timer.isActive()
    s.deleteLater()


def test_a_pinned_tile_keeps_its_volume_popup(qapp, tmp_path):
    from gallery_py_qt.multiview import _Slot
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path / "a.png"))
    s._is_pinned = True
    s._vol_popup.show()
    s._maybe_hide_bar()
    assert not s._vol_popup.isHidden(), "a pinned tile keeps its chrome"
    s.deleteLater()


def test_single_view_volume_hides_with_the_transport(qapp, tmp_path):
    lb, _paths = _lightbox(qapp, tmp_path)
    lb.show_row(0)
    qapp.processEvents()
    lb._maybe_hide_chrome()
    assert lb._transport.isHidden(), "the transport carries the volume slider"
    lb.close()


# -- ruinously slow media is only slow once ---------------------------------------

def test_a_slow_decode_blacklists_the_file(tmp_path, monkeypatch):
    """HEVC with missing parameter sets, or a container with no moov atom,
    eventually answers — so nothing fails, and every touch pays again."""
    v = str(tmp_path / "x.mp4")
    with open(v, "wb") as f:
        f.write(b"\x00" * 64)
    monkeypatch.setattr(media, "_SLOW_VIDEO_SEC", 0.05)
    assert not media.is_bad_video(v)
    with media._cost_budget(v, " (test)"):
        time.sleep(0.08)
    assert media.is_bad_video(v)


def test_a_quick_decode_is_left_alone(tmp_path, monkeypatch):
    v = str(tmp_path / "x.mp4")
    with open(v, "wb") as f:
        f.write(b"\x00" * 64)
    monkeypatch.setattr(media, "_SLOW_VIDEO_SEC", 5.0)
    with media._cost_budget(v):
        pass
    assert not media.is_bad_video(v)


def test_the_budget_never_swallows_an_error(tmp_path):
    v = str(tmp_path / "x.mp4")
    with pytest.raises(ValueError):
        with media._cost_budget(v):
            raise ValueError("boom")


def test_the_slow_warning_is_printed_once(tmp_path, monkeypatch, capsys):
    v = str(tmp_path / "x.mp4")
    with open(v, "wb") as f:
        f.write(b"\x00" * 64)
    monkeypatch.setattr(media, "_SLOW_VIDEO_SEC", 0.01)
    for _ in range(3):
        media.forget_bad_video(v)
        with media._cost_budget(v):
            time.sleep(0.02)
    assert capsys.readouterr().err.count("skipping it from now on") == 1
