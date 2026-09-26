"""Filename tag sync, picker/toolbar filter agreement, and the pinned footer."""
from __future__ import annotations
import os

from PIL import Image

from gallery_py_qt.engine import scan, tagmigrate, tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.main_window import MainWindow
from gallery_py_qt.model import GalleryModel
from gallery_py_qt.multiview import MultiView


def _img(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (16, 16)).save(path)
    return path


# -- tag sync by filename -----------------------------------------------------
def test_same_name_copies_share_the_union(tmp_path):
    a = _img(str(tmp_path / "A" / "clip.jpg"))
    b = _img(str(tmp_path / "B" / "clip.jpg"))
    c = _img(str(tmp_path / "C" / "clip.jpg"))
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(b, "Bp")                    # different tag on another copy
    names, files, added = tagmigrate.reconcile_by_filename([a, b, c])
    assert names == 1
    for p in (a, b, c):
        assert set(tags.tags_for(p)) == {"Az", "Bp"}   # union everywhere
    assert (files, added) == (3, 4)             # a+1, b+1, c+2


def test_sync_never_removes_a_unique_tag(tmp_path):
    a = _img(str(tmp_path / "A" / "x.jpg"))
    b = _img(str(tmp_path / "B" / "x.jpg"))
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(a, "HT")
    tags.toggle_tag(b, "Bp")
    tagmigrate.reconcile_by_filename([a, b])
    assert set(tags.tags_for(a)) == {"Az", "HT", "Bp"}
    assert set(tags.tags_for(b)) == {"Az", "HT", "Bp"}


def test_distinct_names_are_untouched(tmp_path):
    a = _img(str(tmp_path / "a.jpg"))
    b = _img(str(tmp_path / "b.jpg"))
    tags.toggle_tag(a, "Az")
    assert tagmigrate.reconcile_by_filename([a, b]) == (0, 0, 0)
    assert tags.tags_for(b) == []


def test_sync_is_idempotent(tmp_path):
    a = _img(str(tmp_path / "A" / "x.jpg"))
    b = _img(str(tmp_path / "B" / "x.jpg"))
    tags.toggle_tag(a, "Az")
    tagmigrate.reconcile_by_filename([a, b])
    assert tagmigrate.reconcile_by_filename([a, b]) == (0, 0, 0)


def test_preview_reports_without_writing(tmp_path):
    a = _img(str(tmp_path / "A" / "x.jpg"))
    b = _img(str(tmp_path / "B" / "x.jpg"))
    tags.toggle_tag(a, "Az")
    plan = tagmigrate.preview_filename_sync([a, b])
    assert plan[0]["name"] == "x.jpg" and plan[0]["union"] == ["Az"]
    assert tags.tags_for(b) == []               # nothing written yet


def test_case_insensitive_names_group(tmp_path):
    a = _img(str(tmp_path / "A" / "Clip.JPG"))
    b = _img(str(tmp_path / "B" / "clip.jpg"))
    tags.toggle_tag(a, "Az")
    tagmigrate.reconcile_by_filename([a, b])
    assert tags.tags_for(b) == ["Az"]


def test_recursive_scan_finds_nested_media(tmp_path):
    _img(str(tmp_path / "top.jpg"))
    _img(str(tmp_path / "deep" / "inner.jpg"))
    assert len(scan.scan_media(str(tmp_path)).paths) == 1          # default
    assert len(scan.scan_media(str(tmp_path), recursive=True).paths) == 2


# -- picker / toolbar filter agreement ----------------------------------------
def test_picker_tag_filter_is_adopted_by_the_toolbar(qapp, tmp_path):
    from gallery_py_qt.main_window import _FolderPickDlg
    w = MainWindow()
    dlg = _FolderPickDlg(recents=[])
    btn = dlg._path_row._tag_btn
    btn._tag_actions["Az"].setChecked(True)
    qapp.processEvents()
    w._adopt_picker_tag_filter(dlg)
    assert w._selected_filter_tags() == {"Az"}
    dlg.done(0); w.close()


def test_unfiltered_picker_leaves_the_toolbar_alone(qapp):
    from gallery_py_qt.main_window import _FolderPickDlg
    w = MainWindow()
    w._tag_filter_actions["Bp"].setChecked(True)
    dlg = _FolderPickDlg(recents=[])
    w._adopt_picker_tag_filter(dlg)               # picker has nothing checked
    assert w._selected_filter_tags() == {"Bp"}    # untouched
    dlg.done(0); w.close()


def test_empty_hint_names_the_tag_filter(qapp, tmp_path):
    w = MainWindow()
    p = _img(str(tmp_path / "a.jpg"))
    w._model.set_paths([p])
    w._tag_filter_actions["Az"].setChecked(True)  # hides the only item
    w._apply_filter()
    assert w._model.rowCount() == 0
    assert "none match" in w._view._empty_primary
    assert "tags: Az" in w._view._empty_secondary   # the culprit is named
    w.close()


# -- pinned footer ------------------------------------------------------------
def test_footer_stays_visible_when_the_header_auto_hides(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [_img(str(tmp_path / f"i{i}.jpg")) for i in range(3)]
    m.set_paths(ps); m.set_dims({p: (16, 16) for p in ps})
    v = MultiView(m, Favorites()); v.resize(900, 600); v.show(); v.open(0)
    v._show_bars()
    assert not v._autoscroll_widget.isHidden()
    v._maybe_hide_bars()                          # the idle timer firing
    assert v._chrome_widget.isHidden()            # header goes…
    assert not v._autoscroll_widget.isHidden()    # …footer stays put
    v.deleteLater()
