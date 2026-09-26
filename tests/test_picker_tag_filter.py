"""Tag-based filtering in the file-import picker."""
from __future__ import annotations
import time

from PIL import Image

from gallery_py_qt.engine import tags
from gallery_py_qt.fs_picker import _CheckFSModel
from gallery_py_qt.main_window import _FolderPickDlg


def _img(path):
    Image.new("RGB", (8, 8)).save(path)
    return str(path)


def _names_under(qapp, fs, folder):
    idx = fs.setRootPath(folder)
    for _ in range(200):
        qapp.processEvents()
        time.sleep(0.005)
        if fs.rowCount(idx) > 0:
            break
    return {fs.fileName(fs.index(r, 0, idx)) for r in range(fs.rowCount(idx))}


def test_no_tag_filter_shows_everything(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    _img(tmp_path / "b.jpg")
    tags.toggle_tag(a, "Az")
    fs = _CheckFSModel()
    assert fs.tag_filter() == (set(), False)
    names = _names_under(qapp, fs, str(tmp_path))
    assert {"a.jpg", "b.jpg"} <= names


def test_tag_filter_hides_untagged(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    _img(tmp_path / "b.jpg")
    tags.toggle_tag(a, "Az")
    fs = _CheckFSModel()
    fs.set_tag_filter({"Az"})
    names = _names_under(qapp, fs, str(tmp_path))
    assert "a.jpg" in names and "b.jpg" not in names


def test_match_all_requires_every_tag(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    b = _img(tmp_path / "b.jpg")
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(a, "Bp")
    tags.toggle_tag(b, "Az")
    fs = _CheckFSModel()
    fs.set_tag_filter({"Az", "Bp"}, match_all=False)      # ANY → both
    assert {"a.jpg", "b.jpg"} <= _names_under(qapp, fs, str(tmp_path))
    fs2 = _CheckFSModel()
    fs2.set_tag_filter({"Az", "Bp"}, match_all=True)      # ALL → only a
    names = _names_under(qapp, fs2, str(tmp_path))
    assert "a.jpg" in names and "b.jpg" not in names


def test_no_matches_shows_nothing_not_everything(qapp, tmp_path):
    _img(tmp_path / "a.jpg")
    fs = _CheckFSModel()
    fs.set_tag_filter({"Az"})            # nothing carries Az
    idx = fs.setRootPath(str(tmp_path))
    for _ in range(60):
        qapp.processEvents()
        time.sleep(0.005)
    names = {fs.fileName(fs.index(r, 0, idx)) for r in range(fs.rowCount(idx))}
    assert "a.jpg" not in names


def test_tag_filter_composes_with_media_class(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    v = str(tmp_path / "v.mp4")
    open(v, "wb").write(b"\x00")
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(v, "Az")
    fs = _CheckFSModel()
    fs.set_media_class("videos")         # videos only …
    fs.set_tag_filter({"Az"})            # … AND tagged Az
    names = _names_under(qapp, fs, str(tmp_path))
    assert "v.mp4" in names and "a.jpg" not in names


def test_glob_metacharacters_in_names_still_match(qapp, tmp_path):
    weird = _img(tmp_path / "clip [1080p].jpg")
    tags.toggle_tag(weird, "Az")
    fs = _CheckFSModel()
    fs.set_tag_filter({"Az"})
    assert "clip [1080p].jpg" in _names_under(qapp, fs, str(tmp_path))


def test_picker_exposes_tag_menu(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    tags.toggle_tag(a, "Az")
    dlg = _FolderPickDlg(recents=[])
    btn = dlg._path_row._tag_btn
    assert "all" in btn.text()                       # nothing filtered yet
    btn._tag_actions["Az"].setChecked(True)
    qapp.processEvents()
    assert dlg._fs.tag_filter()[0] == {"Az"}
    assert "Az" in btn.text()
    dlg.done(0)


def test_huge_tag_match_falls_back_instead_of_wedging(qapp, tmp_path, monkeypatch):
    """QFileSystemModel tests every pattern against every entry, so thousands
    of patterns would wedge the picker.  Past the cap it degrades to the
    extension filter and says so, rather than freezing."""
    from gallery_py_qt.engine import tags as _tags
    fs = _CheckFSModel()
    huge = {f"C:/x/f{i}.jpg": ["Az"] for i in range(fs._MAX_NAME_PATTERNS + 50)}
    monkeypatch.setattr(_tags, "_load", lambda: huge)
    fs.set_tag_filter({"Az"})
    assert fs.tag_filter_truncated
    assert "*.jpg" in set(fs.nameFilters())      # fell back, did not wedge


def test_modest_tag_match_still_filters(qapp, tmp_path, monkeypatch):
    from gallery_py_qt.engine import tags as _tags
    fs = _CheckFSModel()
    small = {f"C:/x/f{i}.jpg": ["Az"] for i in range(5)}
    monkeypatch.setattr(_tags, "_load", lambda: small)
    fs.set_tag_filter({"Az"})
    assert not fs.tag_filter_truncated
    assert "f0.jpg" in set(fs.nameFilters())


# -- favourites filter, and hiding folders that lead nowhere -------------------

def _tree_with(qapp, fs):
    from PySide6.QtWidgets import QTreeView
    from gallery_py_qt.fs_picker import FolderFilter
    tree = QTreeView()
    tree.setModel(fs)
    return tree, FolderFilter(tree, fs)


def _rows(qapp, fs, folder, want=1):
    idx = fs.setRootPath(folder)
    for _ in range(400):
        qapp.processEvents()
        time.sleep(0.005)
        if fs.rowCount(idx) >= want:
            break
    return idx


def test_favourites_filter_is_off_by_default(qapp):
    fs = _CheckFSModel()
    assert not fs.favorites_only()
    assert not fs.filtering_content()


def test_favourites_filter_shows_only_favourites(qapp, tmp_path):
    from gallery_py_qt.engine.favorites import Favorites
    a = _img(tmp_path / "a.jpg")
    _img(tmp_path / "b.jpg")
    Favorites().toggle(a)
    fs = _CheckFSModel()
    fs.set_favorites_only(True)
    names = _names_under(qapp, fs, str(tmp_path))
    assert "a.jpg" in names and "b.jpg" not in names


def test_favourites_and_tags_must_both_hold(qapp, tmp_path):
    from gallery_py_qt.engine.favorites import Favorites
    a = _img(tmp_path / "a.jpg")
    b = _img(tmp_path / "b.jpg")
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(b, "Az")
    Favorites().toggle(a)
    fs = _CheckFSModel()
    fs.set_tag_filter({"Az"})
    fs.set_favorites_only(True)
    names = _names_under(qapp, fs, str(tmp_path))
    assert "a.jpg" in names and "b.jpg" not in names


def test_a_folder_with_no_match_is_hidden(qapp, tmp_path):
    """Name filters only hide FILES, so the tree used to fill with folders
    that opened onto nothing."""
    from gallery_py_qt.engine.favorites import Favorites
    (tmp_path / "HasFav").mkdir()
    (tmp_path / "NoFav").mkdir()
    a = _img(tmp_path / "HasFav" / "a.jpg")
    _img(tmp_path / "NoFav" / "b.jpg")
    Favorites().toggle(a)
    fs = _CheckFSModel()
    tree, ff = _tree_with(qapp, fs)
    fs.set_favorites_only(True)
    idx = _rows(qapp, fs, str(tmp_path), want=2)
    tree.setRootIndex(idx)
    ff.refresh()
    qapp.processEvents()
    hidden = {fs.fileName(fs.index(r, 0, idx)): tree.isRowHidden(r, idx)
              for r in range(fs.rowCount(idx))}
    assert hidden.get("HasFav") is False
    assert hidden.get("NoFav") is True


def test_a_folder_matching_by_tag_is_kept(qapp, tmp_path):
    (tmp_path / "Tagged").mkdir()
    (tmp_path / "Plain").mkdir()
    a = _img(tmp_path / "Tagged" / "a.jpg")
    _img(tmp_path / "Plain" / "b.jpg")
    tags.toggle_tag(a, "Az")
    fs = _CheckFSModel()
    tree, ff = _tree_with(qapp, fs)
    fs.set_tag_filter({"Az"})
    idx = _rows(qapp, fs, str(tmp_path), want=2)
    tree.setRootIndex(idx)
    ff.refresh()
    qapp.processEvents()
    hidden = {fs.fileName(fs.index(r, 0, idx)): tree.isRowHidden(r, idx)
              for r in range(fs.rowCount(idx))}
    assert hidden.get("Tagged") is False and hidden.get("Plain") is True


def test_clearing_the_filter_unhides_every_folder(qapp, tmp_path):
    from gallery_py_qt.engine.favorites import Favorites
    (tmp_path / "HasFav").mkdir()
    (tmp_path / "NoFav").mkdir()
    a = _img(tmp_path / "HasFav" / "a.jpg")
    _img(tmp_path / "NoFav" / "b.jpg")
    Favorites().toggle(a)
    fs = _CheckFSModel()
    tree, ff = _tree_with(qapp, fs)
    fs.set_favorites_only(True)
    idx = _rows(qapp, fs, str(tmp_path), want=2)
    tree.setRootIndex(idx)
    ff.refresh()
    fs.set_favorites_only(False)
    ff.refresh()
    qapp.processEvents()
    assert all(not tree.isRowHidden(r, idx) for r in range(fs.rowCount(idx)))


def test_a_parent_of_a_match_is_kept(qapp, tmp_path):
    from gallery_py_qt.engine.favorites import Favorites
    deep = tmp_path / "Outer" / "Inner"
    deep.mkdir(parents=True)
    a = _img(deep / "a.jpg")
    Favorites().toggle(a)
    fs = _CheckFSModel()
    fs.set_favorites_only(True)
    assert fs.folder_matches(str(tmp_path / "Outer"))


def test_the_picker_exposes_the_favourites_button(qapp, tmp_path):
    dlg = _FolderPickDlg(recents=[])
    btn = dlg._path_row._fav_btn
    assert not dlg._fs.favorites_only()
    btn.setChecked(True)
    qapp.processEvents()
    assert dlg._fs.favorites_only()
    dlg.done(0)
