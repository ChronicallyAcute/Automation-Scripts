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
