"""Media-type filter in the folder/file import picker."""
from __future__ import annotations
import time

from PySide6.QtCore import QDir

from gallery_py_qt.main_window import _CheckFSModel, _FolderPickDlg


def _name_filters(fs):
    return set(fs.nameFilters())


def test_model_class_switches_name_filters(qapp):
    fs = _CheckFSModel()
    fs.set_media_class("images")
    nf = _name_filters(fs)
    assert "*.jpg" in nf and "*.gif" not in nf and "*.mp4" not in nf

    fs.set_media_class("gifs")
    assert _name_filters(fs) == {"*.gif"}

    fs.set_media_class("videos")
    nf = _name_filters(fs)
    assert "*.mp4" in nf and "*.jpg" not in nf

    fs.set_media_class("all")
    nf = _name_filters(fs)
    assert "*.jpg" in nf and "*.gif" in nf and "*.mp4" in nf


def test_model_multi_class_union(qapp):
    fs = _CheckFSModel()
    fs.set_media_classes({"gifs", "videos"})     # gifs AND videos
    nf = _name_filters(fs)
    assert "*.gif" in nf and "*.mp4" in nf and "*.jpg" not in nf
    fs.set_media_classes(set())                  # empty → all
    assert "*.jpg" in _name_filters(fs)


def test_dialog_type_menu_multi_select(qapp):
    dlg = _FolderPickDlg(recents=[])
    acts = dlg._type_btn._class_actions
    # Default: all classes checked → everything shows.
    assert all(a.isChecked() for a in acts.values())
    nf = set(dlg._fs.nameFilters())
    assert "*.jpg" in nf and "*.gif" in nf and "*.mp4" in nf
    # Uncheck Images → gifs AND videos together.
    acts["images"].setChecked(False)
    qapp.processEvents()
    nf = set(dlg._fs.nameFilters())
    assert "*.gif" in nf and "*.mp4" in nf and "*.jpg" not in nf
    dlg.done(0)


def test_quick_locations_resolve_real_paths(qapp, monkeypatch, tmp_path):
    """Downloads resolves via QStandardPaths so a redirected folder is reachable."""
    from PySide6.QtCore import QStandardPaths
    from gallery_py_qt import fs_picker

    real_dl = tmp_path / "OneDrive" / "Downloads"
    real_dl.mkdir(parents=True)

    orig = QStandardPaths.writableLocation

    def fake(loc):
        if loc == QStandardPaths.StandardLocation.DownloadLocation:
            return str(real_dl)
        return orig(loc)

    monkeypatch.setattr(QStandardPaths, "writableLocation", staticmethod(fake))
    locs = dict(fs_picker._quick_locations())
    import os
    assert os.path.normpath(locs["Downloads"]) == os.path.normpath(str(real_dl))


def test_goto_navigates_cold_path(qapp, tmp_path):
    """_goto reaches a folder never expanded in the tree (waits for the model)."""
    import os
    deep = tmp_path / "a" / "b" / "target"
    deep.mkdir(parents=True)
    dlg = _FolderPickDlg(recents=[])
    dlg._goto(str(deep))
    for _ in range(400):
        qapp.processEvents()
        time.sleep(0.005)
        idx = dlg._tree.currentIndex()
        if idx.isValid() and os.path.normpath(dlg._fs.filePath(idx)) == \
                os.path.normpath(str(deep)):
            break
    idx = dlg._tree.currentIndex()
    assert os.path.normpath(dlg._fs.filePath(idx)) == os.path.normpath(str(deep))
    dlg.done(0)


def test_normalize_pasted_path_variants(qapp, tmp_path):
    import os
    from PySide6.QtCore import QUrl
    from gallery_py_qt.fs_picker import normalize_pasted_path

    d = tmp_path / "Downloads"
    d.mkdir()
    target = os.path.normpath(str(d))
    # Plain, quoted, trailing separator, and file:// URL all resolve.
    assert normalize_pasted_path(str(d)) == target
    assert normalize_pasted_path(f'"{d}"') == target
    assert normalize_pasted_path(str(d) + os.sep) == target
    assert normalize_pasted_path(QUrl.fromLocalFile(str(d)).toString()) == target
    # A pasted *file* resolves to its containing folder.
    f = d / "pic.jpg"
    f.write_bytes(b"\x00")
    assert normalize_pasted_path(str(f)) == target
    # Junk / nonexistent → "".
    assert normalize_pasted_path("") == ""
    assert normalize_pasted_path(str(tmp_path / "nope")) == ""


def test_dialog_path_box_navigates(qapp, tmp_path):
    import os
    deep = tmp_path / "x" / "y" / "Downloads"
    deep.mkdir(parents=True)
    dlg = _FolderPickDlg(recents=[])
    edit = dlg._path_row._path_edit
    edit.setText(str(deep))
    edit.returnPressed.emit()
    for _ in range(400):
        qapp.processEvents()
        time.sleep(0.005)
        idx = dlg._tree.currentIndex()
        if idx.isValid() and os.path.normpath(dlg._fs.filePath(idx)) == \
                os.path.normpath(str(deep)):
            break
    idx = dlg._tree.currentIndex()
    assert os.path.normpath(dlg._fs.filePath(idx)) == os.path.normpath(str(deep))
    dlg.done(0)


def test_video_only_hides_images_in_tree(qapp, tmp_path):
    from PIL import Image
    Image.new("RGB", (16, 16)).save(str(tmp_path / "a.jpg"))
    open(str(tmp_path / "b.mp4"), "wb").write(b"\x00")
    fs = _CheckFSModel()
    fs.set_media_class("videos")
    idx = fs.setRootPath(str(tmp_path))
    for _ in range(200):
        qapp.processEvents()
        time.sleep(0.005)
        if fs.rowCount(idx) >= 1:
            break
    names = {fs.fileName(fs.index(r, 0, idx)) for r in range(fs.rowCount(idx))}
    assert "b.mp4" in names and "a.jpg" not in names
