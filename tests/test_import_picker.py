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


def test_dialog_has_type_combo_wired(qapp):
    dlg = _FolderPickDlg(recents=[])
    # Default shows all media.
    assert dlg._type_combo.currentData() == "all"
    # Selecting "Videos" pushes the class down to the model's name filters.
    i = dlg._type_combo.findData("videos")
    dlg._type_combo.setCurrentIndex(i)
    qapp.processEvents()
    nf = set(dlg._fs.nameFilters())
    assert "*.mp4" in nf and "*.jpg" not in nf
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
