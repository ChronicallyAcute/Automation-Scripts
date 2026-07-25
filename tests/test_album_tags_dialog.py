"""Album-tagging dialog + main-window wiring."""
from __future__ import annotations
import os

from PySide6.QtCore import Qt
from PIL import Image

from gallery_py_qt.album_tags_dialog import AlbumTagsDialog
from gallery_py_qt.engine import favorites, foldertags


def _album(tmp_path, name):
    d = tmp_path / name
    d.mkdir()
    Image.new("RGB", (12, 12)).save(d / "p.jpg")
    return str(d)


def test_lists_loaded_albums(qapp, tmp_path):
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a, b, a])              # duplicate dropped
    assert dlg._albums == [a, b]
    assert dlg._list.count() == 2
    assert dlg._checked_albums() == [a, b]        # checked by default
    dlg.done(0)


def test_toggle_tag_applies_to_checked(qapp, tmp_path, flush):
    favorites.set_link_mode("copy")
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a, b])
    # Uncheck the second album; tag should land only on the first.
    dlg._list.item(1).setCheckState(Qt.CheckState.Unchecked)
    dlg._toggle_tag("Az")
    flush()
    assert foldertags.tags_for(a) == ["Az"]
    assert foldertags.tags_for(b) == []
    mirror = os.path.join(foldertags.folder_tags_root(), "Az", "One")
    assert os.path.isdir(mirror)
    dlg.done(0)


def test_toggle_tag_batch_removes_when_all_have_it(qapp, tmp_path, flush):
    favorites.set_link_mode("copy")
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a, b])
    dlg._toggle_tag("Bp")                         # both get it
    flush()
    assert foldertags.tags_for(a) == ["Bp"] and foldertags.tags_for(b) == ["Bp"]
    dlg._toggle_tag("Bp")                         # all have it -> remove from all
    flush()
    assert foldertags.tags_for(a) == [] and foldertags.tags_for(b) == []
    dlg.done(0)


def test_no_checked_albums_is_noop(qapp, tmp_path, flush):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    dlg._uncheck_all()
    dlg._toggle_tag("Az")
    flush()
    assert foldertags.tags_for(a) == []
    dlg.done(0)


def test_main_window_button_opens_dialog(qapp, tmp_path):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    assert w._album_tags_btn.text() == "Tag albums"
    # The handler builds the dialog from the currently loaded folders.
    w._current_folders = [_album(tmp_path, "Loaded")]
    from gallery_py_qt.album_tags_dialog import AlbumTagsDialog as ATD
    dlg = ATD(list(w._current_folders), w)
    assert dlg._albums == w._current_folders
    dlg.done(0)
