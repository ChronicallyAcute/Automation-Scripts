"""Album-tagging dialog (filesystem tree + thumbnails + multi-select)."""
from __future__ import annotations
import os
import time

from PySide6.QtCore import Qt
from PySide6.QtGui import QImage
from PIL import Image

from gallery_py_qt.album_tags_dialog import AlbumTagsDialog, _TILE_PX
from gallery_py_qt.engine import favorites, foldertags


def _album(tmp_path, name, n=2):
    d = tmp_path / name
    d.mkdir()
    for i in range(n):
        Image.new("RGB", (12, 12)).save(d / f"p{i}.jpg")
    return str(d)


def _settle(qapp, fs, path, tries=200):
    """QFileSystemModel populates on a worker thread — spin until it lands."""
    idx = fs.setRootPath(path)
    for _ in range(tries):
        qapp.processEvents()
        time.sleep(0.005)
        if fs.rowCount(idx) >= 1:
            break
    return idx


# -- seeding + multi-select ----------------------------------------------------
def test_seeds_loaded_albums_as_ticked(qapp, tmp_path):
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a, b, a])                 # duplicate dropped
    assert dlg._albums == [a, b]
    assert dlg._checked_albums() == sorted([a, b])   # checked_paths() sorts
    dlg.done(0)


def test_ticking_more_folders_extends_the_batch(qapp, tmp_path):
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a])
    assert dlg._checked_albums() == [a]
    # Tick a second album through the model's real setData path.
    _settle(qapp, dlg._fs, str(tmp_path))
    dlg._fs.setData(dlg._fs.index(b), Qt.CheckState.Checked,
                    Qt.ItemDataRole.CheckStateRole)
    assert dlg._checked_albums() == sorted([a, b])
    dlg.done(0)


def test_untick_all_and_subfolder_tick(qapp, tmp_path):
    parent = tmp_path / "Albums"
    parent.mkdir()
    a = _album(parent, "One")
    b = _album(parent, "Two")
    dlg = AlbumTagsDialog([])
    dlg._untick_all()
    assert dlg._checked_albums() == []
    dlg._show_contents(str(parent))
    dlg._tick_subfolders()
    assert dlg._checked_albums() == sorted([a, b])
    dlg._untick_all()
    assert dlg._checked_albums() == []
    dlg.done(0)


# -- thumbnail contents pane ---------------------------------------------------
def test_contents_pane_lists_folder_media(qapp, tmp_path):
    a = _album(tmp_path, "One", n=3)
    dlg = AlbumTagsDialog([a])
    assert dlg._contents_model.rowCount() == 3
    names = {dlg._contents_model.item(r).text()
             for r in range(dlg._contents_model.rowCount())}
    assert names == {"p0.jpg", "p1.jpg", "p2.jpg"}
    # Cached thumbnails are painted synchronously, so icons are already set.
    assert not dlg._contents_model.item(0).icon().isNull()
    assert "3 media file(s)" in dlg._contents_hdr.text()
    dlg.done(0)


def test_browsing_switches_contents(qapp, tmp_path):
    a = _album(tmp_path, "One", n=1)
    b = _album(tmp_path, "Two", n=2)
    dlg = AlbumTagsDialog([a])
    assert dlg._contents_model.rowCount() == 1
    dlg._show_contents(b)
    assert dlg._contents_model.rowCount() == 2
    assert dlg._browsing == b
    dlg.done(0)


def test_tile_ready_ignores_other_sizes(qapp, tmp_path):
    a = _album(tmp_path, "One", n=1)
    dlg = AlbumTagsDialog([a])
    path = next(iter(dlg._tiles))
    dlg._tiles[path].setIcon(dlg._tiles[path].icon().__class__())   # clear
    qim = QImage(8, 8, QImage.Format.Format_RGB32)
    qim.fill(0xFF00FF)
    dlg._on_tile_ready(path, 160, qim)          # hover-preview size → ignored
    assert dlg._tiles[path].icon().isNull()
    dlg._on_tile_ready(path, _TILE_PX, qim)     # our size → applied
    assert not dlg._tiles[path].icon().isNull()
    dlg.done(0)


# -- tagging -------------------------------------------------------------------
def test_toggle_tag_applies_to_ticked_only(qapp, tmp_path, flush):
    favorites.set_link_mode("copy")
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a])                   # only 'a' is ticked
    dlg._toggle_tag("Az")
    flush()
    assert foldertags.tags_for(a) == ["Az"]
    assert foldertags.tags_for(b) == []
    assert os.path.isdir(
        os.path.join(foldertags.folder_tags_root(), "Az", "One"))
    dlg.done(0)


def test_toggle_tag_batch_removes_when_all_have_it(qapp, tmp_path, flush):
    favorites.set_link_mode("copy")
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a, b])
    dlg._toggle_tag("Bp")                        # neither has it → add to both
    flush()
    assert foldertags.tags_for(a) == ["Bp"] and foldertags.tags_for(b) == ["Bp"]
    dlg._toggle_tag("Bp")                        # all have it → remove from all
    flush()
    assert foldertags.tags_for(a) == [] and foldertags.tags_for(b) == []
    dlg.done(0)


def test_no_ticked_albums_is_noop(qapp, tmp_path, flush):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    dlg._untick_all()
    dlg._toggle_tag("Az")
    flush()
    assert foldertags.tags_for(a) == []
    dlg.done(0)


def test_merely_selecting_a_folder_does_not_tag_it(qapp, tmp_path, flush):
    """Browsing highlights folders; only a tick may apply a tag."""
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a])
    dlg._untick_all()
    _settle(qapp, dlg._fs, str(tmp_path))
    dlg._tree.setCurrentIndex(dlg._fs.index(b))   # selected, not ticked
    assert dlg._checked_albums() == []
    dlg._toggle_tag("Az")
    flush()
    assert foldertags.tags_for(b) == []
    dlg.done(0)


def test_tag_buttons_disabled_without_ticks(qapp, tmp_path):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    assert all(b.isEnabled() for b in dlg._tag_btns)
    dlg._untick_all()
    assert not any(b.isEnabled() for b in dlg._tag_btns)
    assert "Tick one or more folders" in dlg._tag_lbl.text()
    dlg.done(0)


def test_ticked_panel_shows_tags(qapp, tmp_path, flush):
    favorites.set_link_mode("copy")
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    assert "(untagged)" in dlg._picked_model.item(0).text()
    dlg._toggle_tag("Az")
    flush()
    assert "Az" in dlg._picked_model.item(0).text()
    dlg.done(0)


# -- picker parity -------------------------------------------------------------
def test_has_import_picker_controls(qapp, tmp_path):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    # Media-class filter, same as the import picker.
    assert dlg._type_combo.currentData() == "all"
    i = dlg._type_combo.findData("videos")
    dlg._type_combo.setCurrentIndex(i)
    qapp.processEvents()
    nf = set(dlg._fs.nameFilters())
    assert "*.mp4" in nf and "*.jpg" not in nf
    # Hover preview, same as the import picker.
    assert dlg._hover is not None
    dlg.done(0)


def test_main_window_passes_loader(qapp, tmp_path):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    assert w._album_tags_btn.text() == "Tag albums"
    dlg = AlbumTagsDialog([_album(tmp_path, "Loaded")], w, loader=w._loader)
    assert dlg._loader is w._loader
    dlg.done(0)


# -- regressions caught in review ---------------------------------------------
def test_mirror_folders_are_reported_not_silently_skipped(
        qapp, tmp_path, flush, monkeypatch):
    """foldertags refuses to re-mirror a folder that is itself a mirror copy;
    the tree makes those easy to tick, so the dialog must say so."""
    from gallery_py_qt import config
    inside = os.path.join(config.FAVORITES_DIR, "folder tags", "Az", "Nested")
    os.makedirs(inside)
    Image.new("RGB", (8, 8)).save(os.path.join(inside, "n.jpg"))
    dlg = AlbumTagsDialog([inside])
    assert dlg._checked_albums() == [inside]
    shown = []
    monkeypatch.setattr(
        "gallery_py_qt.album_tags_dialog.QMessageBox.information",
        lambda *a, **k: shown.append(a[2]))
    dlg._toggle_tag("Bp")
    flush()
    assert shown, "expected the user to be told the folder was skipped"
    assert "skipped" in shown[0]
    dlg.done(0)


def test_mixed_batch_tags_the_valid_folders(qapp, tmp_path, flush, monkeypatch):
    from gallery_py_qt import config
    favorites.set_link_mode("copy")
    # A genuine mirror-tree folder (under "folder tags") is the only thing skipped.
    mirror = os.path.join(config.FAVORITES_DIR, "folder tags", "Az", "Nested")
    os.makedirs(mirror)
    good = _album(tmp_path, "Good")
    dlg = AlbumTagsDialog([mirror, good])
    monkeypatch.setattr(
        "gallery_py_qt.album_tags_dialog.QMessageBox.information",
        lambda *a, **k: None)
    dlg._toggle_tag("Az")
    flush()
    assert foldertags.tags_for(good) == ["Az"]     # the valid one still tagged
    assert foldertags.tags_for(mirror) == []       # the mirror copy is skipped
    dlg.done(0)


def test_folder_under_favorites_is_taggable(qapp, tmp_path, flush, monkeypatch):
    """Regression: a library folder that merely lives under the Gallery
    Favorites directory (but outside the 'folder tags' mirror) must tag
    normally — it previously tripped a bogus 'skipped' message every time."""
    from gallery_py_qt import config
    favorites.set_link_mode("copy")
    album = os.path.join(config.FAVORITES_DIR, "My Library", "Trip")
    os.makedirs(album)
    Image.new("RGB", (8, 8)).save(os.path.join(album, "p.jpg"))
    dlg = AlbumTagsDialog([album])
    assert not dlg._is_mirror_folder(album)
    warned = []
    monkeypatch.setattr(
        "gallery_py_qt.album_tags_dialog.QMessageBox.information",
        lambda *a, **k: warned.append(a))
    dlg._toggle_tag("Az")
    flush()
    assert warned == []                            # no bogus skip dialog
    assert foldertags.tags_for(album) == ["Az"]
    assert os.path.isdir(
        os.path.join(foldertags.folder_tags_root(), "Az", "Trip"))
    dlg.done(0)


def test_goto_retries_once_the_directory_loads(qapp, tmp_path):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([])
    dlg._goto(a)
    if dlg._pending_goto:                          # model was still cold
        for _ in range(200):
            qapp.processEvents()
            time.sleep(0.005)
            if not dlg._pending_goto:
                break
    assert dlg._fs.index(a).isValid()
    assert dlg._pending_goto == ""
    dlg.done(0)


def test_close_releases_loader_and_tiles(qapp, tmp_path):
    """A closed dialog must stop receiving the shared loader's broadcasts and
    drop its thumbnail model (it stays parented to the window otherwise)."""
    from gallery_py_qt.loader import ThumbnailLoader
    from PySide6.QtWidgets import QWidget
    loader = ThumbnailLoader()
    parent = QWidget()
    a = _album(tmp_path, "One", n=3)
    dlg = AlbumTagsDialog([a], parent, loader=loader)
    assert dlg._loader is loader and dlg._hover._loader is loader
    dlg.done(0)
    assert dlg._loader is None and dlg._hover._loader is None
    assert dlg._tiles == {} and dlg._contents_model.rowCount() == 0


def test_sibling_of_favorites_is_not_skipped(qapp, tmp_path, flush, monkeypatch):
    """A folder that only shares the Favorites leaf-name prefix (a sibling, not
    a child) must still be taggable — not misreported as 'inside Favorites'."""
    from gallery_py_qt import config
    favorites.set_link_mode("copy")
    # Sibling whose path starts with the Favorites path string but isn't under it.
    sibling = config.FAVORITES_DIR + "_backup"
    os.makedirs(sibling)
    Image.new("RGB", (8, 8)).save(os.path.join(sibling, "p.jpg"))
    dlg = AlbumTagsDialog([sibling])
    assert not dlg._is_mirror_folder(sibling)
    warned = []
    monkeypatch.setattr(
        "gallery_py_qt.album_tags_dialog.QMessageBox.information",
        lambda *a, **k: warned.append(a))
    dlg._toggle_tag("Az")
    flush()
    assert warned == []                          # no bogus "skipped" dialog
    assert foldertags.tags_for(sibling) == ["Az"]
    dlg.done(0)
