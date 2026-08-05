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


# -- tagging: per-file across the folder's media -------------------------------
from gallery_py_qt.engine import tags as _tags


def _files_in(folder):
    return [os.path.join(folder, n) for n in sorted(os.listdir(folder))
            if n.endswith(".jpg")]


def test_tags_every_media_file_in_ticked_folder(qapp, tmp_path):
    a = _album(tmp_path, "One", n=3)
    dlg = AlbumTagsDialog([a])                   # 'a' ticked
    dlg._toggle_tag("Az")
    for f in _files_in(a):
        assert _tags.tags_for(f) == ["Az"]       # every file tagged
    dlg.done(0)


def test_tags_recurse_into_subfolders(qapp, tmp_path):
    a = _album(tmp_path, "One", n=1)
    sub = os.path.join(a, "sub")
    os.mkdir(sub)
    Image.new("RGB", (8, 8)).save(os.path.join(sub, "deep.jpg"))
    dlg = AlbumTagsDialog([a])
    dlg._toggle_tag("Bp")
    assert _tags.tags_for(os.path.join(sub, "deep.jpg")) == ["Bp"]
    dlg.done(0)


def test_tags_only_ticked_folders(qapp, tmp_path):
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a])                   # only 'a' ticked
    dlg._toggle_tag("Az")
    assert all(_tags.tags_for(f) == ["Az"] for f in _files_in(a))
    assert all(_tags.tags_for(f) == [] for f in _files_in(b))
    dlg.done(0)


def test_batch_toggle_removes_when_all_tagged(qapp, tmp_path):
    a = _album(tmp_path, "One")
    b = _album(tmp_path, "Two")
    dlg = AlbumTagsDialog([a, b])
    dlg._toggle_tag("Bp")                        # add to every file
    assert all(_tags.tags_for(f) == ["Bp"]
               for f in _files_in(a) + _files_in(b))
    dlg._toggle_tag("Bp")                        # all have it → remove
    assert all(_tags.tags_for(f) == []
               for f in _files_in(a) + _files_in(b))
    dlg.done(0)


def test_highlighted_files_are_the_target(qapp, tmp_path):
    from PySide6.QtCore import QItemSelectionModel
    a = _album(tmp_path, "One", n=3)
    dlg = AlbumTagsDialog([a])
    dlg._show_contents(a)
    m = dlg._contents_model
    # Highlight just the first file tile.
    row = next(r for r in range(m.rowCount())
               if not m.item(r).text().endswith("/"))
    target = m.item(row).data(Qt.ItemDataRole.UserRole)
    dlg._contents.selectionModel().select(
        m.index(row, 0), QItemSelectionModel.SelectionFlag.Select)
    assert dlg._selected_content_files() == [target]
    dlg._toggle_tag("Az")
    assert _tags.tags_for(target) == ["Az"]
    # The other files were NOT tagged (only the highlighted one).
    others = [f for f in _files_in(a) if f != target]
    assert all(_tags.tags_for(f) == [] for f in others)
    dlg.done(0)


def test_no_target_is_noop(qapp, tmp_path):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    dlg._untick_all()
    dlg._toggle_tag("Az")
    assert all(_tags.tags_for(f) == [] for f in _files_in(a))
    dlg.done(0)


def test_tag_buttons_disabled_without_target(qapp, tmp_path):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    assert all(b.isEnabled() for b in dlg._tag_btns)     # 'a' ticked
    dlg._untick_all()
    assert not any(b.isEnabled() for b in dlg._tag_btns)
    assert "Tick folders" in dlg._tag_lbl.text()
    dlg.done(0)


# -- picker parity -------------------------------------------------------------
def test_has_import_picker_controls(qapp, tmp_path):
    a = _album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    # Multi-select media-class filter, same as the import picker.
    acts = dlg._type_btn._class_actions
    acts["images"].setChecked(False)
    acts["gifs"].setChecked(False)               # videos only
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


# -- file management (rename / delete / cut-paste / new folder) ----------------
def _mk_album(tmp_path, name, n=2):
    d = tmp_path / name
    d.mkdir()
    for i in range(n):
        Image.new("RGB", (8, 8)).save(d / f"p{i}.jpg")
    return str(d)


def test_do_rename_updates_disk_and_tick(qapp, tmp_path):
    a = _mk_album(tmp_path, "Old")
    dlg = AlbumTagsDialog([a])
    assert dlg._checked_albums() == [a]
    assert dlg._do_rename(a, "New") is True
    new = os.path.join(str(tmp_path), "New")
    assert os.path.isdir(new) and not os.path.isdir(a)
    assert dlg._checked_albums() == [new]        # tick followed the rename
    dlg.done(0)


def test_do_rename_relocates_folder_tag(qapp, tmp_path, flush):
    from gallery_py_qt.engine import foldertags
    favorites.set_link_mode("copy")
    a = _mk_album(tmp_path, "Old")
    foldertags.toggle_folder_tag(a, "Az")
    flush()
    assert os.path.isdir(os.path.join(foldertags.folder_tags_root(), "Az", "Old"))
    dlg = AlbumTagsDialog([a])
    dlg._do_rename(a, "New")
    flush()
    new = os.path.join(str(tmp_path), "New")
    assert foldertags.tags_for(new) == ["Az"]
    assert os.path.isdir(os.path.join(foldertags.folder_tags_root(), "Az", "New"))
    assert not os.path.exists(os.path.join(foldertags.folder_tags_root(), "Az", "Old"))
    dlg.done(0)


def test_do_delete_moves_to_trash_and_unticks(qapp, tmp_path, monkeypatch):
    a = _mk_album(tmp_path, "Gone")
    dlg = AlbumTagsDialog([a])
    dlg._do_delete([a])
    assert not os.path.isdir(a)
    assert dlg._checked_albums() == []
    names = {it["name"] for it in favorites.list_trash()}
    assert "Gone" in names
    dlg.done(0)


def test_do_delete_forgets_folder_tag(qapp, tmp_path, flush):
    from gallery_py_qt.engine import foldertags
    favorites.set_link_mode("copy")
    a = _mk_album(tmp_path, "Tagged")
    foldertags.toggle_folder_tag(a, "Az")
    flush()
    assert os.path.isdir(os.path.join(foldertags.folder_tags_root(), "Az", "Tagged"))
    dlg = AlbumTagsDialog([a])
    dlg._do_delete([a])
    flush()
    assert foldertags.tags_for(a) == []
    assert not os.path.exists(
        os.path.join(foldertags.folder_tags_root(), "Az", "Tagged"))
    dlg.done(0)


def test_cut_paste_moves_selection(qapp, tmp_path):
    a = _mk_album(tmp_path, "One")
    b = _mk_album(tmp_path, "Two")
    dest = tmp_path / "Dest"
    dest.mkdir()
    dlg = AlbumTagsDialog([])
    dlg._cut_paths = [a, b]
    dlg._do_move(dlg._cut_paths, str(dest))
    assert sorted(os.listdir(dest)) == ["One", "Two"]
    assert not os.path.isdir(a) and not os.path.isdir(b)
    dlg.done(0)


def test_move_keeps_tick_and_relocates_tag(qapp, tmp_path, flush):
    from gallery_py_qt.engine import foldertags
    favorites.set_link_mode("copy")
    a = _mk_album(tmp_path, "Album")
    foldertags.toggle_folder_tag(a, "Bp")
    flush()
    dest = tmp_path / "Dest"
    dest.mkdir()
    dlg = AlbumTagsDialog([a])                    # 'a' ticked
    dlg._do_move([a], str(dest))
    flush()
    moved = os.path.join(str(dest), "Album")
    assert os.path.isdir(moved)
    assert dlg._checked_albums() == [moved]      # tick followed the move
    assert foldertags.tags_for(moved) == ["Bp"]
    assert os.path.isdir(os.path.join(foldertags.folder_tags_root(), "Bp", "Album"))
    dlg.done(0)


def test_new_folder_creates_dir(qapp, tmp_path):
    dlg = AlbumTagsDialog([])
    new = dlg._do_new_folder(str(tmp_path), "Fresh")
    assert new and os.path.isdir(new)
    dlg.done(0)


def test_deleting_browsed_folder_clears_contents(qapp, tmp_path, monkeypatch):
    a = _mk_album(tmp_path, "Browsed", n=3)
    dlg = AlbumTagsDialog([a])
    dlg._show_contents(a)
    assert dlg._contents_model.rowCount() == 3
    dlg._do_delete([a])
    assert dlg._browsing == ""
    assert dlg._contents_model.rowCount() == 0
    dlg.done(0)


def test_selected_paths_reads_tree_selection(qapp, tmp_path):
    a = _mk_album(tmp_path, "One")
    dlg = AlbumTagsDialog([a])
    _settle(qapp, dlg._fs, str(tmp_path))
    dlg._tree.setCurrentIndex(dlg._fs.index(a))
    assert a in dlg._selected_paths()
    dlg.done(0)


def test_move_errors_surface(qapp, tmp_path, monkeypatch):
    a = _mk_album(tmp_path, "One")
    warned = []
    monkeypatch.setattr(
        "gallery_py_qt.album_tags_dialog.QMessageBox.warning",
        lambda *a, **k: warned.append(a[2]))
    dlg = AlbumTagsDialog([])
    # Move a folder into its own subtree → refused, surfaced.
    dlg._do_move([a], os.path.join(a))
    assert warned and "could not be completed" in warned[0]
    dlg.done(0)


# -- contents pane: subfolders first + sort ------------------------------------
def _album_with_subs(tmp_path):
    root = tmp_path / "Root"
    root.mkdir()
    (root / "Zsub").mkdir()
    (root / "Asub").mkdir()
    (root / "Favorites").mkdir()               # excluded
    from PIL import Image
    Image.new("RGB", (40, 10)).save(root / "wide.jpg")     # area 400
    Image.new("RGB", (10, 10)).save(root / "small.png")    # area 100
    Image.new("RGB", (30, 30)).save(root / "big.gif")      # area 900
    return str(root)


def test_subfolders_listed_before_files(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    dlg = AlbumTagsDialog([])
    dlg._show_contents(root)
    m = dlg._contents_model
    labels = [m.item(r).text() for r in range(m.rowCount())]
    # Folders (with trailing /) come first, excluding "Favorites".
    assert labels[0].endswith("/") and labels[1].endswith("/")
    assert "Asub/" in labels[:2] and "Zsub/" in labels[:2]
    assert "Favorites/" not in labels
    # Then the media files.
    assert all(not lbl.endswith("/") for lbl in labels[2:])
    assert "0 folder" not in dlg._contents_hdr.text()
    assert "2 folder(s), 3 media file(s)" in dlg._contents_hdr.text()
    dlg.done(0)


def test_sort_by_name(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    dlg = AlbumTagsDialog([])
    dlg._sort_combo.setCurrentIndex(dlg._sort_combo.findData("name"))
    dlg._show_contents(root)
    files = [dlg._contents_model.item(r).text()
             for r in range(dlg._contents_model.rowCount())
             if not dlg._contents_model.item(r).text().endswith("/")]
    assert files == ["big.gif", "small.png", "wide.jpg"]
    dlg.done(0)


def test_sort_by_size(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    dlg = AlbumTagsDialog([])
    dlg._sort_combo.setCurrentIndex(dlg._sort_combo.findData("size"))
    dlg._show_contents(root)
    files = [dlg._contents_model.item(r).data(Qt.ItemDataRole.UserRole)
             for r in range(dlg._contents_model.rowCount())
             if not dlg._contents_model.item(r).text().endswith("/")]
    sizes = [os.path.getsize(p) for p in files]
    assert sizes == sorted(sizes, reverse=True)     # largest first
    dlg.done(0)


def test_sort_by_dimensions(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    dlg = AlbumTagsDialog([])
    dlg._sort_combo.setCurrentIndex(dlg._sort_combo.findData("dimensions"))
    dlg._show_contents(root)
    files = [dlg._contents_model.item(r).text()
             for r in range(dlg._contents_model.rowCount())
             if not dlg._contents_model.item(r).text().endswith("/")]
    assert files == ["big.gif", "wide.jpg", "small.png"]   # 900, 400, 100
    dlg.done(0)


def test_sort_by_tags(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    from gallery_py_qt.engine import tags as _tags
    _tags.toggle_tag(os.path.join(root, "small.png"), "Az")
    dlg = AlbumTagsDialog([])
    dlg._sort_combo.setCurrentIndex(dlg._sort_combo.findData("tags"))
    dlg._show_contents(root)
    files = [dlg._contents_model.item(r).text()
             for r in range(dlg._contents_model.rowCount())
             if not dlg._contents_model.item(r).text().endswith("/")]
    assert files[0] == "small.png"                 # tagged item first
    dlg.done(0)


def test_double_click_folder_navigates(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    from PIL import Image
    Image.new("RGB", (8, 8)).save(os.path.join(root, "Asub", "inner.jpg"))
    dlg = AlbumTagsDialog([])
    dlg._show_contents(root)
    # First row is a folder tile ("Asub/"); activate it.
    idx = dlg._contents_model.index(0, 0)
    dlg._on_contents_activated(idx)
    assert dlg._browsing == os.path.join(root, "Asub")
    dlg.done(0)


def test_changing_sort_rerenders(qapp, tmp_path):
    root = _album_with_subs(tmp_path)
    dlg = AlbumTagsDialog([])
    dlg._show_contents(root)
    dlg._sort_combo.setCurrentIndex(dlg._sort_combo.findData("name"))
    # _on_sort_changed re-rendered the same folder.
    assert dlg._browsing == root
    dlg.done(0)
