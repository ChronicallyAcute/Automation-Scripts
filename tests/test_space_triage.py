"""Size triage, hard-link-aware duplicates, folder-free scanning, collapsing."""
from __future__ import annotations
import os
import shutil
import time

import pytest
from PIL import Image

from gallery_py_qt.engine import dupes, foldersize, tags


def _img(path, color=(3, 4, 5), size=(16, 16)):
    Image.new("RGB", size, color).save(path)
    return str(path)


def _bin(path, nbytes, fill=b"x"):
    with open(path, "wb") as f:
        f.write(fill * nbytes)
    return str(path)


def _can_hardlink(tmp_path) -> bool:
    a = _bin(tmp_path / "_hl_a", 8)
    try:
        os.link(a, str(tmp_path / "_hl_b"))
        return True
    except OSError:
        return False
    finally:
        for n in ("_hl_a", "_hl_b"):
            try:
                os.remove(tmp_path / n)
            except OSError:
                pass


# -- hard-link awareness ------------------------------------------------------

def test_hardlinks_are_recognised_as_one_file(tmp_path):
    if not _can_hardlink(tmp_path):
        pytest.skip("filesystem has no hard links")
    a = _bin(tmp_path / "a.bin", 4096)
    b = str(tmp_path / "b.bin")
    os.link(a, b)
    assert dupes.is_all_one_file([a, b])


def test_real_copies_are_not_one_file(tmp_path):
    a = _bin(tmp_path / "a.bin", 4096)
    b = _bin(tmp_path / "b.bin", 4096)
    assert not dupes.is_all_one_file([a, b])


def test_reclaim_ignores_space_that_is_not_really_used(tmp_path):
    """Deleting one of two hard links frees nothing — the bytes stay."""
    if not _can_hardlink(tmp_path):
        pytest.skip("filesystem has no hard links")
    a = _bin(tmp_path / "a.bin", 1000)
    link = str(tmp_path / "link.bin")
    os.link(a, link)
    assert dupes.reclaimable_bytes(sorted([a, link])) == 0


def test_reclaim_counts_a_genuine_copy(tmp_path):
    a = _bin(tmp_path / "a.bin", 1000)
    b = _bin(tmp_path / "b.bin", 1000)
    assert dupes.reclaimable_bytes(sorted([a, b])) == 1000


def test_reclaim_mixes_links_and_copies_correctly(tmp_path):
    if not _can_hardlink(tmp_path):
        pytest.skip("filesystem has no hard links")
    a = _bin(tmp_path / "a.bin", 1000)
    link = str(tmp_path / "link.bin")
    os.link(a, link)
    b = _bin(tmp_path / "b.bin", 1000)
    group = sorted([a, link, b])
    # Keeping only the first: the a/link cluster keeps a survivor unless both go.
    assert dupes.reclaimable_bytes(group) <= 2000
    # Deleting the true copy alone frees exactly one file's worth.
    assert dupes.reclaimable_bytes(group, doomed=[b]) == 1000
    # Deleting both names of the linked file frees one file's worth.
    assert dupes.reclaimable_bytes(group, doomed=[a, link]) == 1000


def test_link_clusters_group_shared_storage(tmp_path):
    if not _can_hardlink(tmp_path):
        pytest.skip("filesystem has no hard links")
    a = _bin(tmp_path / "a.bin", 64)
    link = str(tmp_path / "link.bin")
    os.link(a, link)
    b = _bin(tmp_path / "b.bin", 64)
    clusters = dupes.link_clusters([a, link, b])
    assert sorted(len(c) for c in clusters) == [1, 2]


def test_storage_id_is_none_for_a_missing_file(tmp_path):
    assert dupes.storage_id(str(tmp_path / "nope.bin")) is None


# -- choosing which copy survives ---------------------------------------------

def test_best_tagged_wins(tmp_path):
    a, b = _img(tmp_path / "a.png"), _img(tmp_path / "b.png")
    t = tags.get_tags()
    tags.toggle_tag(a, t[0])
    tags.toggle_tag(b, t[0])
    tags.toggle_tag(b, t[1])
    assert dupes.best_tagged([a, b]) == b


def test_best_tagged_falls_back_to_newest(tmp_path):
    a = _img(tmp_path / "a.png")
    time.sleep(0.01)
    b = _img(tmp_path / "b.png")
    os.utime(b, (time.time() + 10, time.time() + 10))
    assert dupes.best_tagged([a, b]) == b        # neither tagged → newest


def test_best_tagged_of_empty_is_empty():
    assert dupes.best_tagged([]) == ""


def test_collapse_keeps_one_per_group(tmp_path):
    a = _img(tmp_path / "a.png")
    b = str(tmp_path / "b.png")
    shutil.copy(a, b)
    c = _img(tmp_path / "c.png", color=(200, 1, 1))
    tags.toggle_tag(b, tags.get_tags()[0])
    groups = dupes.find_duplicates([a, b, c])
    kept, hidden = dupes.collapse([a, b, c], groups)
    assert b in kept and a not in kept and c in kept
    assert hidden[b] == [a]


def test_collapse_preserves_order(tmp_path):
    c = _img(tmp_path / "c.png", color=(9, 9, 9))
    a = _img(tmp_path / "a.png")
    kept, _ = dupes.collapse([c, a], [])
    assert kept == [c, a]                   # no groups → untouched, in order


def test_collapse_ignores_groups_not_loaded(tmp_path):
    a = _img(tmp_path / "a.png")
    kept, hidden = dupes.collapse([a], [[a, str(tmp_path / "elsewhere.png")]])
    assert kept == [a] and hidden == {}


# -- folder sizes -------------------------------------------------------------

def test_measure_totals_a_tree(tmp_path):
    sub = tmp_path / "sub"
    sub.mkdir()
    _bin(tmp_path / "a.bin", 1000)
    _bin(sub / "b.bin", 500)
    total, count, complete = foldersize.measure(str(tmp_path))
    assert total == 1500 and count == 2 and complete


def test_measure_counts_a_hardlink_once(tmp_path):
    if not _can_hardlink(tmp_path):
        pytest.skip("filesystem has no hard links")
    a = _bin(tmp_path / "a.bin", 1000)
    os.link(a, str(tmp_path / "second_name.bin"))
    total, count, _ = foldersize.measure(str(tmp_path))
    assert total == 1000, "one file with two names still occupies one file"
    assert count == 1


def test_measure_does_not_follow_directory_symlinks(tmp_path):
    """A symlink pointing up the tree would otherwise loop or double-count.

    The link node itself is not billed either: its size is the length of the
    path it stores, which is noise in a report about where the space went.
    """
    sub = tmp_path / "sub"
    sub.mkdir()
    _bin(sub / "a.bin", 800)
    try:
        os.symlink(str(tmp_path), str(sub / "loop"), target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    total, _count, _ = foldersize.measure(str(tmp_path))
    assert total == 800


def test_measure_can_be_cancelled(tmp_path):
    for i in range(5):
        _bin(tmp_path / f"f{i}.bin", 100)
    total, _count, complete = foldersize.measure(
        str(tmp_path), cancelled=lambda: True)
    assert not complete and total == 0


def test_children_sizes_ranks_biggest_first(tmp_path):
    big, small = tmp_path / "big", tmp_path / "small"
    big.mkdir()
    small.mkdir()
    _bin(big / "x.bin", 50000)
    _bin(small / "y.bin", 10)
    rows = foldersize.children_sizes(str(tmp_path))
    assert [e.name for e in rows] == ["big", "small"]
    assert rows[0].bytes == 50000 and rows[0].is_dir


def test_children_sizes_includes_loose_files(tmp_path):
    _bin(tmp_path / "loose.bin", 77)
    rows = foldersize.children_sizes(str(tmp_path))
    assert [(e.name, e.bytes, e.is_dir) for e in rows] == [("loose.bin", 77, False)]


def test_children_sizes_of_a_missing_folder_is_empty(tmp_path):
    assert foldersize.children_sizes(str(tmp_path / "nope")) == []


def test_fmt_size_scales():
    assert foldersize.fmt_size(12) == "12 B"
    assert foldersize.fmt_size(2048) == "2 KB"
    assert "MB" in foldersize.fmt_size(5 * 1024 * 1024)
    assert "GB" in foldersize.fmt_size(3 * 1024 ** 3)


# -- the viewer collapses duplicates ------------------------------------------

def _model():
    from gallery_py_qt.engine.favorites import Favorites
    from gallery_py_qt.loader import ThumbnailLoader
    from gallery_py_qt.model import GalleryModel
    return GalleryModel(Favorites(), ThumbnailLoader())


def test_collapsing_is_off_until_asked(qapp, tmp_path):
    a = _img(tmp_path / "a.png")
    b = str(tmp_path / "b.png")
    shutil.copy(a, b)
    m = _model()
    m.set_paths([a, b])
    m.set_duplicate_groups(dupes.find_duplicates([a, b]))
    assert m.rowCount() == 2, "nothing is hidden unless collapsing is enabled"
    assert m.duplicates_hidden() == 0


def test_collapsing_shows_the_best_tagged_copy(qapp, tmp_path):
    a = _img(tmp_path / "a.png")
    b = str(tmp_path / "b.png")
    shutil.copy(a, b)
    tags.toggle_tag(b, tags.get_tags()[0])
    m = _model()
    m.set_paths([a, b])
    m.set_duplicate_groups(dupes.find_duplicates([a, b]))
    m.set_collapse_duplicates(True)
    assert m.rowCount() == 1
    assert m.path_at(0) == b
    assert m.duplicates_hidden() == 1
    assert m.hidden_copies_of(b) == [a]


def test_tagging_the_other_copy_switches_which_is_shown(qapp, tmp_path):
    a = _img(tmp_path / "a.png")
    b = str(tmp_path / "b.png")
    shutil.copy(a, b)
    t = tags.get_tags()
    tags.toggle_tag(b, t[0])
    m = _model()
    m.set_paths([a, b])
    m.set_duplicate_groups(dupes.find_duplicates([a, b]))
    m.set_collapse_duplicates(True)
    assert m.path_at(0) == b
    tags.toggle_tag(a, t[0])
    tags.toggle_tag(a, t[1])            # a now carries more
    m.refresh_duplicate_choice()
    assert m.path_at(0) == a


def test_turning_collapsing_off_restores_every_copy(qapp, tmp_path):
    a = _img(tmp_path / "a.png")
    b = str(tmp_path / "b.png")
    shutil.copy(a, b)
    m = _model()
    m.set_paths([a, b])
    m.set_duplicate_groups(dupes.find_duplicates([a, b]))
    m.set_collapse_duplicates(True)
    assert m.rowCount() == 1
    m.set_collapse_duplicates(False)
    assert m.rowCount() == 2


def test_collapsing_leaves_unique_media_alone(qapp, tmp_path):
    a = _img(tmp_path / "a.png")
    c = _img(tmp_path / "c.png", color=(250, 1, 1))
    m = _model()
    m.set_paths([a, c])
    m.set_duplicate_groups(dupes.find_duplicates([a, c]))
    m.set_collapse_duplicates(True)
    assert m.rowCount() == 2


# -- duplicate finder without importing ---------------------------------------

def _drain(qapp, dlg, tries=400):
    for _ in range(tries):
        qapp.processEvents()
        time.sleep(0.005)
        if dlg._groups:
            return True
    return False


def test_dialog_opens_idle_with_nothing_loaded(qapp):
    from gallery_py_qt.dupes_dialog import DuplicatesDialog
    dlg = DuplicatesDialog([], None)
    assert "add a folder" in dlg._header.text().lower()
    dlg.done(0)


def test_dialog_scans_a_folder_without_importing(qapp, tmp_path):
    from gallery_py_qt.dupes_dialog import DuplicatesDialog
    a = _img(tmp_path / "a.png")
    shutil.copy(a, tmp_path / "copy.png")
    dlg = DuplicatesDialog([], None, roots=[str(tmp_path)], recursive=True)
    assert _drain(qapp, dlg), "folder scan produced no groups"
    found = {os.path.basename(p) for g in dlg._groups for p in g}
    assert found == {"a.png", "copy.png"}
    dlg.done(0)


def test_dialog_finds_duplicates_across_subfolders(qapp, tmp_path):
    from gallery_py_qt.dupes_dialog import DuplicatesDialog
    sub = tmp_path / "sub"
    sub.mkdir()
    a = _img(tmp_path / "a.png")
    shutil.copy(a, sub / "same.png")
    dlg = DuplicatesDialog([], None, roots=[str(tmp_path)], recursive=True)
    assert _drain(qapp, dlg)
    assert len(dlg._groups) == 1 and len(dlg._groups[0]) == 2
    dlg.done(0)


def test_best_tagged_selection_spares_the_richest_copy(qapp, tmp_path):
    from gallery_py_qt.dupes_dialog import DuplicatesDialog
    a = _img(tmp_path / "a.png")
    b = str(tmp_path / "b.png")
    shutil.copy(a, b)
    tags.toggle_tag(b, tags.get_tags()[0])
    dlg = DuplicatesDialog([a, b], None)
    assert _drain(qapp, dlg)
    dlg._select_all_but_best_tagged()
    assert dlg._selected_paths() == [a], "the tagged copy must not be ticked"
    dlg.done(0)


# -- import picker triage ------------------------------------------------------

def test_picker_exposes_size_sorting(qapp, tmp_path):
    from gallery_py_qt.main_window import _FolderPickDlg
    dlg = _FolderPickDlg(recents=[])
    labels = [a.text() for a in dlg._sort_btn._sort_menu.actions()]
    assert "Largest first" in labels and "Smallest first" in labels
    assert not dlg._tree.isColumnHidden(1), "the Size column must be visible"
    assert dlg._tree.isSortingEnabled()
    dlg.done(0)


def test_picker_reports_the_weight_of_the_selection(qapp, tmp_path):
    from gallery_py_qt.main_window import _FolderPickDlg
    f = _bin(tmp_path / "clip.bin", 2048)
    dlg = _FolderPickDlg(recents=[])
    assert "Nothing selected" in dlg._sel_total.text()
    dlg._fs.add(f)
    dlg._refresh_list()
    assert "2 KB" in dlg._sel_total.text()
    dlg.done(0)


def test_picker_flags_folders_as_unmeasured(qapp, tmp_path):
    from gallery_py_qt.main_window import _FolderPickDlg
    dlg = _FolderPickDlg(recents=[])
    dlg._fs.add(str(tmp_path))
    dlg._refresh_list()
    assert "folder" in dlg._sel_total.text()
    dlg.done(0)


# -- link mode: does it actually do what it says? -----------------------------

def test_place_file_reports_the_mode_it_achieved(tmp_path):
    from gallery_py_qt.engine import favorites as fav
    src = _bin(tmp_path / "src.bin", 128)
    fav.set_link_mode("copy")
    assert fav.place_file(src, str(tmp_path / "dst.bin")) == "copy"


def test_hardlink_mode_reports_hardlink_when_possible(tmp_path):
    from gallery_py_qt.engine import favorites as fav
    if not _can_hardlink(tmp_path):
        pytest.skip("filesystem has no hard links")
    src = _bin(tmp_path / "src.bin", 128)
    fav.set_link_mode("hardlink")
    try:
        assert fav.place_file(src, str(tmp_path / "dst.bin")) == "hardlink"
    finally:
        fav.set_link_mode("copy")


def test_a_failed_link_falls_back_to_copy_and_says_so(tmp_path, monkeypatch):
    """The silent fallback is how 'hardlink' can still fill a drive."""
    from gallery_py_qt.engine import favorites as fav
    src = _bin(tmp_path / "src.bin", 128)
    fav.reset_link_fallbacks()
    fav.set_link_mode("hardlink")

    def _boom(*_a, **_k):
        raise OSError("Invalid cross-device link")
    monkeypatch.setattr(os, "link", _boom)
    try:
        assert fav.place_file(src, str(tmp_path / "dst.bin")) == "copy"
    finally:
        fav.set_link_mode("copy")
    n, reason = fav.link_fallbacks()
    assert n == 1 and "cross-device" in reason
    assert os.path.getsize(tmp_path / "dst.bin") == 128   # a real copy exists


def test_probe_reports_copy_when_copy_is_selected(tmp_path):
    from gallery_py_qt.engine import favorites as fav
    fav.set_link_mode("copy")
    mode, why = fav.probe_link_mode(str(tmp_path))
    assert mode == "copy" and why


def test_probe_detects_that_linking_would_fail(tmp_path, monkeypatch):
    from gallery_py_qt.engine import favorites as fav
    fav.set_link_mode("hardlink")

    def _boom(*_a, **_k):
        raise OSError("Invalid cross-device link")
    monkeypatch.setattr(os, "link", _boom)
    try:
        mode, why = fav.probe_link_mode(str(tmp_path))
    finally:
        fav.set_link_mode("copy")
    assert mode == "copy" and "NOT possible" in why


def test_probe_leaves_no_scratch_files_behind(tmp_path):
    from gallery_py_qt.engine import favorites as fav
    fav.set_link_mode("hardlink")
    try:
        fav.probe_link_mode(str(tmp_path))
    finally:
        fav.set_link_mode("copy")
    assert not [p for p in os.listdir(tmp_path) if "linkprobe" in p]


def test_storage_report_labels_each_place(tmp_path):
    rep = foldersize.app_storage_report([])
    labels = {p["label"] for p in rep["places"]}
    assert {"Thumbnail cache", "Trash", "Tag folders"} <= labels
    assert rep["total"] >= 0


def test_storage_report_counts_favourites_mirrors(tmp_path):
    media = tmp_path / "media"
    media.mkdir()
    a = _img(media / "a.png")
    mirror = media / "Favorites"
    mirror.mkdir()
    _bin(mirror / "a.png", 4321)
    rep = foldersize.app_storage_report([a])
    assert rep["mirror_dirs"] == 1 and rep["mirrors"] == 4321


# -- the import tree must actually show names -----------------------------------

def test_the_name_column_stretches(qapp, tmp_path):
    """A fixed-width Name column plus per-level indentation left deep folders
    with nothing to draw their title in."""
    from PySide6.QtWidgets import QHeaderView
    from gallery_py_qt.main_window import _FolderPickDlg
    dlg = _FolderPickDlg(recents=[])
    dlg.resize(900, 580)
    hdr = dlg._tree.header()
    assert hdr.sectionResizeMode(0) == QHeaderView.ResizeMode.Stretch
    assert not hdr.stretchLastSection()
    dlg.done(0)


def test_a_deeply_nested_folder_still_has_room_for_its_name(qapp, tmp_path):
    from gallery_py_qt.main_window import _FolderPickDlg
    deep = tmp_path / "Favorites" / "G" / "X" / "T - Face"
    deep.mkdir(parents=True)
    dlg = _FolderPickDlg(recents=[])
    dlg.resize(900, 580)
    dlg.show()
    qapp.processEvents()
    tree = dlg._tree
    depth = len(deep.parts)
    room = tree.columnWidth(0) - depth * tree.indentation() - 40
    assert room > 80, f"only {room}px left for the folder name"
    dlg.done(0)


def test_indentation_is_not_extravagant(qapp):
    from gallery_py_qt.main_window import _FolderPickDlg
    dlg = _FolderPickDlg(recents=[])
    assert dlg._tree.indentation() <= 12
    dlg.done(0)


def test_the_picker_lists_symlinked_media(qapp, tmp_path):
    """A tag folder holds links, not copies; they must appear like any file."""
    import time as _t
    from gallery_py_qt.fs_picker import _CheckFSModel
    original = _img(tmp_path / "orig.png")
    folder = tmp_path / "T - Face"
    folder.mkdir()
    try:
        os.symlink(original, str(folder / "link.png"))
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    fs = _CheckFSModel()
    idx = fs.setRootPath(str(folder))
    for _ in range(300):
        qapp.processEvents()
        _t.sleep(0.005)
        if fs.rowCount(idx):
            break
    names = {fs.fileName(fs.index(r, 0, idx)) for r in range(fs.rowCount(idx))}
    assert "link.png" in names
