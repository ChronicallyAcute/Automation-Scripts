"""Filesystem operations backing the album manager (move / rename / delete)."""
from __future__ import annotations
import os

from PIL import Image

from gallery_py_qt.engine import fileops, favorites


def _img(path):
    Image.new("RGB", (8, 8)).save(path)
    return str(path)


# -- move ----------------------------------------------------------------------
def test_move_files_and_folder(tmp_path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    a, b = _img(src / "a.jpg"), _img(src / "b.jpg")
    dest = tmp_path / "dest"
    dest.mkdir()
    moved, errors = fileops.move_paths([a, b, str(src / "sub")], str(dest))
    assert len(moved) == 3 and errors == []
    assert sorted(os.listdir(dest)) == ["a.jpg", "b.jpg", "sub"]
    assert not os.path.exists(a)


def test_move_collision_gets_suffix(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    _img(dest / "a.jpg")                         # occupant
    src = tmp_path / "a.jpg"
    _img(src)
    moved, errors = fileops.move_paths([str(src)], str(dest))
    assert errors == []
    assert os.path.basename(moved[0][1]) == "a_1.jpg"
    assert sorted(os.listdir(dest)) == ["a.jpg", "a_1.jpg"]


def test_move_folder_into_itself_refused(tmp_path):
    d = tmp_path / "album"
    (d / "inner").mkdir(parents=True)
    moved, errors = fileops.move_paths([str(d)], str(d / "inner"))
    assert moved == [] and "itself" in errors[0][1]
    assert os.path.isdir(d)                      # untouched


def test_move_missing_and_bad_dest(tmp_path):
    moved, errors = fileops.move_paths([str(tmp_path / "ghost")],
                                       str(tmp_path / "nope"))
    assert moved == [] and "not a folder" in errors[0][1]
    dest = tmp_path / "dest"
    dest.mkdir()
    moved, errors = fileops.move_paths([str(tmp_path / "ghost")], str(dest))
    assert moved == [] and "no longer exists" in errors[0][1]


def test_move_same_dir_is_noop(tmp_path):
    a = _img(tmp_path / "a.jpg")
    moved, errors = fileops.move_paths([a], str(tmp_path))
    assert moved == [] and errors == []          # already there
    assert os.path.exists(a)


# -- rename --------------------------------------------------------------------
def test_rename_ok(tmp_path):
    a = _img(tmp_path / "a.jpg")
    new, err = fileops.rename_path(a, "b.jpg")
    assert err is None and os.path.basename(new) == "b.jpg"
    assert os.path.exists(new) and not os.path.exists(a)


def test_rename_collision(tmp_path):
    a = _img(tmp_path / "a.jpg")
    _img(tmp_path / "b.jpg")
    new, err = fileops.rename_path(a, "b.jpg")
    assert new is None and "already exists" in err


def test_rename_rejects_separators_and_empty(tmp_path):
    a = _img(tmp_path / "a.jpg")
    assert fileops.rename_path(a, "x/y.jpg")[1]
    assert fileops.rename_path(a, "  ")[1] == "name cannot be empty"
    assert fileops.rename_path(a, "..")[1] == "invalid name"


def test_rename_unchanged_is_ok(tmp_path):
    a = _img(tmp_path / "a.jpg")
    new, err = fileops.rename_path(a, "a.jpg")
    assert err is None and new == os.path.abspath(a)


def test_rename_folder(tmp_path):
    d = tmp_path / "Album"
    d.mkdir()
    new, err = fileops.rename_path(str(d), "Renamed")
    assert err is None and os.path.isdir(new)
    assert os.path.basename(new) == "Renamed"


# -- delete (to trash) ---------------------------------------------------------
def test_delete_files_and_folder_go_to_trash(tmp_path):
    a = _img(tmp_path / "a.jpg")
    d = tmp_path / "Album"
    d.mkdir()
    _img(d / "inner.jpg")
    deleted, errors = fileops.delete_paths([a, str(d)])
    assert len(deleted) == 2 and errors == []
    assert not os.path.exists(a) and not os.path.isdir(d)
    # Recoverable: both appear in the trash listing (folders included now).
    names = {it["name"] for it in favorites.list_trash()}
    assert any(n.startswith("a") for n in names)
    assert "Album" in names


def test_delete_missing_reports_error(tmp_path):
    deleted, errors = fileops.delete_paths([str(tmp_path / "ghost")])
    assert deleted == [] and "no longer exists" in errors[0][1]


def test_deleted_folder_restores(tmp_path):
    d = tmp_path / "Album"
    d.mkdir()
    _img(d / "x.jpg")
    deleted, _ = fileops.delete_paths([str(d)])
    trash = deleted[0][1]
    back = favorites.restore_from_trash(trash)
    assert back and os.path.isdir(back) and os.path.exists(os.path.join(back, "x.jpg"))


def test_purge_removes_trashed_folder(tmp_path):
    d = tmp_path / "Album"
    d.mkdir()
    _img(d / "x.jpg")
    deleted, _ = fileops.delete_paths([str(d)])
    assert favorites.purge_item(deleted[0][1]) is True
    assert favorites.list_trash() == []


# -- make folder ---------------------------------------------------------------
def test_make_folder(tmp_path):
    new, err = fileops.make_folder(str(tmp_path), "New Album")
    assert err is None and os.path.isdir(new)
    _, err2 = fileops.make_folder(str(tmp_path), "New Album")
    assert err2 == "already exists"
    assert fileops.make_folder(str(tmp_path), "a/b")[1]
