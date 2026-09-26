"""Undo for file operations: engine inverses + the album dialog's Ctrl+Z stack."""
from __future__ import annotations
import os

from PIL import Image

from gallery_py_qt.album_tags_dialog import AlbumTagsDialog
from gallery_py_qt.engine import fileops


def _img(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (8, 8)).save(path)
    return path


# -- engine inverses ----------------------------------------------------------
def test_undo_move_returns_files(tmp_path):
    src_dir, dst_dir = tmp_path / "a", tmp_path / "b"
    dst_dir.mkdir()
    p = _img(str(src_dir / "x.jpg"))
    moved, errors = fileops.move_paths([p], str(dst_dir))
    assert not errors and len(moved) == 1
    assert not os.path.exists(p)

    restored, errors = fileops.undo_move(moved)
    assert (restored, errors) == (1, [])
    assert os.path.exists(p)


def test_undo_move_refuses_to_clobber(tmp_path):
    src_dir, dst_dir = tmp_path / "a", tmp_path / "b"
    dst_dir.mkdir()
    p = _img(str(src_dir / "x.jpg"))
    moved, _ = fileops.move_paths([p], str(dst_dir))
    _img(p)                                   # something new took the old name
    restored, errors = fileops.undo_move(moved)
    assert restored == 0 and len(errors) == 1


def test_undo_delete_restores_from_trash(tmp_path):
    p = _img(str(tmp_path / "d" / "x.jpg"))
    deleted, errors = fileops.delete_paths([p])
    assert not errors and not os.path.exists(p)
    restored, errors = fileops.undo_delete(deleted)
    assert (restored, errors) == (1, [])
    assert os.path.exists(p)


# -- album dialog stack -------------------------------------------------------
def test_dialog_undoes_move(qapp, tmp_path):
    root = tmp_path / "Root"
    sub = root / "Sub"
    sub.mkdir(parents=True)
    p = _img(str(root / "x.jpg"))
    dlg = AlbumTagsDialog([])
    dlg._do_move([p], str(sub))
    assert not os.path.exists(p)
    assert len(dlg._undo_stack) == 1
    dlg._undo_last()
    assert os.path.exists(p)
    assert dlg._undo_stack == []
    dlg.done(0)


def test_dialog_undoes_delete_and_rename(qapp, tmp_path):
    root = tmp_path / "Root"
    root.mkdir()
    p = _img(str(root / "x.jpg"))
    dlg = AlbumTagsDialog([])

    dlg._do_rename(p, "y.jpg")
    assert os.path.exists(str(root / "y.jpg"))
    dlg._undo_last()
    assert os.path.exists(p)                  # back to x.jpg

    dlg._do_delete([p])
    assert not os.path.exists(p)
    dlg._undo_last()
    assert os.path.exists(p)
    dlg.done(0)


def test_dialog_undo_is_lifo(qapp, tmp_path):
    root = tmp_path / "Root"
    sub = root / "Sub"
    sub.mkdir(parents=True)
    a = _img(str(root / "a.jpg"))
    b = _img(str(root / "b.jpg"))
    dlg = AlbumTagsDialog([])
    dlg._do_move([a], str(sub))
    dlg._do_move([b], str(sub))
    dlg._undo_last()                          # undoes b first
    assert os.path.exists(b) and not os.path.exists(a)
    dlg._undo_last()
    assert os.path.exists(a)
    dlg.done(0)
