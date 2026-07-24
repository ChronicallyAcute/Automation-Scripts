"""Batch tagging a grid multi-selection."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt.engine import tags


@pytest.fixture
def win(qapp, tmp_path):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    w.resize(1000, 700)
    w.show()
    qapp.processEvents()
    paths = [str(tmp_path / f"i{i}.jpg") for i in range(4)]
    for p in paths:
        Image.new("RGB", (32, 32)).save(p)
    w._model.set_paths(paths)
    w._model.set_dims({p: (100, 100) for p in paths})
    return w, paths


def test_batch_tag_adds_to_all(win, flush):
    w, paths = win
    w._batch_tag([0, 1, 2], "BT")
    flush()
    assert all("BT" in tags.tags_for(p) for p in paths[:3])
    assert "BT" not in tags.tags_for(paths[3])


def test_batch_tag_removes_when_all_have_it(win, flush):
    w, paths = win
    for p in paths[:3]:
        tags.toggle_tag(p, "WAM")
    w._batch_tag([0, 1, 2], "WAM")           # all have it -> remove from all
    flush()
    assert all("WAM" not in tags.tags_for(p) for p in paths[:3])


def test_batch_tag_partial_adds_to_missing(win, flush):
    w, paths = win
    tags.toggle_tag(paths[0], "Az")          # only one has it
    w._batch_tag([0, 1, 2], "Az")            # any missing -> add to all
    flush()
    assert all("Az" in tags.tags_for(p) for p in paths[:3])


def test_batch_tag_syncs_folders_for_favorited(win, flush):
    from gallery_py_qt import config
    w, paths = win
    w._favs.toggle(paths[0])                  # favourited
    w._batch_tag([0, 1], "Bcs")
    flush()
    # favourited + tagged -> copied into the tag folder; unfavourited not
    assert os.path.exists(os.path.join(config.FAVORITES_DIR, "Bcs", "i0.jpg"))
    assert not os.path.exists(os.path.join(config.FAVORITES_DIR, "Bcs", "i1.jpg"))


def test_context_menu_selects_clicked_cell_on_bare_view(qapp, tmp_path):
    # Bare GalleryView so nothing wires the modal _show_grid_context_menu.
    from PySide6.QtCore import QPoint
    from PySide6.QtGui import QContextMenuEvent
    from gallery_py_qt.engine.favorites import Favorites
    from gallery_py_qt.loader import ThumbnailLoader
    from gallery_py_qt.model import GalleryModel
    from gallery_py_qt.gallery_view import GalleryView
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"i{i}.jpg") for i in range(4)]
    for p in ps:
        Image.new("RGB", (32, 32)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (100, 100) for p in ps})
    gv = GalleryView()
    gv.setModel(m)
    gv.set_columns(4)
    gv.resize(800, 200)
    gv.show()
    qapp.processEvents()
    gv._relayout()
    got = []
    gv.contextMenu.connect(lambda gp: got.append(gp))
    x, y, cw, ch = gv._cells[0]
    ev = QContextMenuEvent(QContextMenuEvent.Reason.Mouse, QPoint(x + 2, y + 2),
                           gv.mapToGlobal(QPoint(x, y)))
    gv.contextMenuEvent(ev)
    assert got and 0 in gv.selected_rows()


def test_grid_menu_builder(win):
    """The builder returns a menu (Favourite / Tag / Delete) for a selection
    and None for an empty one — behavior of the tag actions is covered above."""
    w, _paths = win
    assert w._build_grid_menu([]) is None
    menu = w._build_grid_menu([0, 1])
    labels = " | ".join(a.text() for a in menu.actions())
    assert "Favourite" in labels and "Delete" in labels and "Tag" in labels
