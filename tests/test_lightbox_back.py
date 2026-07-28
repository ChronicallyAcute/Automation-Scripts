"""Full-screen viewer: exit / back controls that return to the previous set."""
from __future__ import annotations

import pytest
from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.lightbox import Lightbox
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


@pytest.fixture
def lb(qapp, tmp_path):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"i{i}.jpg") for i in range(3)]
    for p in ps:
        Image.new("RGB", (48, 32)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (48, 32) for p in ps})
    box = Lightbox(m, Favorites())
    yield box, ps
    box.close()


def test_back_button_defaults_to_gallery(lb):
    box, _ = lb
    assert box._return_kind == "gallery"
    assert box._back_btn.text() == f"{config.ICON_BACK} Gallery"
    assert not box._back_btn.isHidden()          # always offered, never hidden


def test_back_button_labels_multiview_origin(lb):
    box, _ = lb
    box.set_return_kind("multiview")
    assert box._back_btn.text() == f"{config.ICON_BACK} Multi-view"
    assert "multi-view set" in box._back_btn.toolTip()


def test_back_returns_to_the_multiview_set(lb):
    box, _ = lb
    box.set_return_kind("multiview")
    got = []
    box.returnToMulti.connect(lambda: got.append(1))
    box._go_back()
    assert got == [1]


def test_back_from_gallery_just_closes(lb):
    box, _ = lb
    box.set_return_kind("gallery")
    got = []
    box.returnToMulti.connect(lambda: got.append(1))
    box._go_back()
    assert got == []                              # nothing to return to


def test_escape_uses_the_same_return_path(lb):
    box, _ = lb
    box.set_return_kind("multiview")
    got = []
    box.returnToMulti.connect(lambda: got.append(1))
    box._on_escape()
    assert got == [1]


def test_escape_dismisses_help_before_leaving(qapp, lb):
    box, _ = lb
    box.show()                     # isVisible() is False under a hidden parent
    qapp.processEvents()
    box.set_return_kind("multiview")
    got = []
    box.returnToMulti.connect(lambda: got.append(1))
    box._toggle_help()
    assert box._help.isVisible()
    box._on_escape()                              # first Esc closes help only
    assert not box._help.isVisible()
    assert got == []
    box._on_escape()                              # second Esc leaves
    assert got == [1]


def test_exit_fullscreen_button_tracks_window_state(lb):
    box, _ = lb
    assert box._exit_fs_btn.isHidden()            # windowed → nothing to exit
    box.showFullScreen()
    box._sync_fs_btn()
    assert not box._exit_fs_btn.isHidden()
    box.showNormal()
    box._sync_fs_btn()
    assert box._exit_fs_btn.isHidden()


def test_legacy_alias_still_resolves(lb):
    """Older code addressed the control as _back_mv_btn."""
    box, _ = lb
    assert box._back_mv_btn is box._back_btn


def test_main_window_sets_the_return_kind(qapp, tmp_path, monkeypatch):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    ps = [str(tmp_path / "a.jpg")]
    Image.new("RGB", (32, 32)).save(ps[0])
    w._model.set_paths(ps)
    w._model.set_dims({ps[0]: (32, 32)})
    # Opened from the gallery grid.
    w._open_lightbox(0)
    box = [c for c in w.children() if isinstance(c, Lightbox)][-1]
    assert box._return_kind == "gallery"
    box.close()
    # Opened from multi-view.
    w._open_lightbox(0, from_mv=True)
    box2 = [c for c in w.children() if isinstance(c, Lightbox)][-1]
    assert box2._return_kind == "multiview"
    box2.close()
