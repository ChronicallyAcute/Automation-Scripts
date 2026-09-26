"""Link-instead-of-copy placement for favourites + tag folders."""
from __future__ import annotations
import os

import pytest

from gallery_py_qt import config
from gallery_py_qt.engine import favorites, tags
from gallery_py_qt.engine.favorites import Favorites


@pytest.fixture(autouse=True)
def _reset_mode():
    yield
    favorites.set_link_mode("copy")


def test_copy_mode_default(make_image, flush):
    img = make_image("a.jpg")
    Favorites().toggle(img)
    flush()
    dest = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert os.path.isfile(dest) and not os.path.islink(dest)
    assert os.stat(dest).st_ino != os.stat(img).st_ino    # a real copy


def test_hardlink_mode(make_image, flush):
    favorites.set_link_mode("hardlink")
    img = make_image("a.jpg")
    Favorites().toggle(img)
    flush()
    dest = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert os.stat(dest).st_ino == os.stat(img).st_ino    # same inode = hardlink


def test_symlink_mode(make_image, flush):
    favorites.set_link_mode("symlink")
    img = make_image("a.jpg")
    Favorites().toggle(img)
    flush()
    dest = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert os.path.islink(dest)
    assert os.path.realpath(dest) == os.path.realpath(img)


def test_unfavorite_removes_link(make_image, flush):
    favorites.set_link_mode("symlink")
    img = make_image("a.jpg")
    favs = Favorites()
    favs.toggle(img)
    flush()
    favs.toggle(img)
    flush()
    dest = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert not os.path.lexists(dest)


def test_hardlink_falls_back_to_copy_when_unsupported(make_image, flush, monkeypatch):
    favorites.set_link_mode("hardlink")
    monkeypatch.setattr(favorites.os, "link",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("EXDEV")))
    img = make_image("a.jpg")
    Favorites().toggle(img)
    flush()
    dest = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert os.path.isfile(dest) and not os.path.islink(dest)   # fell back to copy


def test_tag_folder_uses_link_mode(make_image, flush):
    favorites.set_link_mode("hardlink")
    img = make_image("a.jpg")
    tags.toggle_tag(img, "BT")
    tags.sync_tag_folders(img, favored=True)
    flush()
    dest = os.path.join(config.FAVORITES_DIR, "BT", "a.jpg")
    assert os.stat(dest).st_ino == os.stat(img).st_ino


def test_invalid_mode_is_copy():
    favorites.set_link_mode("nonsense")
    assert favorites.LINK_MODE == "copy"
