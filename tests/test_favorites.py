"""Favourites: per-folder mirror + trash / restore."""
from __future__ import annotations
import os

from gallery_py_qt import config
from gallery_py_qt.engine import favorites
from gallery_py_qt.engine.favorites import Favorites


def test_mirror_into_per_folder_favorites(make_image, flush):
    img = make_image("a.jpg")
    favs = Favorites()
    favs.toggle(img)
    flush()
    mirror = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert os.path.exists(mirror)


def test_unfavorite_removes_mirror(make_image, flush):
    img = make_image("a.jpg")
    favs = Favorites()
    favs.toggle(img)
    flush()
    favs.toggle(img)
    flush()
    mirror = os.path.join(os.path.dirname(img), "Favorites", "a.jpg")
    assert not os.path.exists(mirror)


def test_mirror_dir_is_sibling_favorites(make_image):
    img = make_image("sub/a.jpg")
    assert favorites.mirror_dir_for(img) == os.path.join(
        os.path.dirname(img), "Favorites")


def test_trash_and_restore(make_image):
    img = make_image("a.jpg")
    dest = favorites.trash_file(img)
    assert dest and not os.path.exists(img) and os.path.exists(dest)
    assert favorites.restore_file(img, dest)
    assert os.path.exists(img)


def test_trash_manifest_records_origin(make_image):
    img = make_image("a.jpg")
    dest = favorites.trash_file(img)
    entry = [t for t in favorites.list_trash()
             if t["path"] == dest]
    assert entry and entry[0]["orig"] == img


def test_favorites_persist_across_instances(make_image):
    img = make_image("a.jpg")
    Favorites().toggle(img)
    assert Favorites().is_fav(img)
