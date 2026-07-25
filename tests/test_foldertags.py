"""Album (folder) tagging: mirror whole folders into a 'folder tags' subtree."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine import favorites, foldertags, tags


@pytest.fixture
def album(tmp_path):
    """A folder with two images, ready to be tagged as an album."""
    d = tmp_path / "Vacation"
    d.mkdir()
    for n in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16)).save(d / n)
    return str(d)


def _mirror_dir(tag: str, album_name: str) -> str:
    return os.path.join(foldertags.folder_tags_root(), tag, album_name)


# -- store ---------------------------------------------------------------------
def test_toggle_updates_store(album):
    assert foldertags.tags_for(album) == []
    assert foldertags.toggle_folder_tag(album, "Az") is True
    assert "Az" in foldertags.tags_for(album)
    assert foldertags.toggle_folder_tag(album, "Az") is False
    assert foldertags.tags_for(album) == []


def test_persists(album):
    foldertags.toggle_folder_tag(album, "Bp")
    foldertags._store = None                     # force reload from disk
    assert foldertags.tags_for(album) == ["Bp"]


# -- copy mirror ---------------------------------------------------------------
def test_tagging_copies_album(album, flush):
    favorites.set_link_mode("copy")
    foldertags.toggle_folder_tag(album, "Az")
    flush()
    mirror = _mirror_dir("Az", "Vacation")
    assert os.path.isdir(mirror)
    assert not os.path.islink(mirror)            # real copy
    assert sorted(os.listdir(mirror)) == ["a.jpg", "b.jpg"]


def test_untagging_removes_mirror(album, flush):
    favorites.set_link_mode("copy")
    foldertags.toggle_folder_tag(album, "Az")
    flush()
    assert os.path.isdir(_mirror_dir("Az", "Vacation"))
    foldertags.toggle_folder_tag(album, "Az")
    flush()
    assert not os.path.exists(_mirror_dir("Az", "Vacation"))


# -- link mirror ---------------------------------------------------------------
def test_tagging_symlinks_album(album, flush):
    favorites.set_link_mode("symlink")
    foldertags.toggle_folder_tag(album, "Bp")
    flush()
    mirror = _mirror_dir("Bp", "Vacation")
    assert os.path.islink(mirror)
    assert os.path.realpath(mirror) == os.path.realpath(album)
    # Removing the link must not touch the original album.
    foldertags.toggle_folder_tag(album, "Bp")
    flush()
    assert not os.path.lexists(mirror)
    assert os.path.isdir(album)
    favorites.set_link_mode("copy")


def test_copy_ignores_nested_favorites(album, flush):
    # An album may itself contain a per-file Favorites mirror; never copy it.
    os.mkdir(os.path.join(album, "Favorites"))
    Image.new("RGB", (8, 8)).save(os.path.join(album, "Favorites", "x.jpg"))
    favorites.set_link_mode("copy")
    foldertags.toggle_folder_tag(album, "Az")
    flush()
    mirror = _mirror_dir("Az", "Vacation")
    assert "Favorites" not in os.listdir(mirror)


# -- collisions ----------------------------------------------------------------
def test_same_basename_albums_dont_collide(tmp_path, flush):
    favorites.set_link_mode("copy")
    a1 = tmp_path / "x" / "Set"
    a2 = tmp_path / "y" / "Set"
    for d, col in ((a1, (1, 0, 0)), (a2, (0, 0, 1))):
        d.mkdir(parents=True)
        Image.new("RGB", (8, 8), col).save(d / "p.jpg")
    foldertags.toggle_folder_tag(str(a1), "T")
    flush()
    foldertags.toggle_folder_tag(str(a2), "T")
    flush()
    tagdir = os.path.join(foldertags.folder_tags_root(), "T")
    subs = sorted(os.listdir(tagdir))
    assert subs == ["Set", "Set_1"]              # second album got a unique name


# -- guard ---------------------------------------------------------------------
def test_folder_inside_favorites_not_mirrored(tmp_path, flush, monkeypatch):
    inside = os.path.join(config.FAVORITES_DIR, "folder tags", "Az", "Nested")
    os.makedirs(inside)
    Image.new("RGB", (8, 8)).save(os.path.join(inside, "n.jpg"))
    # Assigning a tag is recorded but no mirror is made (would recurse).
    foldertags.toggle_folder_tag(inside, "Bp")
    flush()
    assert not os.path.exists(_mirror_dir("Bp", "Nested"))


# -- tag lifecycle -------------------------------------------------------------
def test_remove_tag_cleans_folder_mirror(album, flush):
    favorites.set_link_mode("copy")
    foldertags.toggle_folder_tag(album, "Az")
    flush()
    assert os.path.isdir(_mirror_dir("Az", "Vacation"))
    tags.remove_tag("Az")
    flush()
    assert not os.path.exists(os.path.join(foldertags.folder_tags_root(), "Az"))
    assert "Az" not in foldertags.tags_for(album)


def test_rename_tag_migrates_folder_mirror(album, flush):
    favorites.set_link_mode("copy")
    foldertags.toggle_folder_tag(album, "Az")
    flush()
    tags.rename_tag("Az", "Zephyr")
    flush()
    assert os.path.isdir(_mirror_dir("Zephyr", "Vacation"))
    assert not os.path.exists(os.path.join(foldertags.folder_tags_root(), "Az"))
    assert foldertags.tags_for(album) == ["Zephyr"]
