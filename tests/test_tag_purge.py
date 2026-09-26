"""Erasing tag names that were never tags, and stopping them coming back."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine import tagmigrate, taglibrary, tags


def _img(path):
    os.makedirs(os.path.dirname(str(path)), exist_ok=True)
    Image.new("RGB", (9, 9)).save(str(path))
    return str(path)


# The names the log reported — media folders mistaken for tags.
BOGUS = ["Gallery Favorites", "Generative", "Newdl", "ygwbt", "Emily Lynne"]


@pytest.fixture
def lib(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "FAVORITES_DIR", str(tmp_path / "X"))
    os.makedirs(os.path.join(taglibrary.tag_folders_root(), "T - Face"))
    tags.set_tags(["T - Face"] + BOGUS)
    return tmp_path


def _tagged(lib, n=3):
    out = []
    for i in range(n):
        p = _img(lib / "media" / f"{i}.png")
        tags.toggle_tag(p, "T - Face")
        for b in BOGUS:
            tags.toggle_tag(p, b)
        out.append(p)
    return out


# -- spotting them ---------------------------------------------------------------

def test_a_tag_without_a_folder_is_flagged(lib):
    assert set(tags.tags_without_folders()) == set(BOGUS)


def test_a_tag_with_a_folder_is_not_flagged(lib):
    assert "T - Face" not in tags.tags_without_folders()


def test_folder_matching_ignores_case(lib, tmp_path):
    os.makedirs(os.path.join(taglibrary.tag_folders_root(), "newdl"))
    assert "Newdl" not in tags.tags_without_folders()


# -- erasing them -----------------------------------------------------------------

def test_purge_clears_the_tag_set(lib):
    _tagged(lib)
    rep = tags.purge_tags(BOGUS)
    assert rep["tags"] == len(BOGUS)
    assert tags.get_tags() == ("T - Face",)


def test_purge_clears_them_off_every_file(lib):
    paths = _tagged(lib)
    rep = tags.purge_tags(BOGUS)
    assert rep["files"] == len(paths)
    for p in paths:
        assert tags.tags_for(p) == ["T - Face"]


def test_purge_leaves_the_real_tag_alone(lib):
    paths = _tagged(lib)
    tags.purge_tags(BOGUS)
    assert all("T - Face" in tags.tags_for(p) for p in paths)


def test_purge_drops_the_store_entry_when_nothing_is_left(lib):
    p = _img(lib / "media" / "only.png")
    tags.toggle_tag(p, "Newdl")
    tags.purge_tags(["Newdl"])
    assert p not in tags._load()


def test_purge_drops_their_colours(lib):
    _tagged(lib)
    tags.set_color("Newdl", "#ff0000")
    rep = tags.purge_tags(BOGUS)
    assert rep["colors"] == 1
    assert tags.color_of("Newdl") in (None, "")


def test_purge_deletes_no_folder(lib):
    folder = os.path.join(config.FAVORITES_DIR, "Generative")
    os.makedirs(folder)
    _img(os.path.join(folder, "keep.png"))
    tags.purge_tags(BOGUS)
    assert os.path.isdir(folder) and os.listdir(folder) == ["keep.png"]


def test_purging_nothing_is_a_no_op(lib):
    _tagged(lib)
    before = list(tags.get_tags())
    assert tags.purge_tags([])["tags"] == 0
    assert list(tags.get_tags()) == before


def test_purge_refuses_when_the_store_is_unreadable(lib, monkeypatch):
    _tagged(lib)
    monkeypatch.setattr(tags, "_load_failed", True)
    assert tags.purge_tags(BOGUS)["files"] == 0


# -- and they stay gone ------------------------------------------------------------

def test_they_do_not_come_back_on_the_next_launch(lib):
    """adopt_tags_in_use() heals the set from the store on every launch, so a
    name left on one file returns — clearing the STORE is the part that
    matters."""
    _tagged(lib)
    tags.purge_tags(BOGUS)
    tags.TAGS = ("T - Face",)
    assert not (set(tags.adopt_tags_in_use()) & set(BOGUS))


def test_a_name_left_on_one_file_would_return(lib):
    """Guards the reasoning above: without the store pass they do come back."""
    paths = _tagged(lib)
    tags.set_tags(["T - Face"])              # set only, store untouched
    tags.TAGS = ("T - Face",)
    assert set(tags.adopt_tags_in_use()) & set(BOGUS)
    assert paths


# -- the source of the problem ------------------------------------------------------

def test_recovery_no_longer_reads_every_folder_as_a_tag(lib):
    """Listing every directory in the favourites folder is how media folder
    names became tags in the first place."""
    for name in ("Generative", "Newdl", "Gallery Favorites"):
        os.makedirs(os.path.join(config.FAVORITES_DIR, name), exist_ok=True)
        _img(os.path.join(config.FAVORITES_DIR, name, "shared.png"))
    _img(lib / "media" / "shared.png")
    found = tagmigrate.read_tag_folders()
    adopted = {t for names in found.values() for t in names}
    assert not (adopted & {"Generative", "Newdl", "Gallery Favorites"})


def test_recovery_still_reads_real_tag_folders(lib):
    original = _img(lib / "media" / "pic.png")
    tags.toggle_tag(original, "T - Face")
    os.makedirs(os.path.join(taglibrary.tag_folders_root(), "T - Face"),
                exist_ok=True)
    _img(os.path.join(taglibrary.tag_folders_root(), "T - Face", "pic.png"))
    found = tagmigrate.read_tag_folders()
    adopted = {t for names in found.values() for t in names}
    assert "T - Face" in adopted


def test_recovery_skips_the_reserved_folders(lib):
    os.makedirs(os.path.join(taglibrary.tag_folders_root(), "Favorites"),
                exist_ok=True)
    found = tagmigrate.read_tag_folders()
    adopted = {t for names in found.values() for t in names}
    assert "Favorites" not in adopted


def test_an_old_layout_folder_is_recovered_when_it_is_a_known_tag(lib):
    """An un-tidied library keeps its tag folders beside Tag Folders; those
    still recover, because the tag set vouches for the name."""
    original = _img(lib / "media" / "pic.png")
    tags.toggle_tag(original, "T - Face")
    old = os.path.join(config.FAVORITES_DIR, "T - Face")
    os.makedirs(old, exist_ok=True)
    _img(os.path.join(old, "pic.png"))
    found = tagmigrate.read_tag_folders()
    assert "T - Face" in found.get(original, [])


def test_an_old_layout_folder_is_ignored_when_it_is_not_a_tag(lib):
    original = _img(lib / "media" / "pic.png")
    tags.toggle_tag(original, "T - Face")
    tags.set_tags(["T - Face"])              # "Newdl" is no longer a tag
    junk = os.path.join(config.FAVORITES_DIR, "Newdl")
    os.makedirs(junk, exist_ok=True)
    _img(os.path.join(junk, "pic.png"))
    found = tagmigrate.read_tag_folders()
    assert "Newdl" not in found.get(original, [])
