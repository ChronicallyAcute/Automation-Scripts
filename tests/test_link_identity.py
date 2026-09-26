"""Media reached through a tag/favourite link is the same media."""
from __future__ import annotations
import os
import time

import pytest
from PIL import Image

from gallery_py_qt.engine import ratings, tags


def _img(path, size=(20, 20)):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", size).save(path)
    return str(path)


def _linked(tmp_path):
    """(original, symlink_to_it) or skip."""
    original = _img(str(tmp_path / "media" / "a.png"))
    folder = tmp_path / "X" / "Tag Folders" / "T - Face"
    folder.mkdir(parents=True)
    link = str(folder / "a.png")
    try:
        os.symlink(original, link)
    except (OSError, NotImplementedError):
        pytest.skip("no symlink privilege")
    return original, link


# -- tags ----------------------------------------------------------------------

def test_tags_show_through_a_link(tmp_path):
    """Browsing a tag folder must not present every file as untagged."""
    original, link = _linked(tmp_path)
    tags.set_tags(["T - Face"])
    tags.toggle_tag(original, "T - Face")
    assert tags.tags_for(link) == ["T - Face"]


def test_tagging_through_a_link_updates_the_original(tmp_path):
    original, link = _linked(tmp_path)
    tags.set_tags(["T - Face", "T - Landscape"])
    tags.toggle_tag(original, "T - Face")
    tags.toggle_tag(link, "T - Landscape")
    assert sorted(tags.tags_for(original)) == ["T - Face", "T - Landscape"]


def test_a_link_does_not_create_a_second_entry(tmp_path):
    original, link = _linked(tmp_path)
    tags.set_tags(["T - Face"])
    tags.toggle_tag(link, "T - Face")
    assert list(tags._load()) == [original]


def test_untagging_through_a_link_clears_the_original(tmp_path):
    original, link = _linked(tmp_path)
    tags.set_tags(["T - Face"])
    tags.toggle_tag(original, "T - Face")
    tags.toggle_tag(link, "T - Face")
    assert tags.tags_for(original) == []


def test_an_ordinary_path_is_untouched(tmp_path):
    p = _img(str(tmp_path / "media" / "a.png"))
    tags.set_tags(["T - Face"])
    tags.toggle_tag(p, "T - Face")
    assert tags.store_key(p) == p


def test_a_broken_link_does_not_raise(tmp_path):
    original, link = _linked(tmp_path)
    os.remove(original)
    assert tags.tags_for(link) == []


# -- ratings -------------------------------------------------------------------

def test_ratings_show_through_a_link(tmp_path):
    original, link = _linked(tmp_path)
    ratings.set_rating(original, 4)
    assert ratings.rating_of(link) == 4


def test_rating_through_a_link_updates_the_original(tmp_path):
    original, link = _linked(tmp_path)
    ratings.set_rating(link, 3)
    assert ratings.rating_of(original) == 3
    assert list(ratings._load()) == [original]


# -- the grid shows the chips for linked media ---------------------------------

def test_a_tile_opened_from_a_tag_folder_shows_its_chips(qapp, tmp_path):
    from gallery_py_qt.engine.favorites import Favorites
    from gallery_py_qt.multiview import _Slot
    original, link = _linked(tmp_path)
    tags.set_tags(["T - Face", "T - Landscape"])
    tags.toggle_tag(original, "T - Face")
    s = _Slot(0, Favorites())
    s.show_item(0, link)                   # opened via the tag folder
    s._refresh_tag_styles()
    qapp.processEvents()
    shown = [t for t, b in s._tag_btns.items() if not b.isHidden()]
    assert shown == ["T - Face"], "the chip for the tag it carries must show"
    assert not s._tagbar.isHidden()
    s.deleteLater()
