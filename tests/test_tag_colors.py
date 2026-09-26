"""Tag colour-coding: defaults, assignment, persistence, and chip styling."""
from __future__ import annotations

from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


def test_known_tags_get_stable_palette_defaults():
    a, b = tags.get_tags()[0], tags.get_tags()[1]
    assert tags.color_of(a) and tags.color_of(b)
    assert tags.color_of(a) != tags.color_of(b)      # distinct
    assert tags.color_of(a) == tags.color_of(a)      # stable
    assert tags.color_of("not-a-tag") is None


def test_set_and_clear_colour():
    t = tags.get_tags()[0]
    default = tags.color_of(t)
    assert tags.set_color(t, "#AB12CD")
    assert tags.color_of(t) == "#ab12cd"             # normalised to lowercase
    assert tags.set_color(t, None)
    assert tags.color_of(t) == default               # back to the palette default


def test_invalid_colour_rejected():
    t = tags.get_tags()[0]
    assert not tags.set_color(t, "red")
    assert not tags.set_color(t, "#12345")


def test_colour_persists_and_follows_rename():
    t = tags.get_tags()[0]
    tags.set_color(t, "#010203")
    tags._colors = None                              # force a re-read from disk
    assert tags.color_of(t) == "#010203"
    assert tags.rename_tag(t, "Renamed")
    assert tags.color_of("Renamed") == "#010203"


def test_removing_a_tag_drops_its_colour():
    t = tags.get_tags()[0]
    tags.set_color(t, "#040506")
    tags.remove_tag(t)
    assert t not in tags.get_tags()
    assert tags._load_colors().get(t) is None


def test_chip_uses_the_tag_colour(qapp, tmp_path):
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (8, 8)).save(p)
    t = tags.get_tags()[0]
    tags.set_color(t, "#c0ffee")
    s = _Slot(0, Favorites())
    s.show_item(0, p)
    tags.toggle_tag(p, t)
    s._refresh_tag_styles()
    assert "#c0ffee" in s._tag_btns[t].styleSheet()
