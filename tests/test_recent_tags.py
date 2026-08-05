"""Repeat-last-tags: a ditto button re-applies the most recent tag set."""
from __future__ import annotations

from PIL import Image

from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


def _img(tmp_path, name):
    p = str(tmp_path / name)
    Image.new("RGB", (8, 8)).save(p)
    return p


# -- engine --------------------------------------------------------------------
def test_recent_tracks_last_applied(make_image):
    a = make_image("a.jpg")
    assert tags.recent_tags() == []
    tags.toggle_tag(a, "T")
    tags.toggle_tag(a, "BT")
    assert tags.recent_tags() == ["T", "BT"]


def test_recent_survives_untag_of_another_file(make_image):
    a = make_image("a.jpg")
    tags.toggle_tag(a, "T")
    tags.toggle_tag(a, "BT")
    b = make_image("b.jpg")
    tags.toggle_tag(b, "HT")            # b now [HT] → recent updates
    assert tags.recent_tags() == ["HT"]
    tags.toggle_tag(b, "HT")            # emptying b must NOT wipe the memory
    assert tags.recent_tags() == ["HT"]


def test_apply_recent_adds_missing_only(make_image):
    b = make_image("b.jpg")
    tags.toggle_tag(b, "T")            # b already has T
    a = make_image("a.jpg")            # …then establish recent = [T, BT]
    tags.toggle_tag(a, "T")
    tags.toggle_tag(a, "BT")
    added = tags.apply_recent(b)
    assert added == 1                  # only BT added (T already present)
    assert set(tags.tags_for(b)) == {"T", "BT"}


# -- slot ditto button ---------------------------------------------------------
def test_slot_repeat_button_applies_recent(qapp, tmp_path):
    a, b = _img(tmp_path, "a.jpg"), _img(tmp_path, "b.jpg")
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(a, "Bp")
    s = _Slot(0, Favorites())
    s.show_item(0, b)
    assert s._repeat_btn.isEnabled()               # b lacks the recent tags
    s._repeat_btn.click()
    assert set(tags.tags_for(b)) == {"Az", "Bp"}
    assert not s._repeat_btn.isEnabled()           # nothing left to add


def test_slot_repeat_button_disabled_without_recent(qapp, tmp_path):
    b = _img(tmp_path, "b.jpg")
    s = _Slot(0, Favorites())
    s.show_item(0, b)
    assert not s._repeat_btn.isEnabled()           # no recent tags yet


def test_slot_repeat_button_disabled_when_already_tagged(qapp, tmp_path):
    a = _img(tmp_path, "a.jpg")
    tags.toggle_tag(a, "T")
    s = _Slot(0, Favorites())
    s.show_item(0, a)                              # a already has the recent tag
    assert not s._repeat_btn.isEnabled()
