"""Tags stay findable when the same file arrives spelled a different way.

The real-world failure: the JSON store is valid and accurate, but Qt's file
model hands us "C:/Users/..." while the entry was written as "C:\\Users\\...".
An exact dict lookup missed, so every tag looked deleted.
"""
from __future__ import annotations
import json
import os

from gallery_py_qt.engine import ratings, tags


def _seed_store(mapping):
    with open(tags._TAGS_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f)
    tags.reload()


def test_trailing_and_dotted_segments_resolve(make_image):
    a = make_image("a.jpg")
    tags.toggle_tag(a, "Az")
    d, base = os.path.dirname(a), os.path.basename(a)
    # Same file, spelled with a redundant "." segment.
    assert tags.tags_for(os.path.join(d, ".", base)) == ["Az"]
    # …and with a doubled separator.
    assert tags.tags_for(d + os.sep + os.sep + base) == ["Az"]


def test_forward_slash_spelling_resolves(make_image):
    """Qt returns forward slashes; the store may hold native separators."""
    a = make_image("a.jpg")
    tags.toggle_tag(a, "Bp")
    assert tags.tags_for(a.replace(os.sep, "/")) == ["Bp"]


def test_writing_via_another_spelling_updates_the_same_entry(make_image):
    a = make_image("a.jpg")
    _seed_store({a: ["Az"]})
    alt = os.path.join(os.path.dirname(a), ".", os.path.basename(a))
    tags.toggle_tag(alt, "Bp")                 # add through the other spelling
    store = tags._load()
    assert len(store) == 1                     # no duplicate entry created
    assert set(store[a]) == {"Az", "Bp"}       # original key kept


def test_untagged_file_still_reports_nothing(make_image):
    a = make_image("a.jpg")
    b = make_image("b.jpg")
    tags.toggle_tag(a, "Az")
    assert tags.tags_for(b) == []              # no false positives


def test_removing_the_last_tag_drops_the_entry(make_image):
    a = make_image("a.jpg")
    tags.toggle_tag(a, "Az")
    alt = os.path.join(os.path.dirname(a), ".", os.path.basename(a))
    tags.toggle_tag(alt, "Az")                 # untag via the other spelling
    assert tags.tags_for(a) == []
    assert a not in tags._load()


def test_ratings_resolve_across_spellings(make_image):
    a = make_image("a.jpg")
    ratings.set_rating(a, 4)
    alt = os.path.join(os.path.dirname(a), ".", os.path.basename(a))
    assert ratings.rating_of(alt) == 4
    ratings.set_rating(alt, 2)                 # write through the other spelling
    assert len(ratings._load()) == 1
    assert ratings.rating_of(a) == 2


def test_diagnose_reports_normalised_matches(make_image):
    from gallery_py_qt.engine import tagmigrate
    a = make_image("a.jpg")
    b = make_image("b.jpg")
    _seed_store({a: ["Az"]})
    alt = os.path.join(os.path.dirname(a), ".", os.path.basename(a))
    rep = tagmigrate.diagnose([alt, b])
    assert rep["normalised_only"] == 1         # a matched only after normalising
    assert rep["untagged"] == 1                # b genuinely has none


def test_diagnose_flags_tags_without_buttons(make_image):
    from gallery_py_qt.engine import tagmigrate
    a = make_image("a.jpg")
    _seed_store({a: ["NotInTagSet"]})
    rep = tagmigrate.diagnose([a])
    assert rep["exact"] == 1
    assert rep["tags_without_buttons"] == ["NotInTagSet"]
