"""An unreadable tag/rating file must never be mistaken for 'no tags'.

The failure this guards against: a transient read error (antivirus lock,
sharing violation, a file truncated by an earlier crash) left the in-memory
store empty, and the very next save serialised that emptiness over the real
file — silently destroying every tag.
"""
from __future__ import annotations
import json
import os

from gallery_py_qt.engine import ratings, tags


def _write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


# -- tags ---------------------------------------------------------------------
def test_missing_file_is_a_legitimate_empty_store(make_image):
    assert not os.path.exists(tags._TAGS_FILE)
    assert tags._load() == {}
    assert not tags.load_failed()          # absent != unreadable
    a = make_image("a.jpg")
    tags.toggle_tag(a, "Az")               # saving is allowed
    assert tags.tags_for(a) == ["Az"]


def test_corrupt_file_blocks_saving_and_preserves_it(make_image):
    _write(tags._TAGS_FILE, "{ this is not json")
    before = open(tags._TAGS_FILE, encoding="utf-8").read()
    tags._store = None                     # force a fresh load
    assert tags._load() == {}
    assert tags.load_failed()              # flagged, not silently empty

    a = make_image("a.jpg")
    tags.toggle_tag(a, "Az")               # would previously have wiped the file
    assert open(tags._TAGS_FILE, encoding="utf-8").read() == before


def test_recovery_after_the_file_is_repaired(make_image):
    a = make_image("a.jpg")
    _write(tags._TAGS_FILE, "garbage")
    tags._store = None
    assert tags.load_failed()
    # Repair the file, then retry.
    _write(tags._TAGS_FILE, json.dumps({a: ["Bp"]}))
    tags.reload()
    assert not tags.load_failed()
    assert tags.tags_for(a) == ["Bp"]
    tags.toggle_tag(a, "Az")               # saving works again
    assert set(tags.tags_for(a)) == {"Az", "Bp"}


def test_a_backup_is_kept_before_the_first_overwrite(make_image):
    a = make_image("a.jpg")
    _write(tags._TAGS_FILE, json.dumps({a: ["Bp"]}))
    tags.reload()
    tags.toggle_tag(a, "Az")
    bak = tags._TAGS_FILE + ".bak"
    assert os.path.exists(bak)
    assert json.load(open(bak, encoding="utf-8")) == {a: ["Bp"]}   # pre-change


def test_existing_tags_survive_a_normal_session(make_image):
    a, b = make_image("a.jpg"), make_image("b.jpg")
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(b, "Bp")
    tags.reload()                          # simulate a restart
    assert tags.tags_for(a) == ["Az"] and tags.tags_for(b) == ["Bp"]


# -- ratings ------------------------------------------------------------------
def test_corrupt_ratings_file_blocks_saving(make_image):
    _write(ratings._RATINGS_FILE, "nope")
    before = open(ratings._RATINGS_FILE, encoding="utf-8").read()
    ratings._store = None
    assert ratings.load_failed()
    ratings.set_rating(make_image("a.jpg"), 4)
    assert open(ratings._RATINGS_FILE, encoding="utf-8").read() == before


def test_missing_ratings_file_saves_normally(make_image):
    a = make_image("a.jpg")
    assert not ratings.load_failed()
    ratings.set_rating(a, 3)
    assert ratings.rating_of(a) == 3
