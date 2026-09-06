"""Recovering tags from the historical storage mechanisms."""
from __future__ import annotations
import json
import os

from PIL import Image

from gallery_py_qt import config
from gallery_py_qt.engine import tagmigrate, tags


def _img(tmp_path, name="a.jpg"):
    p = str(tmp_path / name)
    Image.new("RGB", (16, 16)).save(p)
    return p


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


# -- 1. legacy / backup JSON stores -------------------------------------------
def test_backup_store_is_discovered_and_merged(tmp_path):
    a = _img(tmp_path)
    _write_json(tags._TAGS_FILE + ".bak", {a: ["Az", "Bp"]})
    assert tags._TAGS_FILE + ".bak" in tagmigrate.legacy_json_sources()
    rep = tagmigrate.recover()
    assert rep["tags"] == 2
    assert set(tags.tags_for(a)) == {"Az", "Bp"}


def test_merge_never_removes_existing_tags(tmp_path):
    a = _img(tmp_path)
    tags.toggle_tag(a, "HT")                       # already present
    _write_json(tags._TAGS_FILE + ".bak", {a: ["Az"]})
    tagmigrate.recover()
    assert set(tags.tags_for(a)) == {"HT", "Az"}   # union, nothing lost


def test_missing_files_are_not_repopulated(tmp_path):
    gone = str(tmp_path / "gone.jpg")
    _write_json(tags._TAGS_FILE + ".bak", {gone: ["Az"]})
    rep = tagmigrate.recover()
    assert rep["tags"] == 0
    assert tags.tags_for(gone) == []


def test_recovery_refuses_when_the_store_is_unreadable(tmp_path):
    a = _img(tmp_path)
    with open(tags._TAGS_FILE, "w", encoding="utf-8") as f:
        f.write("not json")
    tags._store = None
    assert tags.load_failed()
    _write_json(tags._TAGS_FILE + ".bak", {a: ["Az"]})
    assert tagmigrate.merge({a: ["Az"]}) == (0, 0)


def test_garbage_source_file_is_ignored(tmp_path):
    p = str(tmp_path / "junk.json")
    with open(p, "w", encoding="utf-8") as f:
        f.write("[[[")
    assert tagmigrate.read_json_store(p) == {}


# -- 2/3. tag folders ---------------------------------------------------------
def test_tag_folder_manifest_is_recovered(tmp_path, monkeypatch):
    a = _img(tmp_path)
    monkeypatch.setattr(tags, "_load_tagdb",
                        lambda: {a: {"Az": "/mirror/Az/a.jpg"}})
    rep = tagmigrate.recover()
    assert rep["tags"] == 1
    assert tags.tags_for(a) == ["Az"]


def test_tag_folders_on_disk_are_recovered(tmp_path, monkeypatch):
    a = _img(tmp_path)
    tags.toggle_tag(a, "HT")                       # so the basename is known
    favs = tmp_path / "Favs"
    (favs / "Bp").mkdir(parents=True)
    Image.new("RGB", (16, 16)).save(favs / "Bp" / "a.jpg")   # mirrored copy
    monkeypatch.setattr(config, "FAVORITES_DIR", str(favs))
    found = tagmigrate.read_tag_folders()
    assert "Bp" in found.get(a, [])


# -- 4. embedded metadata -----------------------------------------------------
def test_embedded_jpeg_keywords_round_trip(tmp_path):
    a = _img(tmp_path)
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(a, "Bp")
    from gallery_py_qt.engine import favorites
    favorites.flush_mirror_ops()                   # let the embed finish
    got = tagmigrate.read_embedded(a)
    if got:                                        # piexif is optional
        assert set(got) == {"Az", "Bp"}


def test_embedded_read_is_safe_on_junk(tmp_path):
    p = str(tmp_path / "broken.jpg")
    open(p, "wb").write(b"not an image")
    assert tagmigrate.read_embedded(p) == []
    assert tagmigrate.read_embedded(str(tmp_path / "nope.gif")) == []


# -- tag-set healing ----------------------------------------------------------
def test_adopt_re_adds_tags_that_files_still_carry(tmp_path):
    a = _img(tmp_path)
    tags.set_tags_for(a, ["Custom1", "Custom2"])
    tags.TAGS = tags.DEFAULT_TAGS                  # simulate a reset tag set
    assert "Custom1" not in tags.get_tags()
    recovered = tags.adopt_tags_in_use()
    assert recovered == ["Custom1", "Custom2"]
    assert "Custom1" in tags.get_tags()            # buttons come back


def test_adopt_is_a_noop_when_nothing_is_missing(tmp_path):
    a = _img(tmp_path)
    tags.toggle_tag(a, tags.get_tags()[0])
    assert tags.adopt_tags_in_use() == []
