"""Portable tags + ratings sidecar: export, round-trip, merge/overwrite."""
from __future__ import annotations
import json
import os
import shutil

from PIL import Image

from gallery_py_qt.engine import tags, ratings, sidecar


def _img(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (8, 8)).save(path)
    return path


def test_export_uses_relative_keys_and_scopes_to_root(tmp_path):
    root = tmp_path / "lib"
    a = _img(str(root / "sub" / "a.jpg"))
    outside = _img(str(tmp_path / "other" / "z.jpg"))
    tags.toggle_tag(a, "Az")
    ratings.set_rating(a, 4)
    tags.toggle_tag(outside, "Bp")           # not under root → excluded

    dest = str(tmp_path / "meta.json")
    n = sidecar.export_metadata(str(root), dest)
    assert n == 1
    data = json.loads(open(dest, encoding="utf-8").read())
    assert data["schema"] == 1
    assert data["items"] == {"sub/a.jpg": {"tags": ["Az"], "rating": 4}}


def test_roundtrip_after_moving_the_library(tmp_path):
    root = tmp_path / "lib"
    a = _img(str(root / "a.jpg"))
    b = str(root / "d" / "b.mp4")             # any file; extension irrelevant
    os.makedirs(os.path.dirname(b), exist_ok=True)
    open(b, "wb").write(b"\x00")
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(a, "Bp")
    ratings.set_rating(b, 5)
    dest = str(tmp_path / "meta.json")
    sidecar.export_metadata(str(root), dest)

    # Simulate a relocated library + a fresh install (empty stores).
    new_root = tmp_path / "moved"
    shutil.copytree(str(root), str(new_root))
    tags._store = {}
    ratings._store = {}

    updated, missing = sidecar.import_metadata(str(new_root), dest)
    assert (updated, missing) == (2, 0)
    assert set(tags.tags_for(str(new_root / "a.jpg"))) == {"Az", "Bp"}
    assert ratings.rating_of(str(new_root / "d" / "b.mp4")) == 5


def test_import_merges_by_default_and_overwrites_on_request(tmp_path):
    root = tmp_path / "lib"
    a = _img(str(root / "a.jpg"))
    tags.toggle_tag(a, "Az")
    ratings.set_rating(a, 3)
    dest = str(tmp_path / "meta.json")
    sidecar.export_metadata(str(root), dest)

    # Change the live state, then re-import.
    tags.set_tags_for(a, ["HT"])
    ratings.set_rating(a, 1)

    sidecar.import_metadata(str(root), dest)                 # merge
    assert set(tags.tags_for(a)) == {"HT", "Az"}             # union
    assert ratings.rating_of(a) == 1                         # existing kept

    sidecar.import_metadata(str(root), dest, overwrite=True)
    assert tags.tags_for(a) == ["Az"]                        # replaced
    assert ratings.rating_of(a) == 3                         # replaced


def test_missing_files_are_counted_not_applied(tmp_path):
    root = tmp_path / "lib"
    a = _img(str(root / "a.jpg"))
    tags.toggle_tag(a, "Az")
    dest = str(tmp_path / "meta.json")
    sidecar.export_metadata(str(root), dest)
    os.remove(a)                                             # file gone
    tags._store = {}
    updated, missing = sidecar.import_metadata(str(root), dest)
    assert (updated, missing) == (0, 1)
