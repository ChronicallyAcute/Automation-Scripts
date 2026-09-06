"""Per-tag aggregation folders under the Gallery Favorites dir."""
from __future__ import annotations
import json
import os
from unittest import mock

from gallery_py_qt import config
from gallery_py_qt.engine import tags


def _tagpath(t, name):
    return os.path.join(config.FAVORITES_DIR, t, name)


def test_tagged_not_favorited_no_copy(make_image, flush):
    img = make_image("a.jpg")
    tags.toggle_tag(img, "BT")
    tags.sync_tag_folders(img, favored=False)
    flush()
    assert not os.path.exists(_tagpath("BT", "a.jpg"))


def test_tagged_and_favorited_copies(make_image, flush):
    img = make_image("a.jpg")
    tags.toggle_tag(img, "BT")
    tags.sync_tag_folders(img, favored=True)
    flush()
    assert os.path.exists(_tagpath("BT", "a.jpg"))


def test_untag_removes_despite_source_drift(make_image, flush):
    """Embedding mutates the source; removal must still work (manifest-based)."""
    img = make_image("a.jpg")
    tags.toggle_tag(img, "BT")
    tags.sync_tag_folders(img, True)
    tags.toggle_tag(img, "WAM")
    tags.sync_tag_folders(img, True)               # source re-embedded here
    flush()
    assert os.path.exists(_tagpath("BT", "a.jpg"))
    tags.toggle_tag(img, "BT")                      # remove BT
    tags.sync_tag_folders(img, True)
    flush()
    assert not os.path.exists(_tagpath("BT", "a.jpg"))
    assert os.path.exists(_tagpath("WAM", "a.jpg"))


def test_unfavorite_clears_all_tag_folders(make_image, flush):
    img = make_image("a.jpg")
    for t in ("BT", "WAM"):
        tags.toggle_tag(img, t)
    tags.sync_tag_folders(img, True)
    flush()
    tags.sync_tag_folders(img, False)
    flush()
    assert not os.path.exists(_tagpath("BT", "a.jpg"))
    assert not os.path.exists(_tagpath("WAM", "a.jpg"))
    assert json.load(open(tags._TAGDB_FILE)) == {}   # manifest emptied


def test_basename_collision_across_folders(tmp_path, flush):
    from PIL import Image
    a = str(tmp_path / "f1" / "a.jpg")
    b = str(tmp_path / "f2" / "a.jpg")
    for p, c in ((a, (200, 0, 0)), (b, (0, 200, 0))):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        Image.new("RGB", (48, 48), c).save(p)
    for p in (a, b):
        tags.toggle_tag(p, "Az")
        tags.sync_tag_folders(p, True)
    flush()
    assert len(os.listdir(_tagpath("Az", ""))) == 2   # a.jpg + a_1.jpg
    tags.sync_tag_folders(b, False)                    # remove one
    flush()
    assert len(os.listdir(_tagpath("Az", ""))) == 1    # other survives


def test_short_video_copied(make_image, flush, tmp_path):
    v = str(tmp_path / "short.mp4")
    open(v, "wb").write(b"\x00" * 64)
    with mock.patch.object(tags.media, "peek_duration", return_value=300.0):
        tags.toggle_tag(v, "Jz")
        tags.sync_tag_folders(v, True)
        flush()
    assert os.path.exists(_tagpath("Jz", "short.mp4"))


def test_long_video_excluded(tmp_path, flush):
    v = str(tmp_path / "long.mp4")
    open(v, "wb").write(b"\x00" * 64)
    with mock.patch.object(tags.media, "peek_duration", return_value=900.0):
        tags.toggle_tag(v, "Jz")
        tags.sync_tag_folders(v, True)
        flush()
    assert not os.path.exists(_tagpath("Jz", "long.mp4"))


def test_bp_tag_present():
    assert "Bp" in tags.TAGS
