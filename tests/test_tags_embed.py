"""Embedded-metadata writing: GIF XMP surgery, retry classification, JPEG EXIF."""
from __future__ import annotations
import os

import pytest
from PIL import Image, ImageSequence

from gallery_py_qt.engine import tags


# -- GIF XMP: animation must survive byte-splicing ----------------------------
def test_gif_embed_preserves_animation(make_gif, gif_frames):
    g = make_gif(frames=4)
    before = gif_frames(g)
    tags._embed_gif_xmp(g, ["BT", "WAM"])
    assert gif_frames(g) == before                 # frames, durations, loop
    assert sorted(tags.read_gif_tags(g)) == ["BT", "WAM"]


def test_gif_embed_preserves_loop_and_comment(make_gif):
    g = make_gif(frames=3, loop=0)
    raw0 = open(g, "rb").read()
    assert b"NETSCAPE2.0" in raw0                   # animated GIFs carry a loop ext
    tags._embed_gif_xmp(g, ["T"])
    assert b"NETSCAPE2.0" in open(g, "rb").read()   # loop preserved


def test_gif_embed_idempotent(make_gif):
    g = make_gif()
    tags._embed_gif_xmp(g, ["BT", "WAM", "T"])
    size1 = os.path.getsize(g)
    for _ in range(4):
        tags._embed_gif_xmp(g, ["BT", "WAM", "T"])
    assert os.path.getsize(g) == size1             # no growth
    assert open(g, "rb").read().count(b"XMP DataXMP") == 1


def test_gif_clear_removes_block(make_gif, gif_frames):
    g = make_gif()
    frames = gif_frames(g)
    tags._embed_gif_xmp(g, ["Az"])
    tags._embed_gif_xmp(g, [])
    assert tags.read_gif_tags(g) == []
    assert b"XMP DataXMP" not in open(g, "rb").read()
    assert gif_frames(g) == frames                 # still animated


def test_gif_lossless_pixels_through_roundtrip(make_gif):
    g = make_gif(frames=3)
    before = [f.convert("RGB").tobytes()
              for f in ImageSequence.Iterator(Image.open(g))]
    tags._embed_gif_xmp(g, ["Az"])
    tags._embed_gif_xmp(g, [])
    after = [f.convert("RGB").tobytes()
             for f in ImageSequence.Iterator(Image.open(g))]
    assert before == after


def test_gif_xml_special_chars_roundtrip(make_gif):
    g = make_gif()
    tags._embed_gif_xmp(g, ["A&B<C>", "WAM"])
    assert tags.read_gif_tags(g) == ["A&B<C>", "WAM"]   # escaped + unescaped
    assert b"&amp;" in open(g, "rb").read()


def test_gif87a_bumped_to_gif89a(tmp_path):
    g = str(tmp_path / "s.gif")
    Image.new("P", (20, 20)).save(g)
    with open(g, "r+b") as f:
        f.seek(0)
        f.write(b"GIF87a")                         # force 87a
    tags._embed_gif_xmp(g, ["T"])
    assert open(g, "rb").read()[:6] == b"GIF89a"


def test_corrupt_gif_left_untouched(tmp_path):
    bad = str(tmp_path / "bad.gif")
    open(bad, "wb").write(b"GIF89a" + b"\x00" * 8)
    before = open(bad, "rb").read()
    with pytest.raises(ValueError):
        tags._embed_gif_xmp(bad, ["T"])
    assert open(bad, "rb").read() == before


# -- Retry classification: no unbounded queue ---------------------------------
def test_permanent_failure_not_queued(tmp_path):
    bad = str(tmp_path / "bad.gif")
    open(bad, "wb").write(b"GIF89a" + b"\x00" * 8)   # ValueError every time
    for _ in range(10):
        tags._embed_tags(bad, ["T"])
    assert bad not in tags._PENDING


def test_transient_failure_queues_then_gives_up(make_gif, monkeypatch):
    g = make_gif()
    monkeypatch.setattr(tags, "_embed_gif_xmp",
                        lambda p, t: (_ for _ in ()).throw(PermissionError("busy")))
    tags._embed_tags(g, ["BT"])
    assert g in tags._PENDING and tags._retries[g] == 1
    for _ in range(tags._MAX_RETRIES + 2):
        for p, t in list(tags._PENDING.items()):
            tags._embed_tags(p, t)
    assert g not in tags._PENDING                   # dropped after the cap


def test_pending_is_size_bounded(make_gif, monkeypatch, tmp_path):
    monkeypatch.setattr(tags, "_embed_gif_xmp",
                        lambda p, t: (_ for _ in ()).throw(PermissionError("busy")))
    for i in range(tags._PENDING_CAP + 200):
        tags._embed_tags(str(tmp_path / f"v{i}.gif"), ["T"])
    assert len(tags._PENDING) <= tags._PENDING_CAP


# -- JPEG EXIF XPKeywords ------------------------------------------------------
def test_jpeg_xpkeywords_nul_terminated(make_image):
    piexif = pytest.importorskip("piexif")
    jp = make_image("p.jpg")
    tags._embed_jpeg(jp, ["BT", "WAM"])
    raw = bytes(piexif.load(jp)["0th"][piexif.ImageIFD.XPKeywords])
    assert raw.endswith(b"\x00\x00")
    assert raw.decode("utf-16-le").rstrip("\x00") == "BT;WAM"


def test_jpeg_empty_deletes_field(make_image):
    piexif = pytest.importorskip("piexif")
    jp = make_image("p.jpg")
    tags._embed_jpeg(jp, ["BT"])
    tags._embed_jpeg(jp, [])
    assert piexif.ImageIFD.XPKeywords not in piexif.load(jp)["0th"]
    assert Image.open(jp).size == (640, 480)        # not corrupted


def test_video_embed_noop_off_windows(tmp_path):
    v = str(tmp_path / "c.mp4")
    open(v, "wb").write(b"\x00" * 64)
    tags._embed_tags(v, ["Az"])                     # must not raise / queue
    assert v not in tags._PENDING
