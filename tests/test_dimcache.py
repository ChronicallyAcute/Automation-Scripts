"""Dimensions are probed once and reused across sessions."""
from __future__ import annotations
import os
import time

from PIL import Image

from gallery_py_qt.engine import dimcache


def _img(tmp_path, name="a.jpg", size=(40, 20)):
    p = str(tmp_path / name)
    Image.new("RGB", size).save(p)
    return p


def test_put_then_get_roundtrip(tmp_path):
    p = _img(tmp_path)
    assert dimcache.get(p) is None
    dimcache.put(p, 40, 20)
    assert dimcache.get(p) == (40, 20)


def test_survives_a_reload_from_disk(tmp_path):
    p = _img(tmp_path)
    dimcache.put(p, 40, 20)
    dimcache.flush()
    dimcache._store = None                  # simulate a restart
    assert dimcache.get(p) == (40, 20)


def test_modifying_the_file_invalidates_the_entry(tmp_path):
    p = _img(tmp_path)
    dimcache.put(p, 40, 20)
    assert dimcache.get(p) == (40, 20)
    time.sleep(0.01)
    Image.new("RGB", (80, 60)).save(p)      # replaced: size + mtime change
    assert dimcache.get(p) is None          # re-probed rather than stale


def test_unknown_dimensions_are_not_cached(tmp_path):
    """A failed probe must be retried later, not pinned as 0x0 forever."""
    p = _img(tmp_path)
    dimcache.put(p, 0, 0)
    assert dimcache.get(p) is None


def test_missing_file_yields_no_hit(tmp_path):
    p = _img(tmp_path)
    dimcache.put(p, 40, 20)
    os.remove(p)
    assert dimcache.get(p) is None


def test_unreadable_cache_never_overwrites(tmp_path):
    with open(dimcache._FILE, "w", encoding="utf-8") as f:
        f.write("not json")
    before = open(dimcache._FILE, encoding="utf-8").read()
    dimcache._store = None
    assert dimcache._load() == {}
    dimcache.put(_img(tmp_path), 40, 20)
    dimcache.flush()
    assert open(dimcache._FILE, encoding="utf-8").read() == before


def test_dims_job_uses_the_cache(qapp, tmp_path, monkeypatch):
    """A cache hit must skip the (expensive) media probe entirely."""
    from gallery_py_qt.main_window import _DimsJob, _DimsSignals
    from gallery_py_qt.engine import media

    p = _img(tmp_path)
    dimcache.put(p, 40, 20)
    calls = []
    monkeypatch.setattr(media, "peek_size",
                        lambda path: calls.append(path) or (1, 1))
    got = {}
    sig = _DimsSignals()
    sig.done.connect(lambda d: got.update(d))
    _DimsJob([p], sig).run()
    assert got == {p: (40, 20)}
    assert calls == []                      # never touched the decoder


def test_dims_job_populates_the_cache(qapp, tmp_path):
    from gallery_py_qt.main_window import _DimsJob, _DimsSignals
    p = _img(tmp_path, size=(64, 32))
    sig = _DimsSignals()
    sig.done.connect(lambda d: None)
    _DimsJob([p], sig).run()
    assert dimcache.get(p) == (64, 32)      # next session gets it free
