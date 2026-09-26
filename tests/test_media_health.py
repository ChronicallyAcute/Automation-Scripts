"""Damaged-media detection and the persistent verdict store."""
from __future__ import annotations
import os

import pytest
from PIL import Image

from gallery_py_qt.engine import health, media


def _img(path):
    Image.new("RGB", (8, 8)).save(path)
    return str(path)


def _fake_video(path, data=b"\x00" * 64):
    with open(path, "wb") as f:
        f.write(data)
    return str(path)


# -- probe -------------------------------------------------------------------

def test_images_are_never_reported_damaged(tmp_path):
    ok, why = media.probe_integrity(_img(tmp_path / "a.jpg"))
    assert ok and why == ""


def test_garbage_video_is_damaged(tmp_path):
    if not media.HAS_CV2:
        pytest.skip("opencv unavailable")
    ok, why = media.probe_integrity(_fake_video(tmp_path / "v.mp4"))
    assert not ok and why


# -- store -------------------------------------------------------------------

def test_verdict_round_trips(tmp_path):
    v = _fake_video(tmp_path / "v.mp4")
    assert health.get(v) is None
    health.put(v, False, "truncated")
    assert health.get(v) == (False, "truncated")


def test_verdict_is_dropped_when_the_file_changes(tmp_path):
    v = _fake_video(tmp_path / "v.mp4")
    health.put(v, False, "truncated")
    _fake_video(tmp_path / "v.mp4", b"\x01" * 4096)   # the download finished
    assert health.get(v) is None, "a replaced file must be re-checked"


def test_verdict_survives_a_reload(tmp_path):
    v = _fake_video(tmp_path / "v.mp4")
    health.put(v, False, "truncated")
    health.flush()
    health._store = None                       # simulate a fresh launch
    assert health.get(v) == (False, "truncated")


def test_unknown_file_has_no_verdict(tmp_path):
    assert health.get(str(tmp_path / "gone.mp4")) is None


def test_forget_reopens_the_question(tmp_path):
    v = _fake_video(tmp_path / "v.mp4")
    health.put(v, False, "truncated")
    media._mark_bad_video(v)
    health.forget(v)
    assert health.get(v) is None
    assert not media.is_bad_video(v)


def test_clear_drops_everything(tmp_path):
    for i in range(3):
        health.put(_fake_video(tmp_path / f"v{i}.mp4"), False, "x")
    assert health.clear() == 3
    assert health.damaged() == []


# -- check -------------------------------------------------------------------

def test_check_records_and_blacklists(tmp_path):
    if not media.HAS_CV2:
        pytest.skip("opencv unavailable")
    v = _fake_video(tmp_path / "v.mp4")
    rep = health.check([v])
    assert rep["checked"] == 1
    assert [p for p, _ in rep["damaged"]] == [v]
    assert media.is_bad_video(v), "a damaged file must be skipped afterwards"


def test_check_skips_images(tmp_path):
    rep = health.check([_img(tmp_path / "a.jpg")])
    assert rep["checked"] == 0 and rep["damaged"] == []


def test_second_check_uses_the_remembered_verdict(tmp_path):
    v = _fake_video(tmp_path / "v.mp4")
    health.put(v, False, "truncated")
    calls = []

    def _boom(path):
        calls.append(path)
        return True, ""
    orig, media.probe_integrity = media.probe_integrity, _boom
    try:
        rep = health.check([v])
    finally:
        media.probe_integrity = orig
    assert calls == [], "a remembered verdict must not re-probe"
    assert rep["cached"] == 1 and len(rep["damaged"]) == 1


def test_recheck_forces_a_fresh_probe(tmp_path):
    v = _fake_video(tmp_path / "v.mp4")
    health.put(v, False, "truncated")
    orig, media.probe_integrity = media.probe_integrity, lambda p: (True, "")
    try:
        rep = health.check([v], recheck=True)
    finally:
        media.probe_integrity = orig
    assert rep["damaged"] == [] and rep["checked"] == 1


def test_progress_can_cancel(tmp_path):
    vids = [_fake_video(tmp_path / f"v{i}.mp4") for i in range(4)]
    seen = []

    def _tick(done, total, path):
        seen.append(done)
        return done < 2                       # cancel on the third
    rep = health.check(vids, progress=_tick)
    assert rep["cancelled"] and rep["checked"] + rep["cached"] == 2


# -- seeding -----------------------------------------------------------------

def test_seed_blacklist_skips_known_bad_before_anything_opens_them(tmp_path):
    good = _fake_video(tmp_path / "good.mp4")
    bad = _fake_video(tmp_path / "bad.mp4")
    health.put(good, True, "")
    health.put(bad, False, "truncated")
    assert health.seed_blacklist([good, bad]) == 1
    assert media.is_bad_video(bad)
    assert not media.is_bad_video(good)


def test_seed_ignores_stale_verdicts(tmp_path):
    bad = _fake_video(tmp_path / "bad.mp4")
    health.put(bad, False, "truncated")
    _fake_video(tmp_path / "bad.mp4", b"\x02" * 8192)     # repaired
    assert health.seed_blacklist([bad]) == 0
    assert not media.is_bad_video(bad)


def test_damaged_lists_only_damaged(tmp_path):
    good = _fake_video(tmp_path / "good.mp4")
    bad = _fake_video(tmp_path / "bad.mp4")
    health.put(good, True, "")
    health.put(bad, False, "truncated")
    assert health.damaged() == [(bad, "truncated")]


def test_unreadable_store_is_not_overwritten(tmp_path, monkeypatch):
    """Same rule as the tag store: never write over data we couldn't read."""
    with open(health._FILE, "w", encoding="utf-8") as f:
        f.write("{ not json")
    health._store = None
    health._load()
    assert health._load_failed
    health.put(_fake_video(tmp_path / "v.mp4"), False, "x")
    health.flush()
    with open(health._FILE, encoding="utf-8") as f:
        assert f.read() == "{ not json"


# -- wired into the app -------------------------------------------------------

def test_overflow_menu_offers_the_check(qapp, tmp_path):
    from gallery_py_qt.main_window import MainWindow
    win = MainWindow()
    labels = [a.text() for a in win._more_menu.actions()]
    assert any("media health" in t.lower() for t in labels)
    win.close()


def test_scan_batches_seed_the_blacklist(qapp, tmp_path):
    """Damaged files must be skipped before the loader ever opens them."""
    from gallery_py_qt.main_window import MainWindow
    v = _fake_video(tmp_path / "v.mp4")
    health.put(v, False, "truncated")
    win = MainWindow()
    win._on_scan_batch(win._scan_gen, [v])
    assert media.is_bad_video(v)
    win.close()
