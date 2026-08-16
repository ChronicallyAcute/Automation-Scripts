"""Lightbox slideshow: auto-advance images/GIFs on a timer, wrap, and stop."""
from __future__ import annotations

from PIL import Image

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.lightbox import Lightbox
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


def _mk_lb(tmp_path, n=3):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"i{i}.jpg") for i in range(n)]
    for p in ps:
        Image.new("RGB", (16, 16)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (16, 16) for p in ps})
    return Lightbox(m, Favorites()), ps


def test_toggle_arms_timer_on_image(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(0)
    assert not lb._slide_timer.isActive()
    lb._slide_btn.setChecked(True)
    lb._toggle_slideshow()
    assert lb._slideshow_on
    assert lb._slide_timer.isActive()
    lb.close()


def test_advance_moves_and_wraps(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(2)
    lb._slide_btn.setChecked(True)
    lb._toggle_slideshow()
    lb._slide_advance()
    assert lb._row == 0                      # 2 → wrap → 0
    lb.close()


def test_toggle_off_stops_timer(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(0)
    lb._slide_btn.setChecked(True)
    lb._toggle_slideshow()
    assert lb._slide_timer.isActive()
    lb._slide_btn.setChecked(False)
    lb._toggle_slideshow()
    assert not lb._slideshow_on
    assert not lb._slide_timer.isActive()
    lb.close()


def test_interval_persists_and_reloads(qapp, tmp_path):
    from gallery_py_qt.engine import prefs
    lb, _ = _mk_lb(tmp_path)
    assert lb._slide_ms == 4000                  # default
    lb._set_slide_ms(10_000)
    assert prefs.load_prefs()["slideshow_ms"] == 10_000
    lb.close()
    lb2, _ = _mk_lb(tmp_path)                    # a fresh viewer picks it up
    assert lb2._slide_ms == 10_000
    lb2.close()


def test_interval_clamped(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb._set_slide_ms(1)                          # below the floor
    assert lb._slide_ms == lb._SLIDE_MIN_MS
    lb._set_slide_ms(10**9)                      # above the ceiling
    assert lb._slide_ms == lb._SLIDE_MAX_MS
    lb.close()


def test_changing_interval_rearms_running_show(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(0)
    lb._slide_btn.setChecked(True)
    lb._toggle_slideshow()
    lb._set_slide_ms(9000)
    assert lb._slide_timer.isActive()
    assert lb._slide_timer.interval() == 9000
    lb.close()


def test_hold_blocks_advance(qapp, tmp_path):
    lb, _ = _mk_lb(tmp_path)
    lb.show_row(1)
    lb._slideshow_on = True
    lb._hold.setChecked(True)
    lb._slide_advance()
    assert lb._row == 1                      # held → no advance
    lb.close()
