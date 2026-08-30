"""Lazy GL viewport, and the opt-in playback stall diagnostic."""
from __future__ import annotations
import os

from PIL import Image

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


def _img(tmp_path, name="a.jpg"):
    p = str(tmp_path / name)
    Image.new("RGB", (16, 16)).save(p)
    return p


def _vid(tmp_path, name="c.mp4"):
    p = str(tmp_path / name)
    open(p, "wb").write(b"\x00" * 64)
    return p


def test_image_only_slot_creates_no_gl_context(qapp, tmp_path):
    s = _Slot(0, Favorites())
    s.show_item(0, _img(tmp_path))
    assert s._gl_viewport is False        # stills never needed one
    s.deleteLater()


def test_gl_is_installed_on_first_video(qapp, tmp_path, monkeypatch):
    monkeypatch.delenv("GALLERY_NO_GL", raising=False)
    s = _Slot(0, Favorites())
    installed = []
    monkeypatch.setattr(s._gview, "setViewport",
                        lambda w: installed.append(w))
    s._ensure_gl_viewport()
    # Either it installed one, or the GL module is unavailable here — both are
    # valid; what must never happen is a crash or a repeated attempt.
    assert s._gl_viewport == bool(installed)
    before = len(installed)
    s._ensure_gl_viewport()               # idempotent
    if s._gl_viewport:
        assert len(installed) == before
    s.deleteLater()


def test_no_gl_env_keeps_the_raster_path(qapp, tmp_path, monkeypatch):
    monkeypatch.setenv("GALLERY_NO_GL", "1")
    s = _Slot(0, Favorites())
    s._ensure_gl_viewport()
    assert s._gl_viewport is False
    s.deleteLater()


def test_diagnostic_is_off_unless_requested(qapp, monkeypatch):
    monkeypatch.delenv("GALLERY_DIAG", raising=False)
    s = _Slot(0, Favorites())
    assert s._diag is False
    s.deleteLater()


def test_diagnostic_reports_a_stall(qapp, tmp_path, monkeypatch):
    """Frame gaps go to the shared diagnostic log, not stderr."""
    from gallery_py_qt import diag
    monkeypatch.setenv("GALLERY_DIAG", "1")
    monkeypatch.setattr(diag, "LOG_PATH", str(tmp_path / "diag.log"))
    s = _Slot(0, Favorites())
    s._is_video = True
    s._path = _vid(tmp_path)
    s._diag = True
    s._diag_last = 0.0
    s._on_diag_frame(None)                # first frame: establishes a baseline
    import time
    s._diag_last = time.perf_counter() - 0.5    # pretend 500ms passed
    s._on_diag_frame(None)
    text = open(diag.LOG_PATH, encoding="utf-8").read()
    assert "video gap" in text and "media" in text
    s.deleteLater()


def test_diagnostic_ignores_normal_spacing(qapp, tmp_path, monkeypatch):
    from gallery_py_qt import diag
    monkeypatch.setenv("GALLERY_DIAG", "1")
    monkeypatch.setattr(diag, "LOG_PATH", str(tmp_path / "diag.log"))
    s = _Slot(0, Favorites())
    s._is_video = True
    s._path = _vid(tmp_path)
    s._diag = True
    import time
    s._diag_last = time.perf_counter() - 0.03   # 30ms: healthy
    s._on_diag_frame(None)
    assert not os.path.exists(diag.LOG_PATH) or \
        "video gap" not in open(diag.LOG_PATH, encoding="utf-8").read()
    s.deleteLater()
