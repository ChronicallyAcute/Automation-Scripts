"""Shared fixtures for the gallery_py_qt test suite.

Runs headless (QT_QPA_PLATFORM=offscreen).  Every persistent location and
module-level cache used by the app is redirected into a per-test temp dir so
tests never touch the real user profile and never leak state into each other.
"""
from __future__ import annotations
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
# Force the multi-view raster viewport instead of QOpenGLWidget: creating many
# GL contexts across the suite intermittently stalls under offscreen.
os.environ.setdefault("GALLERY_NO_GL", "1")

import pytest
from PIL import Image, ImageSequence  # noqa: F401  (re-exported for tests)


# -- Qt application (one per process) -----------------------------------------
@pytest.fixture(scope="session")
def qapp():
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication([])
    yield app


# -- Isolate all persistent state ---------------------------------------------
@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    """Redirect every on-disk location + reset in-memory caches per test."""
    from gallery_py_qt import config
    from gallery_py_qt.engine import favorites, tags, ratings

    monkeypatch.setattr(config, "FAVORITES_DIR", str(tmp_path / "GalleryFavorites"))
    monkeypatch.setattr(config, "FAVS_FILE", str(tmp_path / "favs.json"))
    monkeypatch.setattr(config, "TRASH_DIR", str(tmp_path / "trash"))
    monkeypatch.setattr(config, "CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setattr(config, "PREFS_FILE", str(tmp_path / "prefs.json"))
    monkeypatch.setattr(config, "RECENT_FILE", str(tmp_path / "recent.json"))

    monkeypatch.setattr(favorites, "_MANIFEST", str(tmp_path / "trash_manifest.json"))
    monkeypatch.setattr(tags, "_TAGS_FILE", str(tmp_path / "tags.json"))
    monkeypatch.setattr(tags, "_TAGDB_FILE", str(tmp_path / "tagfolders.json"))
    monkeypatch.setattr(tags, "_TAGSET_FILE", str(tmp_path / "tagset.json"))
    monkeypatch.setattr(tags, "_store", None)
    monkeypatch.setattr(tags, "TAGS", tags.DEFAULT_TAGS)
    monkeypatch.setattr(ratings, "_RATINGS_FILE", str(tmp_path / "ratings.json"))
    monkeypatch.setattr(ratings, "_store", None)
    tags._PENDING.clear()
    tags._retries.clear()
    yield
    # Drain the shared single-worker mirror pool so background copies/embeds
    # submitted by this test can't bleed into (and slow down) the next one.
    favorites.flush_mirror_ops(timeout=15)
    # Destroy any widgets the test created so their media pipelines / thread
    # pools are freed — otherwise QMediaPlayers accumulate across the suite and
    # eventually stall Qt's media init under the offscreen platform.
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    if app is not None:
        for w in list(app.topLevelWidgets()):
            w.close()
            w.deleteLater()
        app.processEvents()
        app.processEvents()


@pytest.fixture
def flush():
    """Block until the favourites/tags background mirror worker drains."""
    from gallery_py_qt.engine import favorites

    def _flush():
        favorites.flush_mirror_ops()
    return _flush


# -- Media factories ----------------------------------------------------------
@pytest.fixture
def make_image(tmp_path):
    def _make(name="img.jpg", size=(640, 480), color=(200, 60, 60)):
        p = tmp_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", size, color).save(p)
        return str(p)
    return _make


@pytest.fixture
def make_gif(tmp_path):
    def _make(name="anim.gif", size=(64, 48), frames=4,
              durations=(80, 90, 100, 110), loop=0):
        p = tmp_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        fs = [Image.new("RGB", size, (i * 60 % 256, 40, 200 - i * 30))
              .convert("P", palette=Image.ADAPTIVE) for i in range(frames)]
        fs[0].save(p, save_all=True, append_images=fs[1:],
                   duration=list(durations)[:frames], loop=loop, disposal=2)
        return str(p)
    return _make


@pytest.fixture
def gif_frames():
    """Return (n_frames, [durations], loop) for a GIF path."""
    def _sig(path):
        im = Image.open(path)
        durs = [f.info.get("duration")
                for f in ImageSequence.Iterator(Image.open(path))]
        return im.n_frames, durs, Image.open(path).info.get("loop")
    return _sig
