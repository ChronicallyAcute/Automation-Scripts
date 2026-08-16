"""Coining a new tag inline from a multi-view tile."""
from __future__ import annotations

from PIL import Image
from PySide6 import QtWidgets

from gallery_py_qt.engine import tags
from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.multiview import _Slot


def _img(tmp_path, name):
    p = str(tmp_path / name)
    Image.new("RGB", (8, 8)).save(p)
    return p


def test_inline_tag_creates_applies_and_signals(qapp, tmp_path, monkeypatch):
    a = _img(tmp_path, "a.jpg")
    s = _Slot(0, Favorites())
    s.show_item(0, a)
    fired = []
    s.tagSetChanged.connect(lambda: fired.append(True))

    monkeypatch.setattr(QtWidgets.QInputDialog, "getText",
                        staticmethod(lambda *a, **k: ("Sunset", True)))
    s._create_tag_inline()

    assert "Sunset" in tags.get_tags()           # added to the global set
    assert "Sunset" in tags.tags_for(a)          # applied to this file
    assert fired == [True]                        # refresh signal emitted


def test_inline_tag_cancel_is_noop(qapp, tmp_path, monkeypatch):
    a = _img(tmp_path, "a.jpg")
    s = _Slot(0, Favorites())
    s.show_item(0, a)
    before = set(tags.get_tags())
    monkeypatch.setattr(QtWidgets.QInputDialog, "getText",
                        staticmethod(lambda *a, **k: ("", False)))
    s._create_tag_inline()
    assert set(tags.get_tags()) == before
    assert tags.tags_for(a) == []


def test_inline_existing_tag_applies_without_signal(qapp, tmp_path, monkeypatch):
    a = _img(tmp_path, "a.jpg")
    s = _Slot(0, Favorites())
    s.show_item(0, a)
    existing = tags.get_tags()[0]
    fired = []
    s.tagSetChanged.connect(lambda: fired.append(True))
    monkeypatch.setattr(QtWidgets.QInputDialog, "getText",
                        staticmethod(lambda *a, **k: (existing, True)))
    s._create_tag_inline()
    assert existing in tags.tags_for(a)          # applied
    assert fired == []                            # no set change → no rebuild
