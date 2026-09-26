"""First-run welcome / feature-guide screen."""
from __future__ import annotations

import pytest

from gallery_py_qt.welcome_dialog import WelcomeDialog, SECTIONS


# -- content -------------------------------------------------------------------
def test_sections_are_populated():
    assert len(SECTIONS) >= 4
    for title, features in SECTIONS:
        assert title.strip()
        assert features
        for name, desc in features:
            assert name.strip() and desc.strip()


def test_covers_the_headline_features():
    """The guide must actually mention the program's major capabilities."""
    blob = " ".join(
        f"{title} {name} {desc}"
        for title, feats in SECTIONS for name, desc in feats).lower()
    for keyword in ("open", "favourite", "tag", "rating", "search",
                    "duplicate", "multi-view", "trash", "album", "scrub"):
        assert keyword in blob, f"guide never mentions {keyword!r}"


def test_dialog_renders_every_feature(qapp):
    from PySide6.QtWidgets import QLabel
    dlg = WelcomeDialog(True)
    rendered = " ".join(lbl.text() for lbl in dlg.findChildren(QLabel))
    # Every section title and feature name/description must appear on screen.
    for title, feats in SECTIONS:
        assert title in rendered
        for name, desc in feats:
            assert name in rendered and desc in rendered
    dlg.done(0)


# -- startup preference --------------------------------------------------------
def test_startup_checkbox_reflects_and_reports(qapp):
    on = WelcomeDialog(True)
    assert on.show_at_startup() is True
    on.done(0)
    off = WelcomeDialog(False)
    assert off.show_at_startup() is False
    off._startup_cb.setChecked(True)
    assert off.show_at_startup() is True
    off.done(0)


# -- main-window integration ---------------------------------------------------
@pytest.fixture
def win(qapp):
    from gallery_py_qt.main_window import MainWindow
    return MainWindow()


def test_construction_shows_no_popup(win):
    from PySide6.QtWidgets import QDialog
    assert win.findChildren(QDialog) == []
    assert win._guide_btn.text().endswith("Guide")


def test_maybe_show_welcome_only_on_first_run(win, monkeypatch):
    shown = []
    monkeypatch.setattr(WelcomeDialog, "exec", lambda self: shown.append(1))

    win._prefs["welcome_seen"] = True
    win.maybe_show_welcome()
    assert shown == []                        # already seen → nothing

    win._prefs.pop("welcome_seen", None)
    win.maybe_show_welcome()
    assert shown == [1]                        # first run → shown once


def test_show_welcome_persists_optout(win, monkeypatch):
    saved = {}
    monkeypatch.setattr("gallery_py_qt.main_window.prefs.save_prefs",
                        lambda p: saved.update(p))
    # User unticks "show at startup" and closes.
    monkeypatch.setattr(
        WelcomeDialog, "exec",
        lambda self: self._startup_cb.setChecked(False))
    win.show_welcome()
    assert win._prefs["welcome_seen"] is True   # opted out
    assert saved.get("welcome_seen") is True     # and persisted
    # A second first-run check now stays silent.
    shown = []
    monkeypatch.setattr(WelcomeDialog, "exec", lambda self: shown.append(1))
    win.maybe_show_welcome()
    assert shown == []


def test_reopening_can_reenable_startup(win, monkeypatch):
    monkeypatch.setattr("gallery_py_qt.main_window.prefs.save_prefs",
                        lambda p: None)
    win._prefs["welcome_seen"] = True            # previously opted out
    # User reopens from the button and re-ticks "show at startup".
    monkeypatch.setattr(
        WelcomeDialog, "exec",
        lambda self: self._startup_cb.setChecked(True))
    win.show_welcome()
    assert win._prefs["welcome_seen"] is False   # will show again next launch
