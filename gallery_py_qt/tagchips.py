"""The tag chip row, shared by multi-view tiles and single-view.

The chips are the quickest way to see and change what a file carries, and they
were only ever available on a multi-view tile — so looking at one item
full-screen, which is when you are most likely to be judging it, was the one
place you could not tag it.

Style lives here so both views cannot drift apart: a rounded, transparent chip
wearing its tag's colour — at full strength with weight and an outline when the
tag is set, muted when it is not, so a row reads without being read.
"""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (QWidget, QHBoxLayout, QToolButton, QInputDialog,
                               QMessageBox)

from . import config
from .engine import tags

CHIP_CSS_ON = ("QToolButton { color: %s; background: transparent;"
               " border: 1px solid %s; border-radius: 2px;"
               " font-size: %dpx; font-weight: bold; padding: 0 3px; }"
               " QToolButton:hover { background: rgba(0,0,0,90); }")
CHIP_CSS_OFF = ("QToolButton { color: %s; background: transparent;"
                " border: 1px solid transparent; border-radius: 2px;"
                " font-size: %dpx; padding: 0 3px; }"
                " QToolButton:hover { background: rgba(0,0,0,90); }")


def muted(hex_colour: str, amount: float = 0.5) -> str:
    """`hex_colour` faded toward grey, for a chip whose tag is not set.

    Keeps the hue — which is what identifies the tag — while staying clearly
    weaker than a set chip.
    """
    try:
        h = hex_colour.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
        mix = lambda c: int(c + (0xB4 - c) * amount)      # noqa: E731
        return f"#{mix(r):02x}{mix(g):02x}{mix(b):02x}"
    except Exception:
        return config.OVERLAY_FG


class TagChipBar(QWidget):
    """A row of tag chips for one file, plus ditto and new-tag."""

    tagsChanged = Signal(str)        # path whose tags were edited
    tagSetChanged = Signal()         # a new tag name was coined

    def __init__(self, parent=None, font_px: int = 11):
        super().__init__(parent)
        self._font_px = font_px
        self._path = ""
        self._btns: "dict[str, QToolButton]" = {}
        lay = QHBoxLayout(self)
        lay.setContentsMargins(2, 1, 2, 1)
        lay.setSpacing(3)
        self._lay = lay
        self.rebuild()

    # -- construction ----------------------------------------------------------
    def _chip(self, text: str, tip: str) -> QToolButton:
        b = QToolButton(self)
        b.setText(text)
        b.setToolTip(tip)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        self._lay.addWidget(b)
        return b

    def rebuild(self) -> None:
        """(Re)create the chips from the current tag set."""
        while self._lay.count():
            item = self._lay.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()
        self._btns = {}
        self._repeat_btn = self._chip("〃", "Apply the most recently used tags")
        self._repeat_btn.clicked.connect(self._apply_recent)
        self._new_btn = self._chip("＋#",
                                   "Create a new tag and apply it to this file")
        self._new_btn.clicked.connect(self._create_tag)
        for name in tags.get_tags():
            b = self._chip(name, f'Toggle tag "{name}"')
            b.clicked.connect(lambda _=False, t=name: self.toggle(t))
            self._btns[name] = b
        self._lay.addStretch(1)
        self.refresh()

    # -- state -----------------------------------------------------------------
    def set_path(self, path: str) -> None:
        self._path = path or ""
        self.refresh()

    def path(self) -> str:
        return self._path

    def refresh(self) -> None:
        """Re-colour every chip against the current file's tags."""
        cur = set(tags.tags_for(self._path)) if self._path else set()
        for name, b in self._btns.items():
            hue = tags.color_of(name) or config.ACCENT
            b.setStyleSheet(
                (CHIP_CSS_ON % (hue, hue, self._font_px)) if name in cur
                else (CHIP_CSS_OFF % (muted(hue), self._font_px)))
        recent = tags.recent_tags()
        pending = bool(self._path) and any(
            t not in cur for t in recent if t in tags.TAGS)
        self._repeat_btn.setEnabled(pending)
        self._repeat_btn.setStyleSheet(
            (CHIP_CSS_ON % (config.ACCENT, config.ACCENT, self._font_px))
            if pending else (CHIP_CSS_OFF % (config.FG_DIM, self._font_px)))
        if recent:
            self._repeat_btn.setToolTip(
                "Apply the most recently used tags: " + ", ".join(recent))
        self._new_btn.setStyleSheet(
            CHIP_CSS_OFF % (config.OVERLAY_FG, self._font_px))

    # -- actions ---------------------------------------------------------------
    def toggle(self, name: str) -> None:
        if not self._path:
            return
        tags.toggle_tag(self._path, name)
        self.refresh()
        self.tagsChanged.emit(self._path)

    def _apply_recent(self) -> None:
        if not self._path:
            return
        if tags.apply_recent(self._path):
            self.refresh()
            self.tagsChanged.emit(self._path)

    def _create_tag(self) -> None:
        if not self._path:
            return
        name, ok = QInputDialog.getText(self, "New tag", "Tag name:")
        name = (name or "").strip()
        if not ok or not name:
            return
        if name not in tags.get_tags() and not tags.add_tag(name):
            QMessageBox.information(self, "New tag",
                                    f'"{name}" could not be added.')
            return
        self.rebuild()
        if name not in tags.tags_for(self._path):
            self.toggle(name)
        self.tagSetChanged.emit()
