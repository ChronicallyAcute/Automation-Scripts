"""Settings dialog — surfaces options that were previously prefs-file only.

Returns the (possibly changed) prefs dict via `result_prefs`; the caller
persists and applies them.
"""
from __future__ import annotations

from PySide6.QtWidgets import (QDialog, QFormLayout, QComboBox, QSpinBox,
                               QDialogButtonBox, QLabel, QVBoxLayout,
                               QPushButton)

from . import config


class SettingsDialog(QDialog):
    _LINK_LABELS = [("Copy files (default)", "copy"),
                    ("Hard-link (save space, same drive)", "hardlink"),
                    ("Symlink (save space)", "symlink")]
    _IO_LABELS = [("Auto-detect (default)", "auto"),
                  ("Force reduced (external/slow storage)", "on"),
                  ("Force full speed", "off")]

    def __init__(self, prefs: dict, parent=None, on_manage_tags=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setMinimumWidth(400)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())
        self._prefs = dict(prefs)

        root = QVBoxLayout(self)
        form = QFormLayout()
        root.addLayout(form)

        self._theme = QComboBox()
        for name in config.THEMES:
            self._theme.addItem(config.theme_label(name), name)
        self._select(self._theme, prefs.get("theme", "dark"))
        form.addRow("Theme", self._theme)

        self._link = QComboBox()
        for label, val in self._LINK_LABELS:
            self._link.addItem(label, val)
        self._select(self._link, prefs.get("link_mode", "copy"))
        form.addRow("Favourites / tag folders", self._link)

        self._io = QComboBox()
        for label, val in self._IO_LABELS:
            self._io.addItem(label, val)
        li = prefs.get("low_io_mode")
        self._select(self._io, "on" if li is True else "off" if li is False
                     else "auto")
        form.addRow("Parallel reads", self._io)

        self._purge = QSpinBox()
        self._purge.setRange(0, 3650)
        self._purge.setSpecialValueText("Never")
        self._purge.setSuffix(" days")
        self._purge.setValue(int(prefs.get("trash_purge_days", 0) or 0))
        form.addRow("Auto-empty trash after", self._purge)

        if on_manage_tags is not None:
            btn = QPushButton("Manage tags…")
            btn.clicked.connect(on_manage_tags)
            form.addRow("Tags", btn)

        note = QLabel("Changes apply immediately; link mode affects newly "
                      "favourited/tagged items.")
        note.setWordWrap(True)
        note.setStyleSheet(f"color: {config.FG_DIM}; font-size: 11px;")
        root.addWidget(note)

        bb = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                              | QDialogButtonBox.StandardButton.Cancel)
        bb.accepted.connect(self.accept)
        bb.rejected.connect(self.reject)
        root.addWidget(bb)

    @staticmethod
    def _select(combo: QComboBox, value) -> None:
        i = combo.findData(value)
        if i >= 0:
            combo.setCurrentIndex(i)

    def result_prefs(self) -> dict:
        """Merge the chosen values back into a prefs dict."""
        p = dict(self._prefs)
        p["theme"] = self._theme.currentData()
        p["link_mode"] = self._link.currentData()
        io = self._io.currentData()
        p["low_io_mode"] = (True if io == "on" else False if io == "off"
                            else None)
        p["trash_purge_days"] = self._purge.value()
        return p
