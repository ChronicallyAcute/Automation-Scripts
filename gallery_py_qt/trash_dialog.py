"""Trash browser: restore, permanently delete, or empty trashed media.

The app moves deleted files to a trash directory (never erasing immediately).
Previously that directory had no UI and grew forever.  This dialog lists its
contents — joined with the manifest so each item knows where it came from —
and offers Restore / Delete permanently / Empty, plus an opt-in auto-purge.
"""
from __future__ import annotations
import datetime

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QListWidget, QListWidgetItem, QPushButton,
                               QAbstractItemView, QCheckBox, QMessageBox)

from . import config
from .engine import favorites

_AUTO_PURGE_DAYS = 30


def _fmt_size(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes} B"
    kb = nbytes / 1024
    if kb < 1024:
        return f"{kb:.0f} KB"
    mb = kb / 1024
    return f"{mb:.1f} MB" if mb < 1024 else f"{mb / 1024:.2f} GB"


class TrashDialog(QDialog):
    def __init__(self, parent=None, purge_days: int = 0):
        super().__init__(parent)
        self.setWindowTitle("Trash")
        self.resize(640, 460)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())
        self.auto_purge_days = purge_days

        root = QVBoxLayout(self)

        self._header = QLabel("")
        self._header.setStyleSheet(f"color: {config.FG_MID}; padding: 2px;")
        root.addWidget(self._header)

        self._list = QListWidget()
        self._list.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._list.setAlternatingRowColors(False)
        root.addWidget(self._list, 1)

        btn_row = QHBoxLayout()
        self._restore_btn = QPushButton("Restore")
        self._restore_btn.setToolTip("Move selected item(s) back to where they came from")
        self._restore_btn.clicked.connect(self._restore_selected)
        self._delete_btn = QPushButton("Delete permanently")
        self._delete_btn.clicked.connect(self._delete_selected)
        self._empty_btn = QPushButton("Empty trash")
        self._empty_btn.clicked.connect(self._empty)
        btn_row.addWidget(self._restore_btn)
        btn_row.addWidget(self._delete_btn)
        btn_row.addStretch(1)
        btn_row.addWidget(self._empty_btn)
        root.addLayout(btn_row)

        self._auto_cb = QCheckBox(
            f"Automatically delete items older than {_AUTO_PURGE_DAYS} days on startup")
        self._auto_cb.setChecked(purge_days > 0)
        self._auto_cb.toggled.connect(self._on_auto_toggled)
        root.addWidget(self._auto_cb)

        close_row = QHBoxLayout()
        close_row.addStretch(1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        close_row.addWidget(close_btn)
        root.addLayout(close_row)

        self._refresh()

    # -- helpers ---------------------------------------------------------------
    def _on_auto_toggled(self, on: bool) -> None:
        self.auto_purge_days = _AUTO_PURGE_DAYS if on else 0

    def _refresh(self) -> None:
        items = favorites.list_trash()
        self._list.clear()
        total = 0
        for it in items:
            total += it["size"]
            when = datetime.datetime.fromtimestamp(
                it["mtime"]).strftime("%Y-%m-%d %H:%M")
            orig = it["orig"] or "(original location unknown)"
            label = f"{it['name']}\n    {_fmt_size(it['size'])}  ·  {when}  ·  {orig}"
            row = QListWidgetItem(label)
            row.setData(Qt.ItemDataRole.UserRole, it["path"])
            row.setData(Qt.ItemDataRole.UserRole + 1, bool(it["orig"]))
            self._list.addItem(row)
        n = len(items)
        self._header.setText(
            "Trash is empty" if n == 0
            else f"{n} item(s)  ·  {_fmt_size(total)} total")
        has = n > 0
        self._delete_btn.setEnabled(has)
        self._empty_btn.setEnabled(has)
        self._restore_btn.setEnabled(has)

    def _selected_paths(self) -> list[str]:
        return [i.data(Qt.ItemDataRole.UserRole)
                for i in self._list.selectedItems()]

    def _restore_selected(self) -> None:
        paths = self._selected_paths()
        if not paths:
            return
        restored = skipped = 0
        for p in paths:
            if favorites.restore_from_trash(p):
                restored += 1
            else:
                skipped += 1
        self._refresh()
        if skipped:
            QMessageBox.information(
                self, "Restore",
                f"Restored {restored}. {skipped} item(s) had no recorded "
                "original location and were left in the trash.")

    def _delete_selected(self) -> None:
        paths = self._selected_paths()
        if not paths:
            return
        if QMessageBox.question(
                self, "Delete permanently",
                f"Permanently delete {len(paths)} item(s)? This cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                ) != QMessageBox.StandardButton.Yes:
            return
        for p in paths:
            favorites.purge_item(p)
        self._refresh()

    def _empty(self) -> None:
        if QMessageBox.question(
                self, "Empty trash",
                "Permanently delete everything in the trash? This cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                ) != QMessageBox.StandardButton.Yes:
            return
        favorites.empty_trash()
        self._refresh()
