"""Dialog to customise the tag set: add / remove / rename.

Mutations go straight to the tags engine (which migrates the store and the
Favorites subfolders); the dialog reports whether anything changed so the
caller can rebuild the tag UI.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QListWidget,
                               QListWidgetItem, QLineEdit, QPushButton, QLabel,
                               QInputDialog, QMessageBox)

from . import config
from .engine import tags


class TagManagerDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage tags")
        self.setMinimumWidth(320)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())
        self.changed = False

        root = QVBoxLayout(self)
        root.addWidget(QLabel("Tags (used as buttons and Favorites subfolders):"))
        self._list = QListWidget()
        self._reload()
        root.addWidget(self._list, 1)

        add_row = QHBoxLayout()
        self._new = QLineEdit()
        self._new.setPlaceholderText("new tag name…")
        self._new.returnPressed.connect(self._add)
        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self._add)
        add_row.addWidget(self._new, 1)
        add_row.addWidget(add_btn)
        root.addLayout(add_row)

        btns = QHBoxLayout()
        rename_btn = QPushButton("Rename…")
        rename_btn.clicked.connect(self._rename)
        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(self._remove)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btns.addWidget(rename_btn)
        btns.addWidget(remove_btn)
        btns.addStretch(1)
        btns.addWidget(close_btn)
        root.addLayout(btns)

    def _reload(self) -> None:
        self._list.clear()
        for t in tags.get_tags():
            self._list.addItem(QListWidgetItem(t))

    def _selected(self) -> str | None:
        it = self._list.currentItem()
        return it.text() if it is not None else None

    def _add(self) -> None:
        name = self._new.text().strip()
        if name and tags.add_tag(name):
            self.changed = True
            self._new.clear()
            self._reload()

    def _remove(self) -> None:
        name = self._selected()
        if not name:
            return
        if QMessageBox.question(
                self, "Remove tag",
                f"Remove tag “{name}”?\n\nIt will be stripped from all "
                f"tagged files and its {config.FAVORITES_DIR}\\{name} folder "
                "deleted.") != QMessageBox.StandardButton.Yes:
            return
        tags.remove_tag(name)
        self.changed = True
        self._reload()

    def _rename(self) -> None:
        old = self._selected()
        if not old:
            return
        new, ok = QInputDialog.getText(self, "Rename tag", f"Rename “{old}” to:",
                                       text=old)
        if ok and new.strip() and new.strip() != old:
            if tags.rename_tag(old, new.strip()):
                self.changed = True
                self._reload()
            else:
                QMessageBox.warning(self, "Rename tag",
                                    "Couldn't rename (name already in use?).")
