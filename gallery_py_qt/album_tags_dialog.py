"""Tag whole albums (folders): assign the shared tag vocabulary to folders and
mirror each tagged album into the "folder tags" subtree (copy or link).

Lists the albums currently loaded in the gallery (plus any the user adds), each
with a checkbox and its current folder-tags.  A row of tag buttons toggles a
tag across every checked album — batch semantics matching the grid: if any
checked album lacks the tag it is added to all, otherwise removed from all.
The heavy copy/link work is delegated to :mod:`engine.foldertags`, which runs
it on the background mirror worker.
"""
from __future__ import annotations
import os

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QListWidget, QListWidgetItem, QAbstractItemView,
                               QPushButton, QFileDialog, QFrame)

from . import config
from .engine import favorites, foldertags, tags


class AlbumTagsDialog(QDialog):
    def __init__(self, folders: "list[str]", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Tag albums")
        self.resize(640, 500)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())

        # De-dup while preserving order; keep only real directories.
        self._albums: "list[str]" = []
        for f in folders:
            if os.path.isdir(f) and f not in self._albums:
                self._albums.append(f)

        root = QVBoxLayout(self)

        intro = QLabel(
            "Tagging an album mirrors the whole folder into "
            f"“{foldertags.FOLDER_TAGS_DIRNAME}/&lt;tag&gt;/” inside your "
            "Gallery Favorites.")
        intro.setWordWrap(True)
        intro.setStyleSheet(f"color: {config.FG_MID}; padding: 2px;")
        root.addWidget(intro)

        self._list = QListWidget()
        self._list.setSelectionMode(
            QAbstractItemView.SelectionMode.NoSelection)
        root.addWidget(self._list, 1)

        add_row = QHBoxLayout()
        add_btn = QPushButton(f"{config.ICON_FOLDER}  Add album folder…")
        add_btn.clicked.connect(self._add_album)
        self._all_btn = QPushButton("Check all")
        self._all_btn.clicked.connect(self._check_all)
        self._none_btn = QPushButton("Uncheck all")
        self._none_btn.clicked.connect(self._uncheck_all)
        add_row.addWidget(add_btn)
        add_row.addStretch(1)
        add_row.addWidget(self._all_btn)
        add_row.addWidget(self._none_btn)
        root.addLayout(add_row)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        root.addWidget(sep)

        tag_lbl = QLabel("Apply tag to checked album(s):")
        tag_lbl.setStyleSheet(f"color: {config.FG_MID};")
        root.addWidget(tag_lbl)

        self._tag_row = QHBoxLayout()
        self._tag_btns: "list[QPushButton]" = []
        self._build_tag_buttons()
        root.addLayout(self._tag_row)

        mode = {"copy": "copies", "hardlink": "hard-links",
                "symlink": "sym-links"}.get(favorites.LINK_MODE, "copies")
        self._mode_lbl = QLabel(f"Link mode: {favorites.LINK_MODE} "
                                f"(each album {mode} into the tag folder).")
        self._mode_lbl.setStyleSheet(f"color: {config.FG_DIM}; padding-top: 4px;")
        root.addWidget(self._mode_lbl)

        close_row = QHBoxLayout()
        close_row.addStretch(1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        close_row.addWidget(close_btn)
        root.addLayout(close_row)

        self._refresh()

    # -- album list ------------------------------------------------------------
    def _refresh(self) -> None:
        self._list.clear()
        if not self._albums:
            item = QListWidgetItem("No albums. Use “Add album folder…”.")
            item.setFlags(Qt.ItemFlag.NoItemFlags)
            self._list.addItem(item)
            return
        for folder in self._albums:
            cur = foldertags.tags_for(folder)
            tag_str = ("  ·  " + ", ".join(cur)) if cur else "  ·  (untagged)"
            item = QListWidgetItem(
                f"{os.path.basename(os.path.normpath(folder))}{tag_str}\n"
                f"    {folder}")
            item.setFlags(Qt.ItemFlag.ItemIsUserCheckable
                          | Qt.ItemFlag.ItemIsEnabled)
            item.setCheckState(Qt.CheckState.Checked)
            item.setData(Qt.ItemDataRole.UserRole, folder)
            self._list.addItem(item)

    def _add_album(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Choose an album folder")
        if d and os.path.isdir(d) and d not in self._albums:
            self._albums.append(d)
            self._refresh()

    def _check_all(self) -> None:
        self._set_all(Qt.CheckState.Checked)

    def _uncheck_all(self) -> None:
        self._set_all(Qt.CheckState.Unchecked)

    def _set_all(self, state: "Qt.CheckState") -> None:
        for i in range(self._list.count()):
            it = self._list.item(i)
            if it.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                it.setCheckState(state)

    def _checked_albums(self) -> "list[str]":
        out = []
        for i in range(self._list.count()):
            it = self._list.item(i)
            if (it.flags() & Qt.ItemFlag.ItemIsUserCheckable
                    and it.checkState() == Qt.CheckState.Checked):
                out.append(it.data(Qt.ItemDataRole.UserRole))
        return out

    # -- tagging ---------------------------------------------------------------
    def _build_tag_buttons(self) -> None:
        for b in self._tag_btns:
            b.deleteLater()
        self._tag_btns.clear()
        for t in tags.TAGS:
            btn = QPushButton(t)
            btn.setToolTip(f"Toggle “{t}” on the checked album(s)")
            btn.clicked.connect(lambda _=False, tag=t: self._toggle_tag(tag))
            self._tag_row.addWidget(btn)
            self._tag_btns.append(btn)
        self._tag_row.addStretch(1)

    def _toggle_tag(self, tag: str) -> None:
        albums = self._checked_albums()
        if not albums:
            return
        # Add to all if any album lacks the tag; otherwise remove from all.
        add = any(tag not in foldertags.tags_for(a) for a in albums)
        for a in albums:
            if (tag in foldertags.tags_for(a)) != add:
                foldertags.toggle_folder_tag(a, tag)
        self._refresh()
