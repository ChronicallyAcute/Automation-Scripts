"""Reusable filesystem-picker pieces shared by the import and album dialogs.

The media-import picker grew a checkbox filesystem tree, a debounced hover
thumbnail preview and a quick-access row.  The album (folder) tagging dialog
wants exactly the same browsing experience, so those pieces live here rather
than inside main_window — a dialog that only needs a folder tree should not
drag the whole main window (and multiview, lightbox, QtMultimedia …) in with it.

``main_window`` re-exports ``_CheckFSModel`` for backwards compatibility.
"""
from __future__ import annotations
import os

from PySide6.QtCore import (Qt, QDir, QModelIndex, QObject, QEvent, QTimer)
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (QFileSystemModel, QLabel, QHBoxLayout,
                               QPushButton, QComboBox, QWidget)

from . import config

# Media classes offered by the "Show:" combo, in display order.
MEDIA_CLASSES = (("All media", "all"), ("Images", "images"),
                 ("GIFs", "gifs"), ("Videos", "videos"))


class _CheckFSModel(QFileSystemModel):
    """QFileSystemModel extended with per-item checkbox state.

    Column 0 gets Qt.ItemFlag.ItemIsUserCheckable so the view draws a native
    checkbox on every entry.  Both directories AND individual media files are
    shown and checkable (non-media files are hidden via name filters).
    Checked paths are tracked in a set and survive expansion/collapsing.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._checked: set[str] = set()
        self.setFilter(QDir.Filter.AllDirs | QDir.Filter.Files
                       | QDir.Filter.NoDotAndDotDot)
        # Only media files are relevant; hide everything else entirely.
        self.setNameFilterDisables(False)
        self.set_media_class("all")
        self.setRootPath("")

    def set_media_class(self, cls: str) -> None:
        """Restrict which media files are shown/importable (all/images/gifs/
        videos).  Directories always remain visible so the tree stays browsable."""
        by_class = {
            "all":    config.SUPPORTED,
            "images": {e for e in config.IMAGE_EXT if e != ".gif"},
            "gifs":   {".gif"},
            "videos": config.VIDEO_EXT,
        }
        exts = by_class.get(cls, config.SUPPORTED)
        self.setNameFilters([f"*{ext}" for ext in sorted(exts)])

    def flags(self, index: QModelIndex):
        base = super().flags(index)
        if index.isValid() and index.column() == 0:
            base |= Qt.ItemFlag.ItemIsUserCheckable
        return base

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if (role == Qt.ItemDataRole.CheckStateRole
                and index.isValid() and index.column() == 0):
            return (Qt.CheckState.Checked if self.filePath(index) in self._checked
                    else Qt.CheckState.Unchecked)
        return super().data(index, role)

    def setData(self, index: QModelIndex, value, role=Qt.ItemDataRole.EditRole) -> bool:
        if (role == Qt.ItemDataRole.CheckStateRole
                and index.isValid() and index.column() == 0):
            path = self.filePath(index)
            cs = Qt.CheckState(value) if isinstance(value, int) else value
            if cs == Qt.CheckState.Checked:
                self._checked.add(path)
            else:
                self._checked.discard(path)
            self.dataChanged.emit(index, index, [role])
            return True
        return super().setData(index, value, role)

    # -- check-set maintenance (public so other dialogs need not poke _checked)
    def add(self, path: str) -> None:
        self._checked.add(path)

    def discard(self, path: str) -> None:
        self._checked.discard(path)

    def clear(self) -> None:
        self._checked.clear()

    def is_checked(self, path: str) -> bool:
        """Raw membership — unlike checked_paths(), does NOT require the path to
        still exist on disk (needed to re-key a tick across a rename/move)."""
        return path in self._checked

    def checked_paths(self) -> list[str]:
        return [p for p in sorted(self._checked) if os.path.isdir(p)]

    def checked_files(self) -> list[str]:
        return [p for p in sorted(self._checked) if os.path.isfile(p)]

    def checked_all(self) -> list[str]:
        return [p for p in sorted(self._checked) if os.path.exists(p)]


class HoverPreview(QObject):
    """Debounced floating thumbnail for the file the mouse is over in a tree.

    Extracted from the import picker so the album dialog gets the identical
    behaviour: hovering a media file pops a small tooltip-style thumbnail fed
    by the shared async ThumbnailLoader, cancelled on leave/hide.
    """

    def __init__(self, owner: QWidget, tree, fs_model, loader=None,
                 max_px: int = 160):
        super().__init__(owner)
        self._tree = tree
        self._fs = fs_model
        self._loader = loader
        self._max_px = max_px
        self.path = ""

        tree.setMouseTracking(True)
        tree.entered.connect(self.on_hover_index)
        tree.viewport().installEventFilter(self)

        self.label = QLabel(owner, Qt.WindowType.ToolTip)
        self.label.setStyleSheet(
            "background: #101010; border: 1px solid #333; padding: 3px;")
        self.label.hide()

        self.timer = QTimer(self)
        self.timer.setSingleShot(True)
        self.timer.setInterval(140)     # debounce fast hover sweeps
        self.timer.timeout.connect(self._request)
        if loader is not None:
            loader.ready.connect(self.on_ready)

    def on_hover_index(self, index: QModelIndex) -> None:
        path = self._fs.filePath(index)
        if (os.path.isfile(path)
                and os.path.splitext(path.lower())[1] in config.SUPPORTED):
            if path != self.path:
                self.path = path
                self.label.hide()
                self.timer.start()
        else:
            self.hide()

    def _request(self) -> None:
        if self.path and self._loader is not None:
            self._loader.request(self.path, self._max_px)

    def on_ready(self, path: str, max_px: int, qim) -> None:
        # The loader is shared with the gallery grid and the contents pane, so
        # ignore anything that isn't this preview's own request.
        if path != self.path or max_px != self._max_px or qim.isNull():
            return
        from PySide6.QtGui import QCursor
        self.label.setPixmap(QPixmap.fromImage(qim))
        self.label.adjustSize()
        pos = QCursor.pos()
        self.label.move(pos.x() + 18, pos.y() + 12)
        self.label.show()

    def hide(self) -> None:
        self.path = ""
        self.timer.stop()
        self.label.hide()

    def eventFilter(self, obj, event):
        if (obj is self._tree.viewport()
                and event.type() in (QEvent.Type.Leave, QEvent.Type.Hide)):
            self.hide()
        return super().eventFilter(obj, event)


def quick_access_row(fs_model, goto_cb) -> "tuple[QHBoxLayout, QComboBox]":
    """Build the "Go to: … / Show: …" strip that sits above a picker tree.

    Returns (layout, media-class combo); the caller keeps the combo so it can
    be addressed by name.  `goto_cb(path)` jumps the tree to a folder.
    """
    quick = QHBoxLayout()
    quick.setSpacing(4)
    ql = QLabel("Go to:")
    ql.setStyleSheet(f"color: {config.FG_MID};")
    quick.addWidget(ql)
    home_dir = QDir.homePath()
    for name in ("Downloads", "Pictures", "Videos", "Desktop", "Home"):
        p = home_dir if name == "Home" else os.path.join(home_dir, name)
        if not os.path.isdir(p):
            continue
        b = QPushButton(name)
        b.setToolTip(p)
        b.clicked.connect(lambda _=False, pp=p: goto_cb(pp))
        quick.addWidget(b)
    quick.addStretch(1)
    # Media-type filter for what the tree shows/imports.
    quick.addWidget(QLabel("Show:"))
    combo = QComboBox()
    for label, val in MEDIA_CLASSES:
        combo.addItem(label, val)
    combo.currentIndexChanged.connect(
        lambda _=0: fs_model.set_media_class(combo.currentData()))
    quick.addWidget(combo)
    return quick, combo
