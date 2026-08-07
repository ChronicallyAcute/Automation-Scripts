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

from PySide6.QtCore import (Qt, QDir, QModelIndex, QObject, QEvent, QTimer,
                            QStandardPaths, QUrl)
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (QFileSystemModel, QLabel, QHBoxLayout,
                               QVBoxLayout, QPushButton, QToolButton, QMenu,
                               QWidget, QLineEdit)

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

    @staticmethod
    def _class_exts(cls: str) -> "set[str]":
        return {
            "all":    set(config.SUPPORTED),
            "images": {e for e in config.IMAGE_EXT if e != ".gif"},
            "gifs":   {".gif"},
            "videos": set(config.VIDEO_EXT),
        }.get(cls, set(config.SUPPORTED))

    def set_media_class(self, cls: str) -> None:
        """Single-class convenience wrapper for set_media_classes()."""
        self.set_media_classes({cls})

    def set_media_classes(self, classes) -> None:
        """Show media of ANY of the given classes (images / gifs / videos), so
        several can be combined, e.g. gifs AND videos.  Empty, or containing
        "all", shows everything.  Directories always remain visible."""
        classes = set(classes)
        if not classes or "all" in classes:
            exts = set(config.SUPPORTED)
        else:
            exts = set()
            for c in classes:
                exts |= self._class_exts(c)
            if not exts:
                exts = set(config.SUPPORTED)
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


def _class_menu_button(fs_model) -> QToolButton:
    """A "Show ▾" button whose checkable menu multi-selects media classes, so
    e.g. GIFs AND videos can be shown together.  All checked = all media."""
    btn = QToolButton()
    btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
    menu = QMenu(btn)
    actions = {}
    for label, val in (("Images", "images"), ("GIFs", "gifs"),
                       ("Videos", "videos")):
        act = menu.addAction(label)
        act.setCheckable(True)
        act.setChecked(True)
        actions[val] = act

    def _apply() -> None:
        chosen = {v for v, a in actions.items() if a.isChecked()}
        fs_model.set_media_classes(chosen)
        if len(chosen) == 3 or not chosen:
            btn.setText("Show: all ▾")
        else:
            btn.setText("Show: " + "+".join(
                v for v in ("images", "gifs", "videos") if v in chosen) + " ▾")

    for a in actions.values():
        a.toggled.connect(lambda _=False: _apply())
    btn.setMenu(menu)
    btn._class_actions = actions      # exposed for callers / tests
    _apply()
    return btn


def _quick_locations() -> "list[tuple[str, str]]":
    """Resolve the (label, path) pairs for the quick-access strip.

    ``QStandardPaths`` follows OS redirection — a Downloads folder relocated by
    OneDrive, or an XDG user-dirs override, resolves to its real location rather
    than a naive ``~/Downloads`` that may not exist.  We fall back to the naive
    ``home/<name>`` join when the platform returns nothing, and de-duplicate so
    a bare home path that coincides with a standard location isn't listed twice.
    """
    SL = QStandardPaths.StandardLocation
    home = QDir.homePath()
    wanted = (
        ("Downloads", SL.DownloadLocation, "Downloads"),
        ("Pictures", SL.PicturesLocation, "Pictures"),
        ("Videos", SL.MoviesLocation, "Videos"),
        ("Desktop", SL.DesktopLocation, "Desktop"),
        ("Home", SL.HomeLocation, ""),
    )
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for label, loc, sub in wanted:
        p = QStandardPaths.writableLocation(loc)
        if not p or not os.path.isdir(p):
            p = home if not sub else os.path.join(home, sub)
        p = os.path.normpath(p)
        if not os.path.isdir(p) or p in seen:
            continue
        seen.add(p)
        out.append((label, p))
    return out


def normalize_pasted_path(text: str) -> str:
    """Turn user-pasted directory text into a filesystem path, or "".

    Copes with the shapes people actually paste from a file explorer:
      * ``file:///C:/Users/Dan/Downloads`` or ``file://host/share`` URLs,
      * surrounding single/double quotes (Windows "Copy as path" wraps in "),
      * a leading/trailing whitespace and a trailing separator,
      * ``~``/``~user`` home shortcuts,
      * Windows backslashes on any OS.

    Returns the normalised path when it points at an existing directory, else
    "" so the caller can ignore junk without navigating anywhere.
    """
    s = (text or "").strip()
    if not s:
        return ""
    # Strip a single layer of matching surrounding quotes.
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    if s[:5].lower() == "file:":
        local = QUrl(s).toLocalFile()
        if local:
            s = local
    s = os.path.expanduser(s)
    # Accept a pasted file too: navigate to its containing folder.
    if os.path.isfile(s):
        s = os.path.dirname(s)
    if not s:
        return ""
    s = os.path.normpath(s)
    return s if os.path.isdir(s) else ""


def quick_access_row(fs_model, goto_cb):
    """Build the "Go to: … / Show: …" strip that sits above a picker tree.

    Returns (layout, media-class button); the button's checkable menu selects
    one or more media classes.  `goto_cb(path)` jumps the tree to a folder.

    The strip carries two rows: one-click shortcuts to the common media
    folders, and a free-text path box so any directory can be reached by
    pasting its path (e.g. ``C:\\Users\\me\\Downloads``) when auto-detection
    misses it — the box also accepts ``file://`` URLs and quoted paths.
    """
    outer = QVBoxLayout()
    outer.setSpacing(4)

    quick = QHBoxLayout()
    quick.setSpacing(4)
    ql = QLabel("Go to:")
    ql.setStyleSheet(f"color: {config.FG_MID};")
    quick.addWidget(ql)
    for name, p in _quick_locations():
        b = QPushButton(name)
        b.setToolTip(p)
        b.clicked.connect(lambda _=False, pp=p: goto_cb(pp))
        quick.addWidget(b)
    quick.addStretch(1)
    # Media-type filter (multi-select) for what the tree shows/imports.
    btn = _class_menu_button(fs_model)
    quick.addWidget(btn)
    outer.addLayout(quick)

    # Free-text path box: paste/type any folder and jump straight to it.
    path_row = QHBoxLayout()
    path_row.setSpacing(4)
    pl = QLabel("Path:")
    pl.setStyleSheet(f"color: {config.FG_MID};")
    path_row.addWidget(pl)
    edit = QLineEdit()
    edit.setClearButtonEnabled(True)
    edit.setPlaceholderText(
        r"Paste a folder path (e.g. C:\Users\You\Downloads) and press Enter")
    go = QPushButton("Go")

    def _jump() -> None:
        resolved = normalize_pasted_path(edit.text())
        if resolved:
            edit.setStyleSheet("")
            goto_cb(resolved)
        else:
            # Flag the entry as unusable without stealing focus/alerting.
            edit.setStyleSheet("QLineEdit { border: 1px solid #c0392b; }")

    edit.returnPressed.connect(_jump)
    go.clicked.connect(_jump)
    path_row.addWidget(edit, 1)
    path_row.addWidget(go)
    outer.addLayout(path_row)
    outer._path_edit = edit          # exposed for callers / tests
    return outer, btn
