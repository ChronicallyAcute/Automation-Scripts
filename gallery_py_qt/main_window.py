"""Main application window: toolbar, gallery, scanning, wiring, persistence.

Fork improvements over gallery_qt.main_window:
  \u2022 Streaming scan \u2014 _StreamScanJob emits path batches incrementally via a
    batch signal so the gallery populates as files are discovered, rather than
    waiting for a full-folder enumeration to complete before showing anything.
    A generation counter (_scan_gen) ensures stale batches from a superseded
    scan are silently dropped when the user opens a new folder mid-scan.
  \u2022 ThumbnailLoader is constructed without an explicit max_threads so it uses
    the auto-detected CPU count instead of the hard-coded 4.
"""
from __future__ import annotations
import os

from PySide6.QtCore import (Qt, QObject, QRunnable, QThreadPool, Signal,
                            QTimer, QSize, QDir, QModelIndex)
from PySide6.QtGui import QAction, QKeySequence, QShortcut, QPixmap
from PySide6.QtWidgets import (QMainWindow, QWidget, QFrame, QHBoxLayout,
                               QVBoxLayout, QLabel, QToolButton, QLineEdit,
                               QComboBox, QSpinBox, QMenu, QPushButton,
                               QApplication, QListView, QTreeView,
                               QAbstractItemView, QGridLayout, QDialog,
                               QFileSystemModel, QDialogButtonBox,
                               QSplitter, QSizePolicy, QStackedWidget,
                               QCheckBox)

BAR_HIDE_MS = 2000


# ---------------------------------------------------------------------------
# Custom folder picker with checkbox support
# ---------------------------------------------------------------------------

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
        self.setNameFilters([f"*{ext}" for ext in sorted(config.SUPPORTED)])
        self.setNameFilterDisables(False)
        self.setRootPath("")

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

    def checked_paths(self) -> list[str]:
        return [p for p in sorted(self._checked) if os.path.isdir(p)]

    def checked_files(self) -> list[str]:
        return [p for p in sorted(self._checked) if os.path.isfile(p)]

    def checked_all(self) -> list[str]:
        return [p for p in sorted(self._checked) if os.path.exists(p)]


class _FolderPickDlg(QDialog):
    """Folder picker: checkbox tree (left) + selected-folder list (right).

    Users can tick folders via checkboxes, Ctrl/Shift-click for multi-select,
    or double-click a folder to add it immediately.  Both mechanisms are
    reconciled on Accept: checked folders take priority; if nothing is checked,
    the current tree selection is used instead.
    """

    def __init__(self, parent=None, recents: list[str] | None = None,
                 loader=None):
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle(
            "Open folders & files  —  check boxes or Ctrl-click for multi-select")
        self.resize(900, 580)
        self.setStyleSheet(parent.styleSheet() if parent else "")
        self._loader = loader

        # File-system model with checkboxes (folders AND media files)
        self._fs = _CheckFSModel(self)

        # Tree view
        self._tree = QTreeView()
        self._tree.setModel(self._fs)
        self._tree.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._tree.setAnimated(True)
        for col in range(1, 4):           # hide size/type/date
            self._tree.hideColumn(col)
        home = self._fs.index(QDir.homePath())
        self._tree.expand(home)
        self._tree.scrollTo(home)
        self._tree.setCurrentIndex(home)
        self._tree.doubleClicked.connect(self._on_dbl)

        # Hover thumbnail preview for individual files: a small floating
        # tooltip-style label fed by the shared async thumbnail loader.
        self._tree.setMouseTracking(True)
        self._tree.entered.connect(self._on_hover_index)
        self._tree.viewport().installEventFilter(self)
        self._preview = QLabel(self, Qt.WindowType.ToolTip)
        self._preview.setStyleSheet(
            "background: #101010; border: 1px solid #333; padding: 3px;")
        self._preview.hide()
        self._preview_path = ""
        self._preview_timer = QTimer(self)
        self._preview_timer.setSingleShot(True)
        self._preview_timer.setInterval(140)     # debounce fast hover sweeps
        self._preview_timer.timeout.connect(self._request_preview)
        if self._loader is not None:
            self._loader.ready.connect(self._on_preview_ready)

        # Quick access: common media locations, one click to jump the tree.
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
            b.clicked.connect(lambda _=False, pp=p: self._goto(pp))
            quick.addWidget(b)
        quick.addStretch(1)

        # Checked-paths list
        self._list = QListView()
        from PySide6.QtCore import QStringListModel
        self._list_model = QStringListModel(self)
        self._list.setModel(self._list_model)
        self._list.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._list.setSizePolicy(QSizePolicy.Policy.Expanding,
                                 QSizePolicy.Policy.Expanding)

        list_label = QLabel("Selected folders && files:")
        list_label.setStyleSheet(f"color: {config.FG_MID}; padding: 2px 0;")
        right_panel = QWidget()
        rlay = QVBoxLayout(right_panel)
        rlay.setContentsMargins(0, 0, 0, 0)
        rlay.addWidget(list_label)
        rlay.addWidget(self._list, 1)
        clear_btn = QPushButton("Clear all")
        clear_btn.clicked.connect(self._clear_all)
        rlay.addWidget(clear_btn)

        # Recent folders (quick-add)
        if recents:
            rec_label = QLabel("Recent:")
            rec_label.setStyleSheet(f"color: {config.FG_MID}; padding-top: 8px;")
            rlay.addWidget(rec_label)
            for r in recents[:6]:
                btn = QPushButton(os.path.basename(r) or r)
                btn.setToolTip(r)
                btn.clicked.connect(lambda _=False, p=r: self._add_path(p))
                rlay.addWidget(btn)

        left_panel = QWidget()
        llay = QVBoxLayout(left_panel)
        llay.setContentsMargins(0, 0, 0, 0)
        llay.addLayout(quick)
        llay.addWidget(self._tree, 1)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([580, 300])

        self._recursive_cb = QCheckBox("Include subfolders (scan recursively)")
        self._recursive_cb.setToolTip(
            "Also scan every folder nested inside the selected one(s)")

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Open |
            QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        bottom = QHBoxLayout()
        bottom.addWidget(self._recursive_cb)
        bottom.addStretch(1)
        bottom.addWidget(buttons)

        root = QVBoxLayout(self)
        root.addWidget(splitter, 1)
        root.addLayout(bottom)

        # Keep checked-list in sync with checkbox changes
        self._fs.dataChanged.connect(self._refresh_list)

    # -- internal helpers -----------------------------------------------------
    def _goto(self, path: str) -> None:
        idx = self._fs.index(path)
        if idx.isValid():
            self._tree.expand(idx)
            self._tree.scrollTo(idx, self._tree.ScrollHint.PositionAtTop)
            self._tree.setCurrentIndex(idx)

    def _on_dbl(self, index: QModelIndex) -> None:
        path = self._fs.filePath(index)
        if os.path.isdir(path) or os.path.isfile(path):
            self._add_path(path)

    # -- hover thumbnail preview -----------------------------------------------
    def _on_hover_index(self, index: QModelIndex) -> None:
        path = self._fs.filePath(index)
        if (os.path.isfile(path)
                and os.path.splitext(path.lower())[1] in config.SUPPORTED):
            if path != self._preview_path:
                self._preview_path = path
                self._preview.hide()
                self._preview_timer.start()
        else:
            self._hide_preview()

    def _request_preview(self) -> None:
        if self._preview_path and self._loader is not None:
            self._loader.request(self._preview_path, 160)

    def _on_preview_ready(self, path: str, max_px: int, qim) -> None:
        if path != self._preview_path or max_px != 160 or qim.isNull():
            return
        from PySide6.QtGui import QCursor
        self._preview.setPixmap(QPixmap.fromImage(qim))
        self._preview.adjustSize()
        pos = QCursor.pos()
        self._preview.move(pos.x() + 18, pos.y() + 12)
        self._preview.show()

    def _hide_preview(self) -> None:
        self._preview_path = ""
        self._preview_timer.stop()
        self._preview.hide()

    def eventFilter(self, obj, event):
        from PySide6.QtCore import QEvent
        if (obj is self._tree.viewport()
                and event.type() in (QEvent.Type.Leave, QEvent.Type.Hide)):
            self._hide_preview()
        return super().eventFilter(obj, event)

    def done(self, result: int) -> None:
        self._hide_preview()
        super().done(result)

    def _add_path(self, path: str) -> None:
        self._fs._checked.add(path)
        self._refresh_list()

    def _clear_all(self) -> None:
        self._fs._checked.clear()
        self._refresh_list()
        # Force repaint of checkboxes
        self._fs.dataChanged.emit(
            QModelIndex(), QModelIndex(),
            [Qt.ItemDataRole.CheckStateRole])

    def _refresh_list(self, *_) -> None:
        self._list_model.setStringList(self._fs.checked_all())

    # -- result ---------------------------------------------------------------
    def selected_items(self) -> tuple[list[str], list[str]]:
        """(folders, files) — checked items first; fall back to the tree
        selection if nothing is checked."""
        folders = self._fs.checked_paths()
        files = self._fs.checked_files()
        if folders or files:
            return folders, files
        for idx in self._tree.selectedIndexes():
            if idx.column() != 0:
                continue
            p = self._fs.filePath(idx)
            if os.path.isdir(p) and p not in folders:
                folders.append(p)
            elif os.path.isfile(p) and p not in files:
                files.append(p)
        return folders, files

    def recursive(self) -> bool:
        return self._recursive_cb.isChecked()

from . import config, theme
from .engine import scan, prefs, favorites, cache
from .engine.favorites import Favorites
from .loader import ThumbnailLoader
from .model import GalleryModel
from .gallery_view import GalleryView
from .lightbox import Lightbox
from .multiview import MultiView
from .exif_panel import InfoDialog
from .trash_dialog import TrashDialog
from .help_overlay import make_help_panel, toggle_help_panel


# -- Streaming scan ------------------------------------------------------------

class _StreamSignals(QObject):
    batch = Signal(int, object)   # (gen, list[str])
    done  = Signal(int, object)   # (gen, ScanResult)


class _StreamScanJob(QRunnable):
    """Scan one or more folders off the GUI thread, emitting path batches."""
    BATCH = 200

    def __init__(self, folders: list[str], gen: int, signals: _StreamSignals,
                 recursive: bool = False):
        super().__init__()
        self._folders = list(folders)
        self._gen = gen
        self._signals = signals
        self._recursive = recursive

    def run(self) -> None:
        from .engine.scan import scan_iter, ScanResult
        last_result = None
        for batch, result in scan_iter(self._folders, batch_size=self.BATCH,
                                       recursive=self._recursive):
            last_result = result
            self._signals.batch.emit(self._gen, list(batch))
        from .engine.scan import ScanResult as SR
        self._signals.done.emit(self._gen, last_result or SR())


# -- Dimension pre-fetch -------------------------------------------------------

class _DimsSignals(QObject):
    done = Signal(dict)        # {path: (w, h)}


class _DimsJob(QRunnable):
    """Compute (w, h) for every path off the GUI thread (for dimension sorts)."""
    def __init__(self, paths: list[str], signals: _DimsSignals):
        super().__init__()
        self._paths = list(paths)
        self._signals = signals

    def run(self) -> None:
        from .engine import media
        out: dict[str, tuple[int, int]] = {}
        for p in self._paths:
            try:
                out[p] = media.peek_size(p)
            except Exception:
                out[p] = (0, 0)
        self._signals.done.emit(out)


# -- Main window ---------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gallery")
        self.resize(1280, 900)

        config.ensure_dirs()
        self._prefs = prefs.load_prefs()
        config.apply_theme(self._prefs.get("theme", "dark"))
        self.setStyleSheet(theme.stylesheet())
        # Opt-in housekeeping: drop trashed items older than the configured age.
        purge_days = self._prefs.get("trash_purge_days", 0)
        if isinstance(purge_days, int) and purge_days > 0:
            favorites.purge_older_than(purge_days)
        self._recents = prefs.load_recent()
        self._favs = Favorites()
        # No explicit max_threads -- ThumbnailLoader auto-sizes to CPU count.
        self._loader = ThumbnailLoader(self)
        self._model = GalleryModel(self._favs, self._loader, self)
        self._model.sortChanged.connect(self._sync_sort_combo)
        self._current_folder: str | None = None
        self._current_folders: list[str] = []
        # Stack of trash batches; each batch is a list of (orig, trash) pairs
        # so a single Undo restores all files deleted together.
        self._trash_stack: list[list[tuple[str, str]]] = []

        # Streaming scan infrastructure.
        self._scan_gen = 0
        self._scan_pool = QThreadPool(self)
        self._scan_pool.setMaxThreadCount(2)
        self._stream_sig = _StreamSignals(self)
        self._stream_sig.batch.connect(self._on_scan_batch)
        self._stream_sig.done.connect(self._on_scan_done)

        # Dimensions run on their own small pool (parallel image header reads;
        # video reads still serialise on the cv2 lock).  An inflight guard stops
        # redundant full-folder passes from piling up.
        self._dims_pool = QThreadPool(self)
        self._dims_pool.setMaxThreadCount(min(4, max(1, (os.cpu_count() or 2))))
        self._dims_sig = _DimsSignals(self)
        self._dims_sig.done.connect(self._on_dims_chunk)
        self._dims_done_for: set[str] = set()
        self._dims_inflight = False
        self._dims_remaining = 0

        self._build_ui()
        self._restore_session()

        self._autoscroll = False
        self._autoscroll_timer = QTimer(self)
        self._autoscroll_timer.setInterval(20)
        self._autoscroll_timer.timeout.connect(self._autoscroll_tick)

        # Trim the on-disk thumbnail cache to its byte budget shortly after
        # startup (deferred so it never delays the window appearing).
        QTimer.singleShot(1500, cache.enforce_cap)

    # -- UI --------------------------------------------------------------------
    def _build_ui(self) -> None:
        central = QWidget()
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._view = GalleryView()
        self._view.setModel(self._model)
        self._view.set_columns(config.DEFAULT_COLS)
        self._view.openLightbox.connect(self._open_lightbox)
        self._view.favToggled.connect(self._on_grid_fav)
        self._view.rotateItem.connect(self._on_grid_rotate)
        self._view.trashItem.connect(self._on_grid_trash)
        self._view.favBatch.connect(self._on_grid_fav_batch)
        self._view.trashBatch.connect(self._on_grid_trash_batch)
        self._view.selectionChanged.connect(self._on_selection_changed)
        # Ctrl+wheel zoom routes through the spin box so UI and view stay in sync.
        self._view.columnsZoom.connect(
            lambda step: self._cols_spin.setValue(self._cols_spin.value() + step))

        # QStackedWidget: page 0 = gallery, page 1 = embedded multiview panel.
        self._content_stack = QStackedWidget()
        self._content_stack.addWidget(self._view)
        self._mv: "MultiView | None" = None   # created lazily on first open
        root.addWidget(self._content_stack, 1)

        # -- Floating control bar ---------------------------------------------
        self._bar = QFrame(central)
        self._bar.setObjectName("OverlayBar")
        self._apply_bar_style()
        h = QHBoxLayout(self._bar)
        h.setContentsMargins(8, 4, 8, 4)
        h.setSpacing(4)

        h.addWidget(self._btn(f"{config.ICON_FOLDER} Open", self._pick_folders))
        self._recent_btn = self._btn("Recent \u25be", None)
        self._recent_menu = QMenu(self._recent_btn)
        self._recent_btn.setMenu(self._recent_menu)
        self._recent_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self._rebuild_recent_menu()
        h.addWidget(self._recent_btn)
        self._trash_btn = self._btn(f"{config.ICON_TRASH} Trash",
                                    self._open_trash)
        self._trash_btn.setToolTip("Browse, restore, or empty trashed items")
        h.addWidget(self._trash_btn)
        h.addWidget(self._sep())

        h.addWidget(QLabel("cols"))
        self._cols_spin = QSpinBox()
        self._cols_spin.setRange(config.MIN_COLS, config.MAX_COLS)
        self._cols_spin.setValue(config.DEFAULT_COLS)
        self._cols_spin.valueChanged.connect(self._on_cols)
        h.addWidget(self._cols_spin)

        h.addWidget(QLabel("sort"))
        self._sort = QComboBox()
        self._sort.addItem("Dimensions \u00b7 like sizes", "like_dims")
        self._sort.addItem("Name", "name")
        self._sort.addItem("Images first", "img_first")
        self._sort.addItem("Videos first", "vid_first")
        self._sort.addItem("Favourites", "favorites")
        self._sort.addItem("Dimensions \u00b7 area", "area")
        self._sort.addItem("Dimensions \u00b7 width", "width")
        self._sort.addItem("Dimensions \u00b7 height", "height")
        self._sort.addItem("Manual order", "manual")
        self._sort.currentIndexChanged.connect(lambda _: self._on_sort_changed())
        h.addWidget(self._sort)
        self._dir_btn = self._btn("\u2191", self._toggle_sort_dir)
        self._dir_btn.setToolTip("Ascending \u2014 click for descending")
        h.addWidget(self._dir_btn)

        h.addWidget(self._sep())
        self._img_btn = self._btn("Images", self._apply_filter, checkable=True)
        self._img_btn.setChecked(True)
        self._vid_btn = self._btn("Videos", self._apply_filter, checkable=True)
        self._vid_btn.setChecked(True)
        self._favs_btn = self._btn(f"{config.ICON_HEART_FULL} Favs",
                                   self._apply_filter, checkable=True)
        self._favs_btn.setToolTip("Show only favourites")
        h.addWidget(self._img_btn); h.addWidget(self._vid_btn)
        h.addWidget(self._favs_btn)

        self._search = QLineEdit()
        self._search.setPlaceholderText("search\u2026")
        self._search.setFixedWidth(150)
        # Debounce: each keystroke re-runs the filter, and set_filter does a full
        # sort + model reset \u2014 coalesce rapid typing into one pass every 200 ms.
        self._search_timer = QTimer(self)
        self._search_timer.setSingleShot(True)
        self._search_timer.timeout.connect(self._apply_filter)
        self._search.textChanged.connect(lambda _: self._search_timer.start(200))
        h.addWidget(self._search)

        h.addWidget(self._sep())
        h.addWidget(self._btn(f"{config.ICON_GRID} Multi-view",
                              lambda: self._open_multiview(0)))
        self._scroll_btn = self._btn(f"{config.ICON_PLAY} Scroll",
                                     self._toggle_autoscroll, checkable=True)
        h.addWidget(self._scroll_btn)
        h.addStretch(1)
        self._status = QLabel("Open a folder to begin")
        self._status.setObjectName("StatusBar")
        h.addWidget(self._status)

        self._theme_btn = self._btn("Theme ▾", None)
        self._theme_btn.setToolTip("Switch colour theme")
        self._theme_menu = QMenu(self._theme_btn)
        for name in config.THEMES:
            act = self._theme_menu.addAction(config.theme_label(name))
            act.setCheckable(True)
            act.setData(name)
            act.setChecked(name == config.theme_name())
            act.triggered.connect(lambda _=False, n=name: self._set_theme(n))
        self._theme_btn.setMenu(self._theme_menu)
        self._theme_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        h.addWidget(self._theme_btn)

        h.addWidget(self._btn(config.ICON_FULLSCREEN, self._toggle_fs))

        # -- Floating undo bar ------------------------------------------------
        self._undo_bar = QFrame(central)
        self._undo_bar.setStyleSheet(
            "QFrame { background: rgba(26,8,8,235); border-radius: 8px; }")
        ub = QHBoxLayout(self._undo_bar)
        ub.setContentsMargins(12, 6, 12, 6)
        self._undo_label = QLabel("")
        self._undo_label.setStyleSheet(
            f"color:{config.RED_BRIGHT}; background: transparent;")
        ub.addWidget(self._undo_label)
        ub.addStretch(1)
        undo_btn = QPushButton("Undo")
        undo_btn.clicked.connect(self._undo_trash)
        ub.addWidget(undo_btn)
        self._undo_bar.hide()
        self._undo_timer = QTimer(self)
        self._undo_timer.setSingleShot(True)
        self._undo_timer.timeout.connect(self._undo_bar.hide)

        self.setCentralWidget(central)

        self._bar_hide_timer = QTimer(self)
        self._bar_hide_timer.setSingleShot(True)
        self._bar_hide_timer.timeout.connect(self._maybe_hide_bar)
        self._view.viewport().installEventFilter(self)
        self._bar.installEventFilter(self)
        self._bar.show()
        self._bar.raise_()

        QShortcut(QKeySequence(Qt.Key.Key_F11), self, activated=self._toggle_fs)
        QShortcut(QKeySequence("Ctrl+O"), self, activated=self._pick_folders)
        QShortcut(QKeySequence(Qt.Key.Key_Space), self,
                  activated=self._toggle_autoscroll)
        QShortcut(QKeySequence(Qt.Key.Key_H), self, activated=self._toggle_bar)

        # Keyboard-shortcuts help (gallery page); MultiView has its own.
        self._help = make_help_panel(central, [
            ("Ctrl+O", "Open folder(s)"),
            ("Ctrl+wheel", "Zoom grid (columns)"),
            ("Click / Ctrl / Shift", "Select · toggle · range-select"),
            ("Ctrl+A  /  Esc", "Select all / clear selection"),
            ("F", "Favourite selection"),
            ("Delete", "Trash selection (undoable)"),
            ("← → ↑ ↓", "Move cursor (Shift extends)"),
            ("Enter / double-click", "Open in viewer"),
            ("Space", "Auto-scroll the grid"),
            ("H", "Show / hide the control bar"),
            ("F11", "Toggle full screen"),
            ("?  /  F1", "Show / hide this help"),
        ])
        QShortcut(QKeySequence(Qt.Key.Key_Question), self,
                  activated=self._toggle_help)
        QShortcut(QKeySequence(Qt.Key.Key_F1), self,
                  activated=self._toggle_help)

    def _toggle_help(self) -> None:
        # Route to the active panel: the multiview overlay covers its own keys.
        if self._mv is not None and self._content_stack.currentWidget() is self._mv:
            self._mv.toggle_help()
        else:
            toggle_help_panel(self._help, self.centralWidget())

    # -- theming ---------------------------------------------------------------
    def _apply_bar_style(self) -> None:
        self._bar.setStyleSheet(
            f"QFrame#OverlayBar {{ background: {config.OVERLAY_BAR_BG};"
            " border-radius: 9px; }")

    def _set_theme(self, name: str) -> None:
        config.apply_theme(name)
        self.setStyleSheet(theme.stylesheet())
        self._apply_bar_style()
        self._view.apply_theme()
        for act in self._theme_menu.actions():
            act.setChecked(act.data() == config.theme_name())
        self._position_overlays()
        self._prefs["theme"] = config.theme_name()
        prefs.save_prefs(self._prefs)

    # -- Floating-bar overlay management ---------------------------------------
    def _position_overlays(self) -> None:
        c = self.centralWidget()
        if c is None:
            return
        cw, ch = c.width(), c.height()
        bh = self._bar.sizeHint().height()
        self._bar.setGeometry(8, 8, cw - 16, bh)
        if self._undo_bar.isVisible():
            uh = self._undo_bar.sizeHint().height()
            self._undo_bar.setGeometry(8, ch - uh - 8, cw - 16, uh)
        if self._help.isVisible():
            self._help.move(max(0, (cw - self._help.width()) // 2),
                            max(0, (ch - self._help.height()) // 2))
            self._help.raise_()

    def _show_bar(self) -> None:
        self._bar_hide_timer.stop()
        self._position_overlays()
        self._bar.show()
        self._bar.raise_()

    def _toggle_bar(self) -> None:
        # Single owner of the H shortcut: dispatch to whichever panel is
        # active (a second window-context H in MultiView would make the key
        # ambiguous and Qt would fire neither handler).
        if self._mv is not None and self._content_stack.currentWidget() is self._mv:
            self._mv._toggle_bars()
            return
        self._show_bar() if not self._bar.isVisible() else self._bar.hide()

    def _bar_should_stay(self) -> bool:
        if self._model.rowCount() == 0:
            return True
        if self._bar.underMouse():
            return True
        if QApplication.activePopupWidget() is not None:
            return True
        if self._sort.view().isVisible():
            return True
        if self._search.hasFocus():
            return True
        return False

    def _maybe_hide_bar(self) -> None:
        if self._bar_should_stay():
            self._bar_hide_timer.start(BAR_HIDE_MS)
            return
        self._bar.hide()

    def eventFilter(self, obj, event):
        from PySide6.QtCore import QEvent
        et = event.type()
        if obj is self._view.viewport() and et == QEvent.Type.MouseMove:
            try:
                y = event.position().y()
            except Exception:
                y = 0
            if y <= self._bar.height() + 40:
                self._show_bar()
            elif self._bar.isVisible():
                self._bar_hide_timer.start(BAR_HIDE_MS)
        elif obj is self._bar:
            if et == QEvent.Type.Enter:
                self._bar_hide_timer.stop()
            elif et == QEvent.Type.Leave:
                self._bar_hide_timer.start(BAR_HIDE_MS)
        return super().eventFilter(obj, event)

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._position_overlays()

    def showEvent(self, e):
        super().showEvent(e)
        self._position_overlays()

    def _btn(self, text, cb, checkable=False) -> QToolButton:
        b = QToolButton()
        b.setText(text)
        b.setCheckable(checkable)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        if cb is not None:
            b.clicked.connect(cb)
        return b

    def _sep(self) -> QFrame:
        f = QFrame(); f.setFrameShape(QFrame.Shape.VLine)
        f.setStyleSheet("color:#262626;")
        return f

    # -- folder / scan ---------------------------------------------------------
    def _pick_folders(self) -> None:
        """Folder & file chooser with checkboxes, hover previews, quick access."""
        dlg = _FolderPickDlg(self, self._recents, loader=self._loader)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            folders, files = dlg.selected_items()
            if folders or files:
                self.open_media(folders, files, recursive=dlg.recursive())


    def open_folder(self, folder: str) -> None:
        self.open_folders([folder])

    def open_folders(self, folders: list[str], recursive: bool = False) -> None:
        self.open_media(folders, [], recursive=recursive)

    def open_media(self, folders: list[str], files: list[str],
                   recursive: bool = False) -> None:
        """Open a mix of folders (scanned) and individually chosen files."""
        folders = [f for f in folders if os.path.isdir(f)]
        files = [f for f in files if os.path.isfile(f)
                 and os.path.splitext(f.lower())[1] in config.SUPPORTED]
        if not folders and not files:
            return
        self._current_folders = folders
        self._current_folder = folders[0] if folders else None
        if len(folders) == 1 and not files:
            self.setWindowTitle(
                f"Gallery \u2014 {os.path.basename(os.path.normpath(folders[0]))}")
        elif folders:
            self.setWindowTitle(f"Gallery \u2014 {len(folders)} folder(s)"
                                + (f" + {len(files)} file(s)" if files else ""))
        else:
            self.setWindowTitle(f"Gallery \u2014 {len(files)} file(s)")
        for f in folders:
            self._recents = [f] + [r for r in self._recents if r != f]
        self._recents = self._recents[:10]
        prefs.save_recent(self._recents)
        self._rebuild_recent_menu()

        # Bump generation to discard batches from any in-progress scan.
        self._scan_gen += 1
        self._model.set_paths([])    # clear immediately
        scanning = (os.path.basename(os.path.normpath(folders[0]))
                    if len(folders) == 1 else f"{len(folders)} folders")
        if folders:
            self._status.setText(f"Scanning {len(folders)} folder(s)\u2026")
            self._view.set_empty_hint(f"Scanning {scanning}\u2026",
                                      "Media appears once dimensions are read")
        # Hand-picked files go through the same batch path as a scan (dedup
        # against later folder hits happens in the model).
        if files:
            self._on_scan_batch(self._scan_gen, files)
        if folders:
            self._scan_pool.start(
                _StreamScanJob(folders, self._scan_gen, self._stream_sig,
                               recursive=recursive))
        else:
            # Files only: no scan job will fire done \u2014 synthesise the result.
            from .engine.scan import ScanResult
            res = ScanResult()
            res.paths = list(files)
            res.total = len(files)
            self._on_scan_done(self._scan_gen, res)

    def _on_scan_batch(self, gen: int, paths: list) -> None:
        if gen != self._scan_gen:
            return
        if self._model.needs_dimensions():
            # Buffer silently — view stays empty until dims arrive and sort can
            # be applied correctly, avoiding the mid-scan reorder flash.
            self._model.add_paths_silent(paths)
            n = len(self._model.all_paths())
        else:
            self._model.add_paths_batch(paths)
            n = self._model.rowCount()
        self._status.setText(f"Scanning\u2026  {n} found")

    def _on_scan_done(self, gen: int, result) -> None:
        if gen != self._scan_gen:
            return
        self._dims_done_for.clear()
        if not self._model.all_paths():
            self._view.set_empty_hint(
                "No media found in this folder",
                "Try another folder, or check the Images / Videos filters")
        if not self._model.needs_dimensions():
            # Non-dimension sort: reveal everything now, then compute dims for
            # the masonry aspect-ratio reflow.
            self._model.finalize_scan()
            self._status.setText(result.summary())
        else:
            # Dimension sort: paths are staged in _all via add_paths_silent.
            # _on_dims_done -> set_dims -> _reindex() will reveal them sorted once
            # dimensions are known — no unsorted flash.
            self._status.setText(
                f"Computing dimensions\u2026  {len(self._model.all_paths())} items")
        self._ensure_dims()
        self._bar_hide_timer.start(BAR_HIDE_MS)

    def _rebuild_recent_menu(self) -> None:
        self._recent_menu.clear()
        if not self._recents:
            act = self._recent_menu.addAction("(no recent folders)")
            act.setEnabled(False)
            return
        for folder in self._recents:
            act = QAction(folder, self._recent_menu)
            act.triggered.connect(lambda _=False, f=folder: self.open_folder(f))
            self._recent_menu.addAction(act)

    # -- filters / columns -----------------------------------------------------
    def _apply_filter(self) -> None:
        self._model.set_filter(self._img_btn.isChecked(),
                               self._vid_btn.isChecked(),
                               self._search.text(),
                               self._favs_btn.isChecked())
        # If a folder is loaded but the filter hides everything, explain why
        # the grid is blank rather than leaving a bare black screen.
        if self._model.all_paths() and self._model.rowCount() == 0:
            if self._favs_btn.isChecked():
                self._view.set_empty_hint(
                    "No favourites here",
                    "Tap the heart on items, or turn off the Favs filter")
            else:
                self._view.set_empty_hint(
                    "Nothing matches the current filter",
                    "Adjust the Images / Videos toggles or clear the search box")

    def _on_cols(self, n: int) -> None:
        self._view.set_columns(n)

    # -- sorting ---------------------------------------------------------------
    def _on_sort_changed(self) -> None:
        self._model.set_sort(self._sort.currentData())
        if self._model.needs_dimensions():
            self._ensure_dims()

    def _sync_sort_combo(self, mode: str) -> None:
        """Reflect a model-driven sort change (e.g. drag-reorder -> manual)
        in the combo without re-triggering _on_sort_changed."""
        i = self._sort.findData(mode)
        if i >= 0 and self._sort.currentIndex() != i:
            self._sort.blockSignals(True)
            self._sort.setCurrentIndex(i)
            self._sort.blockSignals(False)

    def _toggle_sort_dir(self) -> None:
        desc = not self._model.descending()
        self._model.set_descending(desc)
        self._dir_btn.setText("\u2193" if desc else "\u2191")
        self._dir_btn.setToolTip("Descending \u2014 click for ascending" if desc
                                 else "Ascending \u2014 click for descending")

    def _ensure_dims(self) -> None:
        # One pass at a time \u2014 a second call while a pass runs would re-scan the
        # whole folder and saturate the cv2 lock. _on_dims_chunk re-checks for
        # newly-added paths when the current pass finishes.
        if self._dims_inflight:
            return
        from .engine import media as _media
        paths = [p for p in self._model.all_paths()
                 if p not in self._dims_done_for]
        if not paths:
            return
        self._dims_inflight = True
        # Images and videos are separated: image header reads are fast and run
        # as parallel chunks; video probes serialise on the cv2 lock anyway, so
        # they go in ONE trailing job.  Each job's results apply as they land
        # (_on_dims_chunk), so the gallery reveals \u2014 images sorted, videos
        # provisionally at the end \u2014 within seconds instead of staying blank
        # until the last video has been probed.
        images = [p for p in paths if not _media.is_video(p)]
        videos = [p for p in paths if _media.is_video(p)]
        jobs: list[list[str]] = []
        if images:
            n = min(self._dims_pool.maxThreadCount(),
                    max(1, len(images) // 400 + 1))
            jobs.extend(c for c in (images[i::n] for i in range(n)) if c)
        if videos:
            jobs.append(videos)
        self._dims_remaining = len(jobs)
        self._status.setText("Computing dimensions\u2026")
        for c in jobs:
            self._dims_pool.start(_DimsJob(c, self._dims_sig))

    def _on_dims_chunk(self, dims: dict) -> None:
        # Apply each job's results immediately \u2014 progressive reveal.  At most a
        # handful of jobs run per pass, so the extra re-sorts are cheap next to
        # minutes of blank grid on a video-heavy folder.
        self._dims_done_for.update(dims.keys())
        if dims:
            self._model.set_dims(dims)
        self._dims_remaining -= 1
        if self._dims_remaining > 0:
            return
        self._dims_inflight = False
        if self._model.needs_dimensions():
            self._status.setText(
                f"Sorted by {self._sort.currentText().lower()}")
        # Paths added during the pass (streaming scan) still need measuring.
        if any(p not in self._dims_done_for for p in self._model.all_paths()):
            self._ensure_dims()

    # -- favourites / rotate / trash -------------------------------------------
    def _on_grid_fav(self, row: int) -> None:
        path = self._model.path_at(row)
        if not path:
            return
        new = self._favs.toggle(path)
        self._model.refresh_fav(path)
        self._view.refresh_overlay_fav(row, new)
        if self._favs_btn.isChecked():
            self._apply_filter()    # un-favourited item leaves the filtered view

    def _toggle_fav_path(self, path: str) -> None:
        self._favs.toggle(path)
        self._model.refresh_fav(path)
        if self._favs_btn.isChecked():
            self._apply_filter()

    def _on_grid_rotate(self, row: int) -> None:
        path = self._model.path_at(row)
        if path:
            self._rotate_path(path, 90)

    def _rotate_path(self, path: str, degrees: int) -> bool:
        """Permanently rotate an image file on disk and refresh every view.

        Returns True on success so the calling viewer can reload its own
        full-resolution display.  Videos are not supported.
        """
        from .engine import media as _media
        if _media.is_video(path):
            self._status.setText("Rotating video files isn't supported")
            return False
        if not _media.rotate_image_file(path, degrees):
            self._status.setText(f"Couldn't rotate {os.path.basename(path)}")
            return False
        # Invalidate cached pixels and the masonry aspect ratio, then push the
        # rotated file's true dimensions so the grid reflows immediately.
        self._model.reload_path(path)
        self._view.forget_path_dims(path)
        w, h = _media.peek_size(path)
        if w > 0 and h > 0:
            self._dims_done_for.add(path)
            self._model.set_dims({path: (w, h)})
        # Keep a favourite's Downloads mirror in sync with the rotated original.
        self._favs.resync_mirror(path)
        self._status.setText(f"Rotated {os.path.basename(path)}")
        return True

    def _on_grid_trash(self, row: int) -> None:
        path = self._model.path_at(row)
        if path:
            self._trash_path(path)

    # -- batch actions on a multi-selection ------------------------------------
    def _on_grid_fav_batch(self, rows: list) -> None:
        """Favourite all selected items if any is unfavourited, else clear all."""
        paths = [p for p in (self._model.path_at(r) for r in rows) if p]
        if not paths:
            return
        make_fav = any(not self._favs.is_fav(p) for p in paths)
        for p in paths:
            if self._favs.is_fav(p) != make_fav:
                self._favs.toggle(p)
                self._model.refresh_fav(p)
        verb = "Favourited" if make_fav else "Unfavourited"
        self._status.setText(f"{verb} {len(paths)} item(s)")
        if self._favs_btn.isChecked():
            self._apply_filter()

    def _on_grid_trash_batch(self, rows: list) -> None:
        # Resolve paths up front — remove_path() shifts row indices as we go.
        paths = [p for p in (self._model.path_at(r) for r in rows) if p]
        if paths:
            self._view.clear_selection()
            self._trash_paths(paths)

    def _on_selection_changed(self, count: int) -> None:
        self._status.setText(f"{count} selected" if count else "")

    def _trash_path(self, path: str) -> None:
        self._trash_paths([path])

    def _trash_paths(self, paths: list[str]) -> None:
        """Trash one or more files as a single undoable batch."""
        batch: list[tuple[str, str]] = []
        for path in paths:
            # Release every open handle on the file first — multiview tiles
            # (including duplicates and the side-scroll buffer) and the
            # gallery's video-preview players each hold the file open, which
            # blocks moving a video to the trash on Windows.
            if self._mv is not None:
                self._mv.release_path(path)
            self._view.release_video(path)
            dest = favorites.trash_file(path)
            if not dest:
                continue
            self._favs.discard(path)
            batch.append((path, dest))
        if not batch:
            self._status.setText("Delete failed")
            return
        # One model update for the whole batch (remove_path per item is O(n·k)).
        self._model.remove_paths([p for p, _ in batch])
        self._trash_stack.append(batch)
        if len(batch) == 1:
            self._undo_label.setText(
                f"{config.ICON_TRASH}  Deleted {os.path.basename(batch[0][0])}")
        else:
            self._undo_label.setText(
                f"{config.ICON_TRASH}  Deleted {len(batch)} items")
        self._undo_bar.show()
        self._undo_bar.raise_()
        self._position_overlays()
        self._undo_timer.start(6000)

    def _undo_trash(self) -> None:
        self._undo_bar.hide()
        if not self._trash_stack:
            return
        batch = self._trash_stack.pop()
        restored = 0
        for orig, trash in batch:
            if favorites.restore_file(orig, trash):
                self._model.add_path(orig)
                restored += 1
        if restored == 1:
            self._status.setText(f"Restored {os.path.basename(batch[0][0])}")
        elif restored:
            self._status.setText(f"Restored {restored} items")

    # -- trash management ------------------------------------------------------
    def _open_trash(self) -> None:
        days = self._prefs.get("trash_purge_days", 0)
        dlg = TrashDialog(self, days if isinstance(days, int) else 0)
        dlg.exec()
        if dlg.auto_purge_days != days:
            self._prefs["trash_purge_days"] = dlg.auto_purge_days
            prefs.save_prefs(self._prefs)

    def _rotate_from_lightbox(self, lb, degrees: int) -> None:
        path = lb.current_path()
        if path and self._rotate_path(path, degrees):
            lb.reload_current()

    # -- lightbox / multiview --------------------------------------------------
    def _open_lightbox(self, row: int) -> None:
        lb = Lightbox(self._model, self._favs, self)
        lb.favToggled.connect(self._toggle_fav_path)
        lb.trashed.connect(self._trash_path)
        lb.requestInfo.connect(lambda p: InfoDialog(p, self).exec())
        lb.rotateRequested.connect(
            lambda deg, _lb=lb: self._rotate_from_lightbox(_lb, deg))
        # Close the lightbox before switching to the embedded multiview panel.
        lb.openMulti.connect(lambda r, _lb=lb: (_lb.close(), self._open_multiview(r)))
        lb.showFullScreen()
        lb.raise_()
        lb.activateWindow()
        lb.show_row(row)

    def _open_multiview(self, start_row: int) -> None:
        if self._mv is None:
            self._mv = MultiView(self._model, self._favs, self)
            self._mv.favToggled.connect(self._toggle_fav_path)
            self._mv.trashed.connect(self._trash_path)
            self._mv.rotated.connect(self._rotate_from_multiview)
            self._mv.openLightbox.connect(self._open_lightbox)
            self._mv.closeRequested.connect(self._close_multiview)
            self._content_stack.addWidget(self._mv)
        # Kick off dims computation so orientation lists are accurate.
        self._ensure_dims()
        self._mv.open(start_row)
        self._content_stack.setCurrentWidget(self._mv)
        self._bar.hide()

    def _rotate_from_multiview(self, path: str) -> None:
        if self._rotate_path(path, 90) and self._mv is not None:
            self._mv.reload_path(path)

    def _close_multiview(self) -> None:
        if self._mv is not None:
            self._mv.stop_autoscroll()
            # Stop hidden playback and release file handles — otherwise tiles
            # kept decoding their videos behind the gallery, and any file last
            # shown in multi-view stayed locked against deletion.
            self._mv.release_all_media()
        self._content_stack.setCurrentWidget(self._view)
        self._bar.show()
        self._position_overlays()

    # -- autoscroll / fullscreen -----------------------------------------------
    def _toggle_autoscroll(self) -> None:
        self._autoscroll = not self._autoscroll
        self._scroll_btn.setChecked(self._autoscroll)
        if self._autoscroll:
            self._autoscroll_timer.start()
        else:
            self._autoscroll_timer.stop()

    def _autoscroll_tick(self) -> None:
        sb = self._view.verticalScrollBar()
        if sb.value() >= sb.maximum():
            self._toggle_autoscroll()
            return
        sb.setValue(sb.value() + 2)

    def _toggle_fs(self) -> None:
        if self.isFullScreen():
            self.showNormal()
        else:
            self.showFullScreen()
            self.raise_()
            self.activateWindow()

    # -- session persistence ---------------------------------------------------
    def _restore_session(self) -> None:
        g = self._prefs.get("geometry")
        if isinstance(g, list) and len(g) == 4:
            self.setGeometry(*g)
        cols = self._prefs.get("cols")
        if isinstance(cols, int) and config.MIN_COLS <= cols <= config.MAX_COLS:
            self._cols_spin.setValue(cols)
        if self._prefs.get("sort_desc"):
            self._model.set_descending(True)
            self._dir_btn.setText("\u2193")
            self._dir_btn.setToolTip("Descending \u2014 click for ascending")

    def closeEvent(self, e) -> None:
        try:
            self._loader.clear()
            self._autoscroll_timer.stop()
            if not self.isFullScreen():
                geo = self.geometry()
                self._prefs["geometry"] = [
                    geo.x(), geo.y(), geo.width(), geo.height()]
            self._prefs["cols"] = self._cols_spin.value()
            self._prefs["sort_desc"] = self._model.descending()
            prefs.save_prefs(self._prefs)
        except Exception:
            pass
        super().closeEvent(e)

    def clear_cache(self) -> int:
        return cache.clear()
