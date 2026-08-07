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

# _CheckFSModel now lives in fs_picker so the album-tagging dialog can
# reuse it without importing this module.  Re-exported here because
# existing callers/tests address it as main_window._CheckFSModel.
from .fs_picker import (_CheckFSModel, HoverPreview,  # noqa: E402
                        quick_access_row)


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
        # QFileSystemModel populates on a gatherer thread, so a jump to a folder
        # that hasn't been visited yet has to wait for it to load.
        self._pending_goto = ""
        self._fs.directoryLoaded.connect(self._on_dir_loaded)

        # Tree view
        self._tree = QTreeView()
        self._tree.setModel(self._fs)
        # ExtendedSelection already gives the tree click-drag range selection.
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
        self._hover = HoverPreview(self, self._tree, self._fs, self._loader)

        # Quick access: common media locations, one click to jump the tree,
        # plus the media-type filter for what the tree shows/imports.
        quick, self._type_btn = quick_access_row(self._fs, self._goto)

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
        """Jump the tree to `path`, waiting for the model if it isn't loaded yet.

        QFileSystemModel populates on a gatherer thread, so index() is invalid
        until the parent directory has been visited — jumping on a cold path
        (e.g. a Downloads folder never expanded in the tree) would otherwise
        silently do nothing.
        """
        idx = self._fs.index(path)
        if idx.isValid():
            self._tree.expand(idx)
            self._tree.scrollTo(idx, self._tree.ScrollHint.PositionAtTop)
            self._tree.setCurrentIndex(idx)
            return
        self._pending_goto = path
        self._fs.setRootPath(os.path.dirname(path) or path)

    def _on_dir_loaded(self, _loaded: str) -> None:
        want = getattr(self, "_pending_goto", "")
        if want and self._fs.index(want).isValid():
            self._pending_goto = ""
            self._goto(want)

    def _on_dbl(self, index: QModelIndex) -> None:
        path = self._fs.filePath(index)
        if os.path.isdir(path) or os.path.isfile(path):
            self._add_path(path)

    # -- hover thumbnail preview (delegated to fs_picker.HoverPreview) ---------
    def _hide_preview(self) -> None:
        self._hover.hide()

    def done(self, result: int) -> None:
        self._hover.hide()
        super().done(result)

    def _add_path(self, path: str) -> None:
        self._fs.add(path)
        self._refresh_list()

    def _clear_all(self) -> None:
        self._fs.clear()
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


class _FnJob(QRunnable):
    """Run an arbitrary callable on a pool thread (fire-and-forget)."""
    def __init__(self, fn):
        super().__init__()
        self._fn = fn

    def run(self) -> None:
        try:
            self._fn()
        except Exception:
            pass


class _ProbeSignals(QObject):
    done = Signal(int, float)     # (scan generation, avg ms per sample read)


class _IoProbeJob(QRunnable):
    """Measure the media drive's small-read latency on a worker.

    Drive-type APIs can't reliably identify an external SSD (most USB
    enclosures report as fixed disks), so we measure instead: read a small
    block from a few sample files.  Internal SSDs come in well under a
    millisecond per read; USB enclosures, hubs, HDDs, network shares and
    drives waking from autosuspend come in far above the threshold.
    """
    SAMPLE_BYTES = 32 * 1024

    def __init__(self, gen: int, paths: list[str], signals: _ProbeSignals):
        super().__init__()
        self._gen = gen
        self._paths = list(paths)
        self._signals = signals

    def run(self) -> None:
        import time as _time
        times: list[float] = []
        for p in self._paths[:4]:
            try:
                t0 = _time.perf_counter()
                with open(p, "rb") as f:
                    f.read(self.SAMPLE_BYTES)
                times.append((_time.perf_counter() - t0) * 1000.0)
            except OSError:
                continue
        avg = (sum(times) / len(times)) if times else 0.0
        self._signals.done.emit(self._gen, avg)


class _RotateSignals(QObject):
    done = Signal(str, int, bool, int, int)   # path, degrees, ok, new w, new h


class _RotateJob(QRunnable):
    """Permanent rotation on a worker: full decode + re-encode + write."""
    def __init__(self, path: str, degrees: int, signals: _RotateSignals):
        super().__init__()
        self._path = path
        self._degrees = degrees
        self._signals = signals

    def run(self) -> None:
        from .engine import media as _media
        ok = False
        w = h = 0
        try:
            ok = _media.rotate_image_file(self._path, self._degrees)
            if ok:
                w, h = _media.peek_size(self._path)
        except Exception:
            ok = False
        self._signals.done.emit(self._path, self._degrees, ok, w, h)


# -- Background trash (cross-device moves copy the whole file) ------------------

class _TrashSignals(QObject):
    done = Signal(list, list)     # (moved (orig, dest) pairs, failed paths)


class _TrashJob(QRunnable):
    """Move files to the trash off the GUI thread.

    Same-device deletes are instant renames and stay synchronous, but when the
    media lives on a different drive than the trash dir, shutil.move copies
    the entire file — deleting one large video froze the UI for seconds.
    """
    def __init__(self, paths: list[str], signals: _TrashSignals):
        super().__init__()
        self._paths = list(paths)
        self._signals = signals

    def run(self) -> None:
        moved: list[tuple[str, str]] = []
        failed: list[str] = []
        for p in self._paths:
            dest = favorites.trash_file(p)
            if dest:
                moved.append((p, dest))
            else:
                failed.append(p)
        self._signals.done.emit(moved, failed)


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
        # How favourites / tag folders place files: copy (default) or
        # hardlink / symlink to save disk space. Set via the Settings dialog.
        favorites.set_link_mode(self._prefs.get("link_mode", "copy"))
        self.setStyleSheet(theme.stylesheet())
        self._recents = prefs.load_recent()
        self._favs = Favorites()
        # No explicit max_threads -- ThumbnailLoader auto-sizes to CPU count.
        self._loader = ThumbnailLoader(self)
        self._model = GalleryModel(self._favs, self._loader, self)
        self._model.sortChanged.connect(self._sync_sort_combo)
        self._current_folder: str | None = None
        self._current_folders: list[str] = []
        self._lightbox_count = 0     # open viewer windows (previews pause)
        # Stack of trash batches; each batch is a list of (orig, trash) pairs
        # so a single Undo restores all files deleted together.
        self._trash_stack: list[list[tuple[str, str]]] = []

        # Streaming scan infrastructure.
        self._scan_gen = 0
        self._scan_pool = QThreadPool(self)
        self._scan_pool.setMaxThreadCount(2)
        self._trash_sig = _TrashSignals(self)
        self._trash_sig.done.connect(self._on_trash_moved)
        self._rotate_sig = _RotateSignals(self)
        self._rotate_sig.done.connect(self._on_rotate_done)
        self._rotating: set[str] = set()

        # Slow-storage throttle: probe read latency per folder open and cut
        # parallel reads when media lives on slow/external storage.  A
        # "low_io_mode": true/false entry in the prefs file forces the mode
        # and skips probing.
        self._probe_sig = _ProbeSignals(self)
        self._probe_sig.done.connect(self._on_io_probe)
        self._probed_gen = -1
        self._low_io = False
        li = self._prefs.get("low_io_mode")
        self._low_io_forced = isinstance(li, bool)
        if self._low_io_forced and li:
            QTimer.singleShot(0, lambda: self._set_low_io(True, forced=True))
        self._stream_sig = _StreamSignals(self)
        self._stream_sig.batch.connect(self._on_scan_batch)
        self._stream_sig.done.connect(self._on_scan_done)

        # Dimensions run on their own small pool (parallel image header reads;
        # video reads still serialise on the cv2 lock).  An inflight guard stops
        # redundant full-folder passes from piling up.
        self._dims_pool = QThreadPool(self)
        self._dims_default = min(4, max(1, (os.cpu_count() or 2)))
        self._dims_pool.setMaxThreadCount(self._dims_default)
        self._dims_sig = _DimsSignals(self)
        self._dims_sig.done.connect(self._on_dims_chunk)
        self._dims_done_for: set[str] = set()
        self._dims_inflight = False
        self._dims_remaining = 0
        # Coalesce chunk results: every set_dims() re-sorts and resets the
        # model (and re-lays the grid), so applying 20+ video chunks
        # individually produced a storm of 100-300 ms GUI stalls.
        self._dims_apply: dict[str, tuple[int, int]] = {}
        self._dims_apply_timer = QTimer(self)
        self._dims_apply_timer.setSingleShot(True)
        self._dims_apply_timer.setInterval(350)
        self._dims_apply_timer.timeout.connect(self._apply_dims_batch)

        self._build_ui()
        self._restore_session()

        self._autoscroll = False
        self._autoscroll_timer = QTimer(self)
        self._autoscroll_timer.setInterval(20)
        self._autoscroll_timer.timeout.connect(self._autoscroll_tick)

        # Housekeeping (thumbnail-cache trim + opt-in trash auto-purge) runs on
        # a worker shortly after startup: it stats/deletes potentially
        # thousands of files, which used to stall the GUI thread exactly when
        # the user starts interacting.
        purge_days = self._prefs.get("trash_purge_days", 0)

        def _housekeeping(days=purge_days):
            try:
                if isinstance(days, int) and days > 0:
                    favorites.purge_older_than(days)
                cache.enforce_cap()
            except Exception:
                pass
        QTimer.singleShot(
            1500, lambda: self._scan_pool.start(_FnJob(_housekeeping)))

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
        self._view.contextMenu.connect(self._show_grid_context_menu)
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
        self._dupes_btn = self._btn("Duplicates", self._open_dupes)
        self._dupes_btn.setToolTip(
            "Find byte-identical copies among the loaded media")
        h.addWidget(self._dupes_btn)
        self._album_tags_btn = self._btn("Tag albums", self._open_album_tags)
        self._album_tags_btn.setToolTip(
            "Tag whole folders; each mirrors into a folder-tags subfolder")
        h.addWidget(self._album_tags_btn)
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
        self._sort.addItem("Rating", "rating")
        self._sort.addItem("Dimensions \u00b7 area", "area")
        self._sort.addItem("Dimensions \u00b7 width", "width")
        self._sort.addItem("Dimensions \u00b7 height", "height")
        self._sort.addItem("Manual order", "manual")
        self._sort.currentIndexChanged.connect(lambda _: self._on_sort_changed())
        h.addWidget(self._sort)
        # Secondary sort: ties from the primary key break by this one.
        h.addWidget(QLabel("then"))
        self._sort2 = QComboBox()
        self._sort2.setToolTip("Secondary sort \u2014 breaks ties from the primary")
        self._sort2.addItem("Name", "name")
        self._sort2.addItem("(none)", "")
        self._sort2.addItem("Images first", "img_first")
        self._sort2.addItem("Videos first", "vid_first")
        self._sort2.addItem("Favourites", "favorites")
        self._sort2.addItem("Dimensions \u00b7 area", "area")
        self._sort2.addItem("Dimensions \u00b7 width", "width")
        self._sort2.addItem("Dimensions \u00b7 height", "height")
        self._sort2.currentIndexChanged.connect(lambda _: self._on_sort_changed())
        h.addWidget(self._sort2)
        self._dir_btn = self._btn("\u2191", self._toggle_sort_dir)
        self._dir_btn.setToolTip("Ascending \u2014 click for descending")
        h.addWidget(self._dir_btn)

        h.addWidget(self._sep())
        self._img_btn = self._btn("Images", self._apply_filter, checkable=True)
        self._img_btn.setChecked(True)
        self._gif_btn = self._btn("GIFs", self._apply_filter, checkable=True)
        self._gif_btn.setChecked(True)
        self._gif_btn.setToolTip("Show / hide animated GIFs")
        self._vid_btn = self._btn("Videos", self._apply_filter, checkable=True)
        self._vid_btn.setChecked(True)
        self._favs_btn = self._btn(f"{config.ICON_HEART_FULL} Favs",
                                   self._apply_filter, checkable=True)
        self._favs_btn.setToolTip("Show only favourites")
        self._favdir_btn = self._btn("📁♥", self._toggle_folder_favs,
                                     checkable=True)
        self._favdir_btn.setToolTip(
            "View the Favorites subfolder(s) of the opened folder(s)")
        h.addWidget(self._img_btn); h.addWidget(self._gif_btn)
        h.addWidget(self._vid_btn)
        h.addWidget(self._favs_btn)
        h.addWidget(self._favdir_btn)

        # Filter-by-tag menu: check any tags to show only items carrying them.
        self._tag_menu_btn = self._btn("Tags ▾", None)
        self._tag_menu_btn.setToolTip("Show only items with the checked tags")
        self._tag_menu = QMenu(self._tag_menu_btn)
        self._tag_filter_actions: dict[str, QAction] = {}
        self._tag_matchall_act = None
        self._rebuild_tag_filter_menu()
        self._tag_menu_btn.setMenu(self._tag_menu)
        h.addWidget(self._tag_menu_btn)

        self._search = QLineEdit()
        self._search.setPlaceholderText("search\u2026")
        self._search.setToolTip(
            "Filename substring, or filters: tag:BT  fav:yes  type:video  "
            "w>1920  h<=1080 \u2014 combine freely (all must match)")
        self._search.setFixedWidth(190)
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

        settings_btn = self._btn("⚙", self._open_settings)
        settings_btn.setToolTip("Settings")
        h.addWidget(settings_btn)

        self._guide_btn = self._btn(f"{config.ICON_INFO} Guide",
                                    self.show_welcome)
        self._guide_btn.setToolTip("What this program can do (welcome guide)")
        h.addWidget(self._guide_btn)

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
        # In multi-view the settings bar sits just below the panel's own
        # chrome strip instead of overlapping it.
        y0 = 8
        if (self._mv is not None
                and self._content_stack.currentWidget() is self._mv
                and self._mv._chrome_widget.isVisible()):
            y0 = self._mv._chrome_widget.sizeHint().height() + 4
        self._bar.setGeometry(8, y0, cw - 16, bh)
        if self._undo_bar.isVisible():
            # Deleted-media banner sits at the top, just below the control
            # bar's strip (fixed slot, so the two never overlap even while
            # the control bar auto-hides/shows).
            uh = self._undo_bar.sizeHint().height()
            self._undo_bar.setGeometry(8, 8 + bh + 6, cw - 16, uh)
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
        # In multi-view the bar follows the panel's own chrome bars.
        if (self._mv is not None
                and self._content_stack.currentWidget() is self._mv):
            return self._mv._chrome_widget.isVisible()
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
        if gen != self._probed_gen and not self._low_io_forced and paths:
            # First batch of this open: measure the drive before the loader
            # fans out (sample a spread of files, not just the first few).
            self._probed_gen = gen
            sample = paths[:: max(1, len(paths) // 4)][:4]
            self._scan_pool.start(_IoProbeJob(gen, sample, self._probe_sig))
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
    def _rebuild_tag_filter_menu(self) -> None:
        """(Re)populate the Tags filter menu from the current tag set."""
        from .engine import tags as _tags
        match_all = (self._tag_matchall_act.isChecked()
                     if self._tag_matchall_act is not None else False)
        self._tag_menu.clear()
        self._tag_filter_actions = {}
        for t in _tags.get_tags():
            act = self._tag_menu.addAction(t)
            act.setCheckable(True)
            act.toggled.connect(lambda _=False: self._apply_filter())
            self._tag_filter_actions[t] = act
        self._tag_menu.addSeparator()
        self._tag_matchall_act = self._tag_menu.addAction("Match all (AND)")
        self._tag_matchall_act.setCheckable(True)
        self._tag_matchall_act.setChecked(match_all)
        self._tag_matchall_act.toggled.connect(lambda _=False: self._apply_filter())
        self._tag_menu.addAction("Clear tag filter").triggered.connect(
            self._clear_tag_filter)
        self._tag_menu.addSeparator()
        self._tag_menu.addAction("Manage tags…").triggered.connect(
            self._open_tag_manager)

    def _open_settings(self) -> None:
        from .settings_dialog import SettingsDialog
        dlg = SettingsDialog(self._prefs, self,
                             on_manage_tags=self._open_tag_manager)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        new = dlg.result_prefs()
        if new.get("theme") != self._prefs.get("theme"):
            self._set_theme(new["theme"])
        favorites.set_link_mode(new.get("link_mode", "copy"))
        li = new.get("low_io_mode")
        self._low_io_forced = isinstance(li, bool)
        if self._low_io_forced:
            self._set_low_io(bool(li), forced=True)
        elif self._low_io:                 # was forced on, now auto -> restore
            self._set_low_io(False)
        self._prefs = new
        prefs.save_prefs(self._prefs)
        self._status.setText("Settings saved")

    # -- welcome / guide -------------------------------------------------------
    def show_welcome(self) -> None:
        """Open the feature guide.  Persists the 'show at startup' choice so
        first-run auto-display and the manual button share one setting."""
        from .welcome_dialog import WelcomeDialog
        at_startup = not self._prefs.get("welcome_seen", False)
        dlg = WelcomeDialog(at_startup, self)
        dlg.exec()
        # welcome_seen is the inverse of "show at startup".
        self._prefs["welcome_seen"] = not dlg.show_at_startup()
        prefs.save_prefs(self._prefs)

    def maybe_show_welcome(self) -> None:
        """Show the guide once, on the first launch (until the user opts out).

        Called from the app bootstrap after the window is up — never during
        construction, so building a MainWindow (e.g. in tests) has no popup."""
        if not self._prefs.get("welcome_seen", False):
            self.show_welcome()

    def _open_tag_manager(self) -> None:
        from .tag_manager import TagManagerDialog
        dlg = TagManagerDialog(self)
        dlg.exec()
        if dlg.changed:
            self._rebuild_tag_filter_menu()
            self._tag_menu_btn.setText("Tags ▾")
            if self._mv is not None:
                self._mv.rebuild_tag_buttons()
            self._apply_filter()

    def _selected_filter_tags(self) -> set:
        return {t for t, a in self._tag_filter_actions.items() if a.isChecked()}

    def _clear_tag_filter(self) -> None:
        for a in self._tag_filter_actions.values():
            a.setChecked(False)
        self._tag_matchall_act.setChecked(False)
        self._apply_filter()

    def _apply_filter(self) -> None:
        tag_filter = self._selected_filter_tags()
        self._model.set_filter(self._img_btn.isChecked(),
                               self._vid_btn.isChecked(),
                               self._search.text(),
                               self._favs_btn.isChecked(),
                               gifs=self._gif_btn.isChecked(),
                               tag_filter=tag_filter,
                               tag_match_all=self._tag_matchall_act.isChecked())
        # Reflect active tag filtering on the button label.
        self._tag_menu_btn.setText(
            f"Tags ({len(tag_filter)}) ▾" if tag_filter else "Tags ▾")
        self._refresh_mv_after_model_change()
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
                    "Adjust the Images / GIFs / Videos toggles or clear the search box")

    def _toggle_folder_favs(self) -> None:
        """View the per-folder Favorites subfolder(s) of the opened folder(s);
        toggling off restores the folders that were open before."""
        if self._favdir_btn.isChecked():
            favdirs = [os.path.join(f, "Favorites")
                       for f in self._current_folders]
            favdirs = [f for f in favdirs if os.path.isdir(f)]
            if not favdirs:
                self._favdir_btn.setChecked(False)
                self._status.setText(
                    "No Favorites folders yet — favourite something first")
                return
            self._folders_before_favdirs = list(self._current_folders)
            self.open_folders(favdirs)
        else:
            back = getattr(self, "_folders_before_favdirs", None)
            if back:
                self._folders_before_favdirs = None
                self.open_folders(back)

    def _on_cols(self, n: int) -> None:
        self._view.set_columns(n)

    # -- sorting ---------------------------------------------------------------
    def _on_sort_changed(self) -> None:
        primary = self._sort.currentData()
        secondary = self._sort2.currentData()
        chain = [primary]
        if secondary and secondary != primary and primary != "manual":
            chain.append(secondary)
        self._model.set_sort_chain(chain)
        if self._model.needs_dimensions():
            self._ensure_dims()
        self._refresh_mv_after_model_change()

    def _sync_sort_combo(self, mode: str) -> None:
        """Reflect a model-driven sort change (e.g. drag-reorder -> manual)
        in the combos without re-triggering _on_sort_changed."""
        i = self._sort.findData(mode)
        if i >= 0 and self._sort.currentIndex() != i:
            self._sort.blockSignals(True)
            self._sort.setCurrentIndex(i)
            self._sort.blockSignals(False)
        if mode == "manual":
            j = self._sort2.findData("")
            if j >= 0:
                self._sort2.blockSignals(True)
                self._sort2.setCurrentIndex(j)
                self._sort2.blockSignals(False)

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
        # as parallel chunks.  Video probes serialise on the cv2 lock, so they
        # were ONE monolithic trailing job \u2014 but that meant dimsChanged fired
        # only when the LAST video finished, so the multi-view's orientation
        # groups looked complete-but-tiny ("the only videos") for minutes on a
        # big video folder.  Small video chunks make results land every few
        # seconds and the groups grow live.
        images = [p for p in paths if not _media.is_video(p)]
        videos = [p for p in paths if _media.is_video(p)]
        jobs: list[list[str]] = []
        if images:
            n = min(self._dims_pool.maxThreadCount(),
                    max(1, len(images) // 400 + 1))
            jobs.extend(c for c in (images[i::n] for i in range(n)) if c)
        _VCHUNK = 24
        jobs.extend(videos[i:i + _VCHUNK]
                    for i in range(0, len(videos), _VCHUNK))
        self._dims_remaining = len(jobs)
        self._status.setText("Computing dimensions\u2026")
        if self._mv is not None:
            self._mv.set_measuring(True)
        for c in jobs:
            self._dims_pool.start(_DimsJob(c, self._dims_sig))

    # -- slow-storage throttle -------------------------------------------------
    _LOW_IO_ON_MS  = 6.0    # avg 32 KB read slower than this → throttle
    _LOW_IO_OFF_MS = 2.0    # faster than this → restore full parallelism

    def _on_io_probe(self, gen: int, avg_ms: float) -> None:
        if gen != self._scan_gen or self._low_io_forced or avg_ms <= 0.0:
            return
        if avg_ms > self._LOW_IO_ON_MS:
            self._set_low_io(True)
        elif avg_ms < self._LOW_IO_OFF_MS:
            self._set_low_io(False)
        # In-between (hysteresis): keep whatever mode we're in.

    def _set_low_io(self, on: bool, forced: bool = False) -> None:
        """Throttle parallel media reads for slow/external storage.

        Over one USB pipe, 8 concurrent readers compete and finish SLOWER in
        aggregate than 2 — and saturating the bus also starves the video
        players' own reads, which is felt as stutter and input lag.
        """
        if on == self._low_io:
            return
        self._low_io = on
        if on:
            self._loader.set_max_threads(2)
            self._dims_pool.setMaxThreadCount(1)
            src = "(forced by prefs)" if forced else "detected"
            self._status.setText(
                f"Slow/external storage {src} — parallel reads reduced")
        else:
            self._loader.set_max_threads(self._loader.default_threads())
            self._dims_pool.setMaxThreadCount(self._dims_default)

    def _apply_dims_batch(self) -> None:
        self._dims_apply_timer.stop()
        if self._dims_apply:
            batch, self._dims_apply = self._dims_apply, {}
            self._model.set_dims(batch)

    def _on_dims_chunk(self, dims: dict) -> None:
        # Progressive reveal, but coalesced: chunks merge into one apply every
        # ~350 ms instead of a model reset + grid relayout per chunk.
        self._dims_done_for.update(dims.keys())
        if dims:
            self._dims_apply.update(dims)
            if not self._dims_apply_timer.isActive():
                self._dims_apply_timer.start()
        self._dims_remaining -= 1
        if self._dims_remaining > 0:
            return
        self._apply_dims_batch()     # pass complete \u2014 settle immediately
        self._dims_inflight = False
        if self._model.needs_dimensions():
            self._status.setText(
                f"Sorted by {self._sort.currentText().lower()}")
        # Paths added during the pass (streaming scan) still need measuring.
        if any(p not in self._dims_done_for for p in self._model.all_paths()):
            self._ensure_dims()      # sets measuring back on if it restarts
        if not self._dims_inflight and self._mv is not None:
            self._mv.set_measuring(False)

    # -- favourites / rotate / trash -------------------------------------------
    def _sync_tag_folders(self, path: str) -> None:
        from .engine import tags as _tags
        _tags.sync_tag_folders(path, self._favs.is_fav(path))

    def _on_grid_fav(self, row: int) -> None:
        path = self._model.path_at(row)
        if not path:
            return
        new = self._favs.toggle(path)
        self._model.refresh_fav(path)
        self._view.refresh_overlay_fav(row, new)
        self._sync_tag_folders(path)     # (un)favouriting changes tag-folder copies
        if self._favs_btn.isChecked():
            self._apply_filter()    # un-favourited item leaves the filtered view

    def _toggle_fav_path(self, path: str) -> None:
        new = self._favs.toggle(path)
        self._model.refresh_fav(path)
        # Update multi-view hearts in place (tiles are not reloaded on toggle).
        if self._mv is not None:
            self._mv.refresh_fav(path)
        self._sync_tag_folders(path)
        self._status.setText(
            f"{'Favourited' if new else 'Unfavourited'} {os.path.basename(path)}")
        if self._favs_btn.isChecked():
            self._apply_filter()

    def _on_grid_rotate(self, row: int) -> None:
        path = self._model.path_at(row)
        if path:
            self._request_rotate(path, 90)

    def _request_rotate(self, path: str, degrees: int) -> None:
        """Rotate an image permanently — decode/encode/write on a worker.

        The old synchronous version froze the GUI for the whole
        read+re-encode+write round trip, which on an external drive (or one
        waking from USB autosuspend) could be seconds.  Videos unsupported.
        """
        from .engine import media as _media
        if _media.is_video(path):
            self._status.setText("Rotating video files isn't supported")
            return
        if path in self._rotating:
            self._status.setText(
                f"Still rotating {os.path.basename(path)}…")
            return
        self._rotating.add(path)
        self._status.setText(f"Rotating {os.path.basename(path)}…")
        self._scan_pool.start(
            _RotateJob(path, degrees, self._rotate_sig))

    def _on_rotate_done(self, path: str, degrees: int, ok: bool,
                        w: int, h: int) -> None:
        self._rotating.discard(path)
        if not ok:
            self._status.setText(f"Couldn't rotate {os.path.basename(path)}")
            return
        # Invalidate cached pixels and the masonry aspect ratio, then push the
        # rotated file's true dimensions so the grid reflows immediately.
        self._model.reload_path(path)
        self._view.forget_path_dims(path)
        if w > 0 and h > 0:
            self._dims_done_for.add(path)
            self._model.set_dims({path: (w, h)})
        # Rotation re-encoded the file and dropped its embedded tags: restore
        # them FIRST, then resync the favourite mirror — both run on the same
        # single mirror worker (FIFO), so the mirror copy gets the re-tagged
        # file rather than a tagless one.
        from .engine import tags as _tags
        _tags.reembed(path)
        self._favs.resync_mirror(path)
        # Refresh any viewer currently showing this file.
        for c in self.children():
            if isinstance(c, Lightbox) and c.current_path() == path:
                c.reload_current()
        if (self._mv is not None
                and self._content_stack.currentWidget() is self._mv):
            self._mv.reload_path(path)
        self._status.setText(f"Rotated {os.path.basename(path)} {degrees}°")

    def _on_grid_trash(self, row: int) -> None:
        path = self._model.path_at(row)
        if path:
            self._trash_path(path)

    # -- grid context menu -----------------------------------------------------
    def _build_grid_menu(self, rows: list) -> "QMenu | None":
        """Build (but don't show) the selection context menu — separated from
        exec() so the contents are testable without a modal event loop."""
        if not rows:
            return None
        from .engine import tags as _tags
        menu = QMenu(self)
        n = len(rows)
        sel = f"{n} item(s)" if n > 1 else os.path.basename(
            self._model.path_at(rows[0]) or "")
        menu.addAction(f"{config.ICON_HEART_FULL}  Favourite {sel}",
                       lambda: self._on_grid_fav_batch(rows))
        tag_menu = menu.addMenu("Tag")
        paths = [p for p in (self._model.path_at(r) for r in rows) if p]
        for t in _tags.TAGS:
            act = tag_menu.addAction(t)
            act.setCheckable(True)
            # Checked only when EVERY selected item already carries the tag.
            act.setChecked(bool(paths)
                           and all(t in _tags.tags_for(p) for p in paths))
            act.triggered.connect(lambda _=False, tg=t: self._batch_tag(rows, tg))
        menu.addSeparator()
        menu.addAction(f"{config.ICON_TRASH}  Delete {sel}",
                       lambda: self._on_grid_trash_batch(rows))
        return menu

    def _show_grid_context_menu(self, gpos) -> None:
        menu = self._build_grid_menu(self._view.selected_rows())
        if menu is not None:
            menu.exec(gpos)

    def _batch_tag(self, rows: list, tag: str) -> None:
        """Toggle `tag` across a selection: add to all if any lacks it, else
        remove from all (mirrors the batch-favourite convention)."""
        from .engine import tags as _tags
        paths = [p for p in (self._model.path_at(r) for r in rows) if p]
        if not paths:
            return
        add = any(tag not in _tags.tags_for(p) for p in paths)
        for p in paths:
            if (tag in _tags.tags_for(p)) != add:
                _tags.toggle_tag(p, tag)
                _tags.sync_tag_folders(p, self._favs.is_fav(p))
        self._status.setText(
            f"{'Tagged' if add else 'Untagged'} {len(paths)} item(s): {tag}")
        if self._mv is not None:
            for p in paths:
                self._mv.refresh_tag(p)
        # A tag filter that's active may now include/exclude these items.
        if self._selected_filter_tags():
            self._apply_filter()

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
                self._sync_tag_folders(p)
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
        """Trash one or more files as a single undoable batch (worker-side
        moves, optimistic model update)."""
        for path in paths:
            # Release every open handle on the file first — multiview tiles
            # (including duplicates and the side-scroll buffer) and the
            # gallery's video-preview players each hold the file open, which
            # blocks moving a video to the trash on Windows.
            if self._mv is not None:
                self._mv.release_path(path)
            self._view.release_video(path)

        # ALL moves run on the worker: even deciding same-vs-cross device
        # needs an os.stat, and a stat against a drive waking from USB
        # autosuspend can block for seconds — never pay that on a click.
        # Same-device renames complete in milliseconds anyway; the undo bar
        # arms the moment the worker reports back.
        for path in paths:
            self._favs.discard(path)
        self._model.remove_paths(list(paths))
        self._status.setText(f"Moving {len(paths)} item(s) to trash…")
        self._scan_pool.start(_TrashJob(paths, self._trash_sig))

    def _on_trash_moved(self, moved: list, failed: list) -> None:
        """Background trash job finished — arm undo and surface failures."""
        for p in failed:
            self._model.add_path(p)     # optimistic removal rolled back
        if failed:
            self._status.setText(
                f"Couldn't delete {len(failed)} item(s); restored to view")
        if moved:
            self._finish_trash_batch([tuple(m) for m in moved])

    def _finish_trash_batch(self, batch: list[tuple[str, str]]) -> None:
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

    # -- duplicate finder ------------------------------------------------------
    def _open_dupes(self) -> None:
        from .dupes_dialog import DuplicatesDialog
        paths = self._model.all_paths()
        if not paths:
            self._status.setText("No media loaded to scan for duplicates.")
            return
        dlg = DuplicatesDialog(paths, self)
        dlg.revealRequested.connect(self._reveal_path)
        # Route trashing through the existing pipeline (worker move + undo bar).
        dlg.trashRequested.connect(lambda ps: self._trash_paths(list(ps)))
        dlg.exec()

    def _reveal_path(self, path: str) -> None:
        """Select the given path in the grid and scroll it into view."""
        for row in range(self._model.rowCount()):
            if self._model.path_at(row) == path:
                self._view.reveal_row(row)
                self._status.setText(os.path.basename(path))
                return
        # It may be filtered out of the current view.
        self._status.setText(
            f"{os.path.basename(path)} is hidden by the current filter.")

    # -- album (folder) tagging ------------------------------------------------
    def _open_album_tags(self) -> None:
        from .album_tags_dialog import AlbumTagsDialog
        dlg = AlbumTagsDialog(list(self._current_folders), self,
                              loader=self._loader)
        dlg.exec()

    def _rotate_from_lightbox(self, lb, degrees: int) -> None:
        path = lb.current_path()
        if path:
            self._request_rotate(path, degrees)
            # The lightbox reloads from _on_rotate_done when the worker lands.

    # -- lightbox / multiview --------------------------------------------------
    def _resume_previews_if_gallery_active(self) -> None:
        """Resume in-grid video previews only when the gallery is truly on
        top again (stack shows it and no lightbox window is open)."""
        if (self._content_stack.currentWidget() is self._view
                and self._lightbox_count <= 0):
            self._view.resume_video_previews()

    def _open_lightbox(self, row: int, from_mv: bool = False) -> None:
        # Hidden gallery previews under the viewer waste GUI-thread frame
        # conversions and starve input handling.
        self._view.suspend_video_previews()
        self._lightbox_count += 1
        lb = Lightbox(self._model, self._favs, self)
        lb.set_return_kind("multiview" if from_mv else "gallery")
        if from_mv:
            lb.returnToMulti.connect(self._reopen_multiview)
        lb.favToggled.connect(self._toggle_fav_path)
        lb.trashed.connect(self._trash_path)
        lb.requestInfo.connect(lambda p: InfoDialog(p, self).exec())
        lb.rotateRequested.connect(
            lambda deg, _lb=lb: self._rotate_from_lightbox(_lb, deg))
        # Close the lightbox before switching to the embedded multiview panel.
        lb.openMulti.connect(lambda r, _lb=lb: (_lb.close(), self._open_multiview(r)))
        lb.destroyed.connect(self._on_lightbox_gone)
        lb.showFullScreen()
        lb.raise_()
        lb.activateWindow()
        lb.show_row(row)

    def _on_lightbox_gone(self, *_) -> None:
        self._lightbox_count = max(0, self._lightbox_count - 1)
        self._resume_previews_if_gallery_active()

    def _open_multiview(self, start_row: int) -> None:
        # Same reasoning as the lightbox: 4 visible multi-view videos plus 4
        # invisible gallery previews meant up to 8 decode pipelines pushing
        # per-frame work through the GUI thread — every click lagged.
        self._view.suspend_video_previews()
        if self._mv is None:
            self._mv = MultiView(self._model, self._favs, self)
            self._mv.favToggled.connect(self._toggle_fav_path)
            self._mv.trashed.connect(self._trash_path)
            self._mv.rotated.connect(self._rotate_from_multiview)
            self._mv.openLightbox.connect(
                lambda r: self._open_lightbox(r, from_mv=True))
            self._mv.closeRequested.connect(self._close_multiview)
            self._mv.barsVisibleChanged.connect(self._on_mv_bars_visible)
            self._content_stack.addWidget(self._mv)
        # Kick off dims computation so orientation lists are accurate.
        self._ensure_dims()
        self._mv.set_measuring(self._dims_inflight)
        self._mv.open(start_row)
        self._content_stack.setCurrentWidget(self._mv)
        # The settings bar stays available in multi-view: it floats in a
        # RESERVED strip below the multi-view chrome (never over the tiles)
        # and follows the chrome's auto-hide rhythm.
        self._bar.show()
        self._mv.set_top_inset(self._bar.sizeHint().height() + 8)
        self._position_overlays()
        self._bar.raise_()

    def _on_mv_bars_visible(self, on: bool) -> None:
        """Multi-view chrome bars hid/showed — the settings bar follows."""
        if self._mv is None or self._content_stack.currentWidget() is not self._mv:
            return
        self._bar.setVisible(on)
        self._mv.set_top_inset(
            (self._bar.sizeHint().height() + 8) if on else 0)
        if on:
            self._position_overlays()
            self._bar.raise_()

    def _refresh_mv_after_model_change(self) -> None:
        """Sort/filter changed the model while multi-view is showing — refresh
        its orientation groups and page so it tracks the new order."""
        if (self._mv is not None
                and self._content_stack.currentWidget() is self._mv):
            self._mv._on_model_dims_changed()

    def _reopen_multiview(self) -> None:
        """Restore the multi-view page the lightbox was entered from."""
        if self._mv is None:
            return
        self._view.suspend_video_previews()
        self._mv.reopen()
        self._content_stack.setCurrentWidget(self._mv)
        self._bar.show()
        self._mv.set_top_inset(self._bar.sizeHint().height() + 8)
        self._position_overlays()
        self._bar.raise_()

    def _rotate_from_multiview(self, path: str, degrees: int = 90) -> None:
        self._request_rotate(path, degrees)
        # Multi-view tiles reload from _on_rotate_done when the worker lands.

    def _close_multiview(self) -> None:
        if self._mv is not None:
            self._mv.stop_autoscroll()
            # Stop hidden playback and release file handles — otherwise tiles
            # kept decoding their videos behind the gallery, and any file last
            # shown in multi-view stayed locked against deletion.
            self._mv.release_all_media()
            # Players just released their handles — videos whose tag embeds
            # failed while playing can be written now.
            from .engine import tags as _tags
            _tags.flush_pending()
        if self._mv is not None:
            self._mv.set_top_inset(0)
        self._content_stack.setCurrentWidget(self._view)
        self._bar.show()
        self._position_overlays()
        self._resume_previews_if_gallery_active()

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
