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

from PySide6.QtCore import Qt, QObject, QRunnable, QThreadPool, Signal, QTimer, QSize
from PySide6.QtGui import QAction, QKeySequence, QShortcut, QPixmap
from PySide6.QtWidgets import (QMainWindow, QWidget, QFrame, QHBoxLayout,
                               QVBoxLayout, QLabel, QToolButton, QLineEdit,
                               QComboBox, QSpinBox, QFileDialog, QMenu,
                               QPushButton, QApplication, QListView, QTreeView,
                               QAbstractItemView, QGridLayout)

BAR_HIDE_MS = 2000

from . import config, theme
from .engine import scan, prefs, favorites, cache
from .engine.favorites import Favorites
from .loader import ThumbnailLoader
from .model import GalleryModel
from .gallery_view import GalleryView
from .lightbox import Lightbox
from .multiview import MultiView
from .exif_panel import InfoDialog


# -- Streaming scan ------------------------------------------------------------

class _StreamSignals(QObject):
    batch = Signal(int, object)   # (gen, list[str])
    done  = Signal(int, object)   # (gen, ScanResult)


class _StreamScanJob(QRunnable):
    """Scan one or more folders off the GUI thread, emitting path batches."""
    BATCH = 200

    def __init__(self, folders: list[str], gen: int, signals: _StreamSignals):
        super().__init__()
        self._folders = list(folders)
        self._gen = gen
        self._signals = signals

    def run(self) -> None:
        from .engine.scan import scan_iter, ScanResult
        last_result = None
        for batch, result in scan_iter(self._folders, batch_size=self.BATCH):
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
        self.setStyleSheet(theme.stylesheet())

        config.ensure_dirs()
        self._prefs = prefs.load_prefs()
        self._recents = prefs.load_recent()
        self._favs = Favorites()
        # No explicit max_threads -- ThumbnailLoader auto-sizes to CPU count.
        self._loader = ThumbnailLoader(self)
        self._model = GalleryModel(self._favs, self._loader, self)
        self._current_folder: str | None = None
        self._current_folders: list[str] = []
        self._trash_stack: list[tuple[str, str]] = []

        # Streaming scan infrastructure.
        self._scan_gen = 0
        self._scan_pool = QThreadPool(self)
        self._stream_sig = _StreamSignals(self)
        self._stream_sig.batch.connect(self._on_scan_batch)
        self._stream_sig.done.connect(self._on_scan_done)

        self._dims_sig = _DimsSignals(self)
        self._dims_sig.done.connect(self._on_dims_done)
        self._dims_done_for: set[str] = set()

        self._build_ui()
        self._restore_session()

        self._autoscroll = False
        self._autoscroll_timer = QTimer(self)
        self._autoscroll_timer.setInterval(20)
        self._autoscroll_timer.timeout.connect(self._autoscroll_tick)

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
        root.addWidget(self._view, 1)

        # -- Floating control bar ---------------------------------------------
        self._bar = QFrame(central)
        self._bar.setObjectName("OverlayBar")
        self._bar.setStyleSheet(
            "QFrame#OverlayBar { background: rgba(15,15,15,225);"
            " border-radius: 9px; }")
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
        h.addWidget(self._sep())

        h.addWidget(QLabel("cols"))
        self._cols_spin = QSpinBox()
        self._cols_spin.setRange(config.MIN_COLS, config.MAX_COLS)
        self._cols_spin.setValue(config.DEFAULT_COLS)
        self._cols_spin.valueChanged.connect(self._on_cols)
        h.addWidget(self._cols_spin)

        h.addWidget(QLabel("sort"))
        self._sort = QComboBox()
        self._sort.addItem("Name", "name")
        self._sort.addItem("Images first", "img_first")
        self._sort.addItem("Videos first", "vid_first")
        self._sort.addItem("Favourites", "favorites")
        self._sort.addItem("Dimensions \u00b7 area", "area")
        self._sort.addItem("Dimensions \u00b7 width", "width")
        self._sort.addItem("Dimensions \u00b7 height", "height")
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
        h.addWidget(self._img_btn); h.addWidget(self._vid_btn)

        self._search = QLineEdit()
        self._search.setPlaceholderText("search\u2026")
        self._search.setFixedWidth(150)
        self._search.textChanged.connect(lambda _: self._apply_filter())
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

    def _show_bar(self) -> None:
        self._bar_hide_timer.stop()
        self._position_overlays()
        self._bar.show()
        self._bar.raise_()

    def _toggle_bar(self) -> None:
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
        """Folder chooser that allows MULTIPLE folders and previews their media."""
        dlg = QFileDialog(self, "Select folder(s)  \u2014  Ctrl/Shift-click for multiple")
        dlg.setFileMode(QFileDialog.FileMode.Directory)
        dlg.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        dlg.setOption(QFileDialog.Option.ShowDirsOnly, True)
        for v in dlg.findChildren(QListView) + dlg.findChildren(QTreeView):
            v.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

        preview = QLabel("Select a folder\nto preview")
        preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        preview.setWordWrap(True)
        preview.setFixedWidth(240)
        preview.setMinimumHeight(240)
        preview.setStyleSheet("background:#000; color:#888;"
                              " border:1px solid #262626; border-radius:6px;")
        lay = dlg.layout()
        if isinstance(lay, QGridLayout):
            lay.addWidget(preview, 0, lay.columnCount(), lay.rowCount(), 1)
        dlg.currentChanged.connect(
            lambda p: self._update_folder_preview(p, preview))

        if dlg.exec():
            folders = [f for f in dlg.selectedFiles() if os.path.isdir(f)]
            if folders:
                self.open_folders(folders)

    def _update_folder_preview(self, path: str, label: QLabel) -> None:
        if not path or not os.path.isdir(path):
            return
        first_img = first_any = None
        count = 0
        try:
            for name in os.listdir(path):
                ext = os.path.splitext(name.lower())[1]
                if ext in config.SUPPORTED:
                    count += 1
                    full = os.path.join(path, name)
                    if first_any is None:
                        first_any = full
                    if first_img is None and ext in config.IMAGE_EXT:
                        first_img = full
                    if first_img and count > 50:
                        break
        except OSError:
            label.setText("(unreadable)")
            label.setPixmap(QPixmap())
            return
        target = first_img or first_any
        if target is None:
            label.setPixmap(QPixmap())
            label.setText("(no media)")
            return
        try:
            qim = cache.get_thumbnail(target, 220)
        except Exception:
            qim = None
        if qim is not None and not qim.isNull():
            label.setPixmap(QPixmap.fromImage(qim).scaled(
                220, 220, Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation))
            label.setToolTip(f"{count} media file(s)")
        else:
            label.setPixmap(QPixmap())
            label.setText(f"{count} media file(s)")

    def open_folder(self, folder: str) -> None:
        self.open_folders([folder])

    def open_folders(self, folders: list[str]) -> None:
        folders = [f for f in folders if os.path.isdir(f)]
        if not folders:
            return
        self._current_folders = folders
        self._current_folder = folders[0]
        if len(folders) == 1:
            self.setWindowTitle(
                f"Gallery \u2014 {os.path.basename(os.path.normpath(folders[0]))}")
        else:
            self.setWindowTitle(f"Gallery \u2014 {len(folders)} folders")
        for f in folders:
            self._recents = [f] + [r for r in self._recents if r != f]
        self._recents = self._recents[:10]
        prefs.save_recent(self._recents)
        self._rebuild_recent_menu()

        # Bump generation to discard batches from any in-progress scan.
        self._scan_gen += 1
        self._model.set_paths([])    # clear immediately
        self._status.setText(f"Scanning {len(folders)} folder(s)\u2026")
        self._scan_pool.start(
            _StreamScanJob(folders, self._scan_gen, self._stream_sig))

    def _on_scan_batch(self, gen: int, paths: list) -> None:
        if gen != self._scan_gen:
            return
        self._model.add_paths_batch(paths)
        n = self._model.rowCount()
        self._status.setText(f"Scanning\u2026  {n} found")

    def _on_scan_done(self, gen: int, result) -> None:
        if gen != self._scan_gen:
            return
        self._dims_done_for.clear()
        self._model.finalize_scan()
        self._status.setText(result.summary())
        if self._model.needs_dimensions():
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
                               self._search.text())

    def _on_cols(self, n: int) -> None:
        self._view.set_columns(n)

    # -- sorting ---------------------------------------------------------------
    def _on_sort_changed(self) -> None:
        self._model.set_sort(self._sort.currentData())
        if self._model.needs_dimensions():
            self._ensure_dims()

    def _toggle_sort_dir(self) -> None:
        desc = not self._model.descending()
        self._model.set_descending(desc)
        self._dir_btn.setText("\u2193" if desc else "\u2191")
        self._dir_btn.setToolTip("Descending \u2014 click for ascending" if desc
                                 else "Ascending \u2014 click for descending")

    def _ensure_dims(self) -> None:
        paths = [p for p in self._model.all_paths()
                 if p not in self._dims_done_for]
        if not paths:
            return
        self._status.setText("Computing dimensions\u2026")
        self._scan_pool.start(_DimsJob(paths, self._dims_sig))

    def _on_dims_done(self, dims: dict) -> None:
        self._dims_done_for.update(dims.keys())
        self._model.set_dims(dims)
        if self._model.needs_dimensions():
            self._status.setText(
                f"Sorted by {self._sort.currentText().lower()}")

    # -- favourites / rotate / trash -------------------------------------------
    def _on_grid_fav(self, row: int) -> None:
        path = self._model.path_at(row)
        if not path:
            return
        new = self._favs.toggle(path)
        self._model.refresh_fav(path)
        self._view.refresh_overlay_fav(row, new)

    def _toggle_fav_path(self, path: str) -> None:
        self._favs.toggle(path)
        self._model.refresh_fav(path)

    def _on_grid_rotate(self, row: int) -> None:
        path = self._model.path_at(row)
        if path:
            self._model.rotate_path(path)

    def _on_grid_trash(self, row: int) -> None:
        path = self._model.path_at(row)
        if path:
            self._trash_path(path)

    def _trash_path(self, path: str) -> None:
        dest = favorites.trash_file(path)
        if not dest:
            self._status.setText(f"Delete failed: {os.path.basename(path)}")
            return
        self._favs.discard(path)
        self._model.remove_path(path)
        self._trash_stack.append((path, dest))
        self._undo_label.setText(
            f"{config.ICON_TRASH}  Deleted {os.path.basename(path)}")
        self._undo_bar.show()
        self._undo_bar.raise_()
        self._position_overlays()
        self._undo_timer.start(6000)

    def _undo_trash(self) -> None:
        self._undo_bar.hide()
        if not self._trash_stack:
            return
        orig, trash = self._trash_stack.pop()
        if favorites.restore_file(orig, trash):
            self._model.add_path(orig)
            self._status.setText(f"Restored {os.path.basename(orig)}")

    # -- lightbox / multiview --------------------------------------------------
    def _open_lightbox(self, row: int) -> None:
        lb = Lightbox(self._model, self._favs, self)
        lb.favToggled.connect(self._toggle_fav_path)
        lb.trashed.connect(self._trash_path)
        lb.requestInfo.connect(lambda p: InfoDialog(p, self).exec())
        lb.openMulti.connect(self._open_multiview)
        lb.showFullScreen()
        lb.show_row(row)

    def _open_multiview(self, start_row: int) -> None:
        mv = MultiView(self._model, self._favs, start_row, self)
        mv.favToggled.connect(self._toggle_fav_path)
        mv.trashed.connect(self._trash_path)
        mv.openLightbox.connect(self._open_lightbox)
        mv.showFullScreen()

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
        self.showNormal() if self.isFullScreen() else self.showFullScreen()

    # -- session persistence ---------------------------------------------------
    def _restore_session(self) -> None:
        g = self._prefs.get("geometry")
        if isinstance(g, list) and len(g) == 4:
            self.setGeometry(*g)
        cols = self._prefs.get("cols")
        if isinstance(cols, int) and config.MIN_COLS <= cols <= config.MAX_COLS:
            self._cols_spin.setValue(cols)
        srt = self._prefs.get("sort")
        if srt:
            i = self._sort.findData(srt)
            if i >= 0:
                self._sort.setCurrentIndex(i)
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
            self._prefs["sort"] = self._sort.currentData()
            self._prefs["sort_desc"] = self._model.descending()
            if self._current_folder:
                self._prefs["last_folder"] = self._current_folder
            prefs.save_prefs(self._prefs)
        except Exception:
            pass
        super().closeEvent(e)

    def clear_cache(self) -> int:
        return cache.clear()
