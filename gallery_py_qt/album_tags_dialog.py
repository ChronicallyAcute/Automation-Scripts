"""Tag whole albums (folders): browse the filesystem, tick folders, tag them.

Mirrors the media-import picker so both "choose folders" experiences feel the
same: a checkbox filesystem tree on the left (with the import picker's quick
access row, media-class filter and hover thumbnail preview), and on the right a
thumbnail grid of whatever the browsed folder actually contains — so a folder
can be identified by its media, not just its name — over a list of every folder
currently ticked.

Any number of folders can be ticked at once; the tag buttons then apply to the
whole ticked batch with the same semantics the grid uses (add to all if any
lacks the tag, otherwise remove from all).  Each tagged album is mirrored into
``<FAVORITES_DIR>/folder tags/<TAG>/`` by :mod:`engine.foldertags`.
"""
from __future__ import annotations
import os

from PySide6.QtCore import (Qt, QModelIndex, QSize, QObject, QRunnable,
                            QThreadPool, Signal)
from PySide6.QtGui import (QIcon, QPixmap, QStandardItem, QStandardItemModel,
                           QKeySequence, QShortcut)
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QListView, QTreeView, QAbstractItemView,
                               QPushButton, QSplitter, QWidget, QFrame,
                               QMessageBox, QMenu, QInputDialog, QComboBox,
                               QStyle)

from . import config
from .engine import cache, favorites, fileops, foldertags, media, scan, tags
from .fs_picker import _CheckFSModel, HoverPreview, quick_access_row

# Thumbnail edge for the contents pane.  Deliberately different from the hover
# preview's 160 px so the two can share one ThumbnailLoader without their
# `ready` signals being mistaken for one another.
_TILE_PX = 128
# Cap on tiles rendered for one folder — a 5,000-file album must not stall.
_MAX_TILES = 300
# Tagging N albums queues N recursive copies onto the single mirror worker.
_CONFIRM_ABOVE = 10


class _DimsSignals(QObject):
    done = Signal(int, str, dict)      # generation, folder, {path: (w, h)}


class _DimsJob(QRunnable):
    """Compute (w, h) for every file off the GUI thread.

    Sorting the contents pane by dimensions needs the size of every media file,
    and probing a video costs an OpenCV open — doing that synchronously froze
    the dialog on video-heavy folders.  Results are stamped with a generation
    and folder so a stale batch from a folder the user already left is dropped.
    """
    def __init__(self, gen: int, folder: str, paths: "list[str]",
                 signals: "_DimsSignals"):
        super().__init__()
        self._gen = gen
        self._folder = folder
        self._paths = list(paths)
        self._signals = signals

    def run(self) -> None:
        out: "dict[str, tuple[int, int]]" = {}
        for p in self._paths:
            try:
                out[p] = media.peek_size(p)
            except Exception:
                out[p] = (0, 0)
        self._signals.done.emit(self._gen, self._folder, out)


class AlbumTagsDialog(QDialog):
    def __init__(self, folders: "list[str]", parent=None, loader=None):
        super().__init__(parent)
        self.setWindowTitle("Tag albums  —  tick folders, then choose a tag")
        self.resize(940, 620)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())
        self._loader = loader
        self._browsing = ""
        self._tile_gen = 0
        self._pending_goto = ""
        self._tiles: "dict[str, QStandardItem]" = {}
        self._cut_paths: "list[str]" = []      # Cut clipboard (move on Paste)
        # Dimension-sort support: cache (w, h) per file and compute misses off
        # the GUI thread so a video-heavy folder doesn't freeze the dialog.
        self._dims_cache: "dict[str, tuple[int, int]]" = {}
        self._dims_gen = 0
        self._dims_pool = QThreadPool(self)
        self._dims_pool.setMaxThreadCount(1)
        self._dims_sig = _DimsSignals(self)
        self._dims_sig.done.connect(self._on_dims_ready)

        root = QVBoxLayout(self)

        intro = QLabel(
            "Tick folders to tag every media file inside them, or click-drag to "
            "highlight specific files on the right and tag just those.  "
            "Right-click (or F2 / Del / Ctrl+X / Ctrl+V) to rename, delete, "
            "cut and paste.")
        intro.setWordWrap(True)
        intro.setStyleSheet(f"color: {config.FG_MID}; padding: 2px;")
        root.addWidget(intro)

        # ---- left: checkbox filesystem tree ---------------------------------
        self._fs = _CheckFSModel(self)
        self._tree = QTreeView()
        self._tree.setModel(self._fs)
        # ExtendedSelection already gives the tree click-drag range selection.
        self._tree.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._tree.setAnimated(True)
        for col in range(1, 4):           # hide size/type/date
            self._tree.hideColumn(col)
        self._tree.doubleClicked.connect(self._on_dbl)
        self._tree.clicked.connect(self._on_browse)
        sel = self._tree.selectionModel()
        if sel is not None:
            sel.currentChanged.connect(lambda cur, _prev: self._on_browse(cur))

        # File-explorer management: right-click menu + shortcuts on the tree.
        self._tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._tree.customContextMenuRequested.connect(self._tree_menu)
        _ctx = Qt.ShortcutContext.WidgetWithChildrenShortcut
        QShortcut(QKeySequence(Qt.Key.Key_F2), self._tree, context=_ctx,
                  activated=self._rename_prompt)
        QShortcut(QKeySequence(Qt.Key.Key_Delete), self._tree, context=_ctx,
                  activated=self._delete_prompt)
        QShortcut(QKeySequence("Ctrl+X"), self._tree, context=_ctx,
                  activated=self._cut_selected)
        QShortcut(QKeySequence("Ctrl+V"), self._tree, context=_ctx,
                  activated=self._paste_here)

        self._hover = HoverPreview(self, self._tree, self._fs, loader)
        quick, self._type_btn = quick_access_row(self._fs, self._goto)

        left = QWidget()
        llay = QVBoxLayout(left)
        llay.setContentsMargins(0, 0, 0, 0)
        llay.addLayout(quick)
        llay.addWidget(self._tree, 1)

        # ---- right: contents thumbnails + ticked-folder list ----------------
        self._contents_hdr = QLabel("Select a folder to preview its contents")
        self._contents_hdr.setWordWrap(True)
        self._contents_hdr.setStyleSheet(
            f"color: {config.FG_MID}; padding: 2px 0;")

        # Sort control for the contents pane (subfolders always come first).
        sort_row = QHBoxLayout()
        sort_row.setContentsMargins(0, 0, 0, 0)
        sort_lbl = QLabel("Sort by:")
        sort_lbl.setStyleSheet(f"color: {config.FG_MID};")
        self._sort_combo = QComboBox()
        for label, key in (("Type", "type"), ("Name", "name"),
                           ("Size", "size"), ("Dimensions", "dimensions"),
                           ("Tags", "tags")):
            self._sort_combo.addItem(label, key)
        self._sort_combo.currentIndexChanged.connect(self._on_sort_changed)
        sort_row.addWidget(sort_lbl)
        sort_row.addWidget(self._sort_combo)
        sort_row.addStretch(1)

        self._contents = QListView()
        self._contents.setViewMode(QListView.ViewMode.IconMode)
        self._contents.setIconSize(QSize(_TILE_PX, _TILE_PX))
        self._contents.setGridSize(QSize(_TILE_PX + 16, _TILE_PX + 32))
        self._contents.setResizeMode(QListView.ResizeMode.Adjust)
        self._contents.setMovement(QListView.Movement.Static)
        self._contents.setUniformItemSizes(True)
        self._contents.setWordWrap(True)
        # Click-drag rubber-band highlighting of files; highlighted files become
        # the tag target (see _tag_targets).
        self._contents.setSelectionMode(
            QAbstractItemView.SelectionMode.ExtendedSelection)
        self._contents.setSelectionRectVisible(True)
        self._contents.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers)
        self._contents_model = QStandardItemModel(self)
        self._contents.setModel(self._contents_model)
        self._contents.selectionModel().selectionChanged.connect(self._refresh)
        # Double-click a subfolder tile to browse into it.
        self._contents.doubleClicked.connect(self._on_contents_activated)
        self._folder_icon = self.style().standardIcon(
            QStyle.StandardPixmap.SP_DirIcon)

        picked_lbl = QLabel("Ticked albums:")
        picked_lbl.setStyleSheet(f"color: {config.FG_MID}; padding-top: 6px;")
        self._picked = QListView()
        self._picked.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers)
        self._picked.setSelectionMode(
            QAbstractItemView.SelectionMode.NoSelection)
        self._picked_model = QStandardItemModel(self)
        self._picked.setModel(self._picked_model)
        self._picked.setMaximumHeight(150)

        right = QWidget()
        rlay = QVBoxLayout(right)
        rlay.setContentsMargins(0, 0, 0, 0)
        rlay.addWidget(self._contents_hdr)
        rlay.addLayout(sort_row)
        rlay.addWidget(self._contents, 1)
        rlay.addWidget(picked_lbl)
        rlay.addWidget(self._picked)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([440, 480])
        root.addWidget(splitter, 1)

        # ---- controls --------------------------------------------------------
        btn_row = QHBoxLayout()
        self._tick_sub_btn = QPushButton("Tick subfolders")
        self._tick_sub_btn.setToolTip(
            "Tick every immediate subfolder of the browsed folder")
        self._tick_sub_btn.clicked.connect(self._tick_subfolders)
        self._none_btn = QPushButton("Untick all")
        self._none_btn.clicked.connect(self._untick_all)
        btn_row.addWidget(self._tick_sub_btn)
        btn_row.addStretch(1)
        btn_row.addWidget(self._none_btn)
        root.addLayout(btn_row)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        root.addWidget(sep)

        self._tag_lbl = QLabel("Apply tag to ticked album(s):")
        self._tag_lbl.setStyleSheet(f"color: {config.FG_MID};")
        root.addWidget(self._tag_lbl)

        self._tag_row = QHBoxLayout()
        self._tag_btns: "list[QPushButton]" = []
        self._build_tag_buttons()
        root.addLayout(self._tag_row)

        self._mode_lbl = QLabel(
            "Tags are written to each media file (metadata + search), not to "
            "the folder.")
        self._mode_lbl.setStyleSheet(f"color: {config.FG_DIM}; padding-top: 4px;")
        root.addWidget(self._mode_lbl)

        close_row = QHBoxLayout()
        close_row.addStretch(1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        close_row.addWidget(close_btn)
        root.addLayout(close_row)

        # Ticking a checkbox live-updates the ticked-album panel.
        self._fs.dataChanged.connect(self._refresh)
        self._fs.directoryLoaded.connect(self._on_dir_loaded)

        # Seed with the albums already loaded in the gallery.
        self._albums = [f for f in dict.fromkeys(folders) if os.path.isdir(f)]
        for f in self._albums:
            self._fs.add(f)
        if self._albums:
            self._goto(self._albums[0])
            self._show_contents(self._albums[0])
        if loader is not None:
            loader.ready.connect(self._on_tile_ready)
        self._refresh()

    # -- tree navigation -------------------------------------------------------
    def _goto(self, path: str) -> None:
        """Jump the tree to `path`, waiting for the model if it isn't loaded yet.

        QFileSystemModel populates on a gatherer thread, so index() is invalid
        until the parent directory has been visited — jumping on a cold path
        would otherwise silently do nothing.
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
        """Double-click ticks a folder outright (same habit as the picker)."""
        path = self._fs.filePath(index)
        if os.path.isdir(path):
            self._fs.add(path)
            self._refresh()

    def _on_browse(self, index: QModelIndex) -> None:
        if not index.isValid():
            return
        path = self._fs.filePath(index)
        if not os.path.isdir(path):
            path = os.path.dirname(path)
        self._show_contents(path)

    # -- file management (rename / delete / cut / paste / new folder) ----------
    def _selected_paths(self) -> "list[str]":
        """Distinct column-0 paths of the tree's current selection."""
        out: "list[str]" = []
        for idx in self._tree.selectedIndexes():
            if idx.column() != 0:
                continue
            p = self._fs.filePath(idx)
            if p and p not in out:
                out.append(p)
        return out

    def _paste_target(self) -> str:
        """Folder that a paste / new-folder lands in: the selected folder, the
        selected file's parent, else the folder being browsed."""
        idx = self._tree.currentIndex()
        p = self._fs.filePath(idx) if idx.isValid() else ""
        if p and os.path.isdir(p):
            return p
        if p:
            return os.path.dirname(p)
        return self._browsing or ""

    def _tree_menu(self, pos) -> None:
        sel = self._selected_paths()
        menu = QMenu(self)
        if sel:
            act = menu.addAction("Rename…", self._rename_prompt)
            act.setEnabled(len(sel) == 1)
            menu.addAction(f"Cut  ({len(sel)})", self._cut_selected)
        if self._cut_paths:
            menu.addAction(f"Paste  ({len(self._cut_paths)}) here",
                           self._paste_here)
        menu.addAction("New folder…", self._new_folder_prompt)
        if sel:
            menu.addSeparator()
            menu.addAction(f"Delete  ({len(sel)})", self._delete_prompt)
        menu.exec(self._tree.viewport().mapToGlobal(pos))

    # -- prompts (menu / shortcut entry points) --------------------------------
    def _rename_prompt(self) -> None:
        sel = self._selected_paths()
        if len(sel) != 1:
            return
        name, ok = QInputDialog.getText(
            self, "Rename", "New name:", text=os.path.basename(sel[0]))
        if ok and name:
            self._do_rename(sel[0], name)

    def _delete_prompt(self) -> None:
        sel = self._selected_paths()
        if not sel:
            return
        if QMessageBox.question(
                self, "Delete",
                f"Move {len(sel)} item(s) to the trash? "
                "You can restore them from the Trash browser.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                ) == QMessageBox.StandardButton.Yes:
            self._do_delete(sel)

    def _new_folder_prompt(self) -> None:
        parent = self._paste_target()
        if not parent:
            return
        name, ok = QInputDialog.getText(self, "New folder", "Folder name:")
        if ok and name:
            self._do_new_folder(parent, name)

    def _cut_selected(self) -> None:
        self._cut_paths = self._selected_paths()

    def _paste_here(self) -> None:
        dest = self._paste_target()
        if dest and self._cut_paths:
            self._do_move(self._cut_paths, dest)
            self._cut_paths = []

    # -- operations (testable cores; no modal prompts) -------------------------
    def _do_rename(self, path: str, new_name: str) -> bool:
        new, err = fileops.rename_path(path, new_name)
        if err:
            QMessageBox.warning(self, "Rename", f"Couldn't rename: {err}")
            return False
        self._rekey_checked(path, new)
        foldertags.relocate_folder(path, new)     # no-op if untagged
        if self._browsing == path:
            self._browsing = new
        self._after_fs_change([])
        return True

    def _do_delete(self, paths: "list[str]") -> None:
        for p in paths:                           # drop tags + mirrors first
            foldertags.forget_folder(p)
        deleted, errors = fileops.delete_paths(paths)
        for src, _trash in deleted:
            self._fs.discard(src)
        self._after_fs_change(errors)

    def _do_move(self, paths: "list[str]", dest_dir: str) -> None:
        moved, errors = fileops.move_paths(paths, dest_dir)
        for src, new in moved:
            self._rekey_checked(src, new)
            foldertags.relocate_folder(src, new)  # no-op if untagged
        self._after_fs_change(errors)

    def _do_new_folder(self, parent: str, name: str) -> "str | None":
        new, err = fileops.make_folder(parent, name)
        if err:
            QMessageBox.warning(self, "New folder", f"Couldn't create: {err}")
            return None
        self._after_fs_change([])
        return new

    def _rekey_checked(self, old: str, new: str) -> None:
        """Preserve a folder's ticked state across a rename/move (the old path
        no longer exists, so raw membership — not checked_all() — is checked)."""
        if self._fs.is_checked(old):
            self._fs.discard(old)
            self._fs.add(new)

    def _after_fs_change(self, errors: "list[tuple[str, str]]") -> None:
        # The browsed folder may have vanished (deleted / moved / renamed).
        if self._browsing and not os.path.isdir(self._browsing):
            self._browsing = ""
            self._tiles.clear()
            self._contents_model.clear()
            self._contents_hdr.setText("Select a folder to preview its contents")
        elif self._browsing:
            browsed, self._browsing = self._browsing, ""
            self._show_contents(browsed)          # contents may have changed
        self._repaint_checks()
        self._refresh()
        if errors:
            lines = "\n".join(f"• {os.path.basename(p)}: {m}"
                              for p, m in errors[:8])
            QMessageBox.warning(
                self, "Some items were skipped",
                f"{len(errors)} item(s) could not be completed:\n{lines}")

    # -- contents thumbnails ---------------------------------------------------
    def _on_sort_changed(self, *_) -> None:
        if self._browsing:
            browsed, self._browsing = self._browsing, ""
            self._show_contents(browsed)

    def _sort_key(self) -> str:
        return self._sort_combo.currentData() or "type"

    def _subfolders(self, folder: str) -> "list[str]":
        try:
            subs = [os.path.join(folder, n) for n in os.listdir(folder)]
        except OSError:
            return []
        subs = [p for p in subs if os.path.isdir(p)
                and os.path.basename(p) != "Favorites"]
        return sorted(subs, key=lambda p: os.path.basename(p).lower())

    def _sorted_files(self, paths: "list[str]") -> "list[str]":
        key = self._sort_key()
        if key == "name":
            return sorted(paths, key=lambda p: os.path.basename(p).lower())
        if key == "size":
            return sorted(paths, key=self._safe_size, reverse=True)
        if key == "dimensions":
            return sorted(paths, key=self._safe_area, reverse=True)
        if key == "tags":
            # Tagged first (by tag string), untagged last.
            return sorted(paths, key=lambda p: (not tags.tags_for(p),
                                                ",".join(tags.tags_for(p)).lower(),
                                                os.path.basename(p).lower()))
        # "type": by extension, then name.
        return sorted(paths, key=lambda p: (os.path.splitext(p)[1].lower(),
                                            os.path.basename(p).lower()))

    @staticmethod
    def _safe_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return 0

    def _safe_area(self, path: str) -> int:
        """Pixel area from the dimension cache (0 until the worker fills it in)."""
        wh = self._dims_cache.get(path)
        if not wh:
            return 0
        return int(wh[0]) * int(wh[1])

    def _show_contents(self, folder: str) -> None:
        if not folder or folder == self._browsing:
            return
        self._browsing = folder
        self._tile_gen += 1                 # stale `ready` callbacks are dropped
        self._tiles.clear()
        self._contents_model.clear()

        subs = self._subfolders(folder)
        try:
            all_files = scan.scan_media(folder).paths
        except Exception:
            all_files = []
        # Dimension sort needs every file's size; if any are still unknown,
        # show a name-sorted view now and re-sort when the worker reports back.
        if (self._sort_key() == "dimensions"
                and any(p not in self._dims_cache for p in all_files)):
            files = sorted(all_files, key=lambda p: os.path.basename(p).lower())
            self._kick_dims(folder, all_files)
        else:
            files = self._sorted_files(all_files)
        shown = files[:_MAX_TILES]
        extra = (f"  ·  more than {_MAX_TILES} files shown"
                 if len(files) > len(shown) else "")
        self._contents_hdr.setText(
            f"{os.path.basename(os.path.normpath(folder)) or folder}  ·  "
            f"{len(subs)} folder(s), {len(files)} media file(s){extra}")

        # Subfolders first, so the folder structure is visible before the media.
        for d in subs:
            item = QStandardItem(self._folder_icon, os.path.basename(d) + "/")
            item.setEditable(False)
            item.setToolTip(d)
            item.setData(d, Qt.ItemDataRole.UserRole)
            item.setData(True, Qt.ItemDataRole.UserRole + 1)   # is-dir flag
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter)
            self._contents_model.appendRow(item)

        for p in shown:
            item = QStandardItem(os.path.basename(p))
            item.setEditable(False)
            item.setToolTip(p)
            item.setData(p, Qt.ItemDataRole.UserRole)
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter)
            self._contents_model.appendRow(item)
            self._tiles[p] = item
            # Already-cached thumbs paint instantly; misses go to the loader.
            pm = self._cached_thumb(p)
            if pm is not None:
                item.setIcon(QIcon(pm))
            elif self._loader is not None:
                self._loader.request(p, _TILE_PX)

    def _kick_dims(self, folder: str, paths: "list[str]") -> None:
        """Compute the still-unknown dimensions for `folder` off the GUI thread."""
        todo = [p for p in paths if p not in self._dims_cache]
        if not todo:
            return
        self._dims_gen += 1
        self._dims_pool.start(
            _DimsJob(self._dims_gen, folder, todo, self._dims_sig))

    def _on_dims_ready(self, gen: int, folder: str, dims: dict) -> None:
        # Ignore a batch the user has already navigated away from.
        if gen != self._dims_gen or folder != self._browsing:
            self._dims_cache.update(dims)      # still worth caching for later
            return
        self._dims_cache.update(dims)
        if self._sort_key() == "dimensions":
            # Force a re-render past _show_contents' same-folder guard.
            self._browsing = ""
            self._show_contents(folder)

    def _on_contents_activated(self, index) -> None:
        """Double-clicking a subfolder tile browses into it."""
        path = index.data(Qt.ItemDataRole.UserRole)
        if path and os.path.isdir(path):
            self._goto(path)
            self._show_contents(path)

    @staticmethod
    def _cached_thumb(path: str) -> "QPixmap | None":
        try:
            qim = cache.get_thumbnail(path, _TILE_PX)
        except Exception:
            return None
        if qim is None or qim.isNull():
            return None
        return QPixmap.fromImage(qim)

    def _on_tile_ready(self, path: str, max_px: int, qim) -> None:
        # The loader is shared with the gallery grid and the hover preview.
        if max_px != _TILE_PX or qim is None or qim.isNull():
            return
        item = self._tiles.get(path)
        if item is not None:
            item.setIcon(QIcon(QPixmap.fromImage(qim)))

    # -- ticked albums ---------------------------------------------------------
    def _checked_albums(self) -> "list[str]":
        """The ticked folders — and only those.

        Deliberately no "fall back to the tree selection" like the import
        picker has: there, the fallback is resolved by an explicit Open press,
        whereas here a tag applies the moment it is clicked.  Merely
        highlighting a folder while browsing must never tag it; the tag buttons
        are disabled instead when nothing is ticked.
        """
        return self._fs.checked_paths()

    def _tick_subfolders(self) -> None:
        if not self._browsing:
            return
        try:
            for name in sorted(os.listdir(self._browsing)):
                p = os.path.join(self._browsing, name)
                if os.path.isdir(p) and name != "Favorites":
                    self._fs.add(p)
        except OSError:
            pass
        self._repaint_checks()
        self._refresh()

    def _untick_all(self) -> None:
        self._fs.clear()
        self._repaint_checks()
        self._refresh()

    def _repaint_checks(self) -> None:
        self._fs.dataChanged.emit(QModelIndex(), QModelIndex(),
                                  [Qt.ItemDataRole.CheckStateRole])

    def _refresh(self, *_) -> None:
        """Redraw the ticked-album panel and update the tag prompt/targets."""
        self._picked_model.clear()
        folders = self._fs.checked_paths()
        for f in folders:
            item = QStandardItem(os.path.basename(os.path.normpath(f)))
            item.setEditable(False)
            item.setToolTip(f)
            self._picked_model.appendRow(item)
        sel = self._selected_content_files()
        if sel:
            self._tag_lbl.setText(
                f"Apply tag to {len(sel)} highlighted file(s):")
        elif folders:
            self._tag_lbl.setText(
                f"Apply tag to every media file in {len(folders)} "
                f"ticked folder(s):")
        else:
            self._tag_lbl.setText(
                "Tick folders (or highlight files) to tag their media")
        can = bool(sel) or bool(folders)
        for b in self._tag_btns:
            b.setEnabled(can)

    # -- tagging ---------------------------------------------------------------
    def _build_tag_buttons(self) -> None:
        for b in self._tag_btns:
            b.deleteLater()
        self._tag_btns.clear()
        for t in tags.TAGS:
            btn = QPushButton(t)
            btn.setToolTip(f"Toggle “{t}” on every media file being targeted")
            btn.clicked.connect(lambda _=False, tag=t: self._toggle_tag(tag))
            self._tag_row.addWidget(btn)
            self._tag_btns.append(btn)
        self._tag_row.addStretch(1)

    def _media_in(self, folder: str) -> "list[str]":
        """Every supported media file inside `folder`, recursively.  Skips the
        per-folder "Favorites" mirrors so their copies aren't double-counted."""
        out: "list[str]" = []
        try:
            for root_dir, dirs, names in os.walk(folder):
                dirs[:] = [d for d in dirs if d != "Favorites"]
                for n in names:
                    if os.path.splitext(n)[1].lower() in config.SUPPORTED:
                        out.append(os.path.join(root_dir, n))
        except OSError:
            pass
        return out

    def _selected_content_files(self) -> "list[str]":
        """Files highlighted (click-drag) in the contents pane."""
        sm = self._contents.selectionModel()
        if sm is None:
            return []
        out: "list[str]" = []
        for idx in sm.selectedIndexes():
            p = idx.data(Qt.ItemDataRole.UserRole)
            if p and os.path.isfile(p) and p not in out:
                out.append(p)
        return out

    def _tag_targets(self) -> "list[str]":
        """What a tag click acts on: the highlighted files if any, else every
        media file inside the ticked folders."""
        sel = self._selected_content_files()
        if sel:
            return sel
        files: "list[str]" = []
        seen: "set[str]" = set()
        for folder in self._checked_albums():
            for m in self._media_in(folder):
                if m not in seen:
                    seen.add(m)
                    files.append(m)
        return files

    def _toggle_tag(self, tag: str) -> None:
        targets = self._tag_targets()
        if not targets:
            return
        if len(targets) > _CONFIRM_ABOVE and QMessageBox.question(
                self, "Tag media",
                f"Apply “{tag}” to {len(targets)} media file(s)?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                ) != QMessageBox.StandardButton.Yes:
            return
        # Add to all if any target lacks the tag; otherwise remove from all.
        add = any(tag not in tags.tags_for(f) for f in targets)
        tags.apply_tag_to_paths(targets, tag, add)
        self._refresh()
        if self._browsing:
            browsed, self._browsing = self._browsing, ""
            self._show_contents(browsed)      # per-file tag summaries may change

    def done(self, result: int) -> None:
        # QDialog.exec() leaves the dialog owned by its parent window, so a
        # closed one lingers for the life of the main window.  Release what
        # actually costs: the shared loader keeps broadcasting `ready` to every
        # dialog ever opened, and each holds a model of up to _MAX_TILES icons.
        self._hover.hide()
        self._release_loader()
        # Drain in-flight dimension probes so no worker emits into a dead dialog.
        self._dims_gen += 1
        self._dims_pool.clear()
        self._dims_pool.waitForDone(3000)
        self._tiles.clear()
        self._contents_model.clear()
        super().done(result)

    def _release_loader(self) -> None:
        if self._loader is None:
            return
        for slot in (self._on_tile_ready, self._hover.on_ready):
            try:
                self._loader.ready.disconnect(slot)
            except (RuntimeError, TypeError):
                pass                      # already disconnected
        self._loader = None
        self._hover._loader = None
