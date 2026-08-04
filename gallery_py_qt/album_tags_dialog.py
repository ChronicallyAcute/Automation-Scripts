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

from PySide6.QtCore import Qt, QModelIndex, QSize
from PySide6.QtGui import (QIcon, QPixmap, QStandardItem, QStandardItemModel,
                           QKeySequence, QShortcut)
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QListView, QTreeView, QAbstractItemView,
                               QPushButton, QSplitter, QWidget, QFrame,
                               QMessageBox, QMenu, QInputDialog)

from . import config
from .engine import cache, favorites, fileops, foldertags, scan, tags
from .fs_picker import _CheckFSModel, HoverPreview, quick_access_row

# Thumbnail edge for the contents pane.  Deliberately different from the hover
# preview's 160 px so the two can share one ThumbnailLoader without their
# `ready` signals being mistaken for one another.
_TILE_PX = 128
# Cap on tiles rendered for one folder — a 5,000-file album must not stall.
_MAX_TILES = 300
# Tagging N albums queues N recursive copies onto the single mirror worker.
_CONFIRM_ABOVE = 10


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

        root = QVBoxLayout(self)

        intro = QLabel(
            "Tagging an album mirrors the whole folder into "
            f"“{foldertags.FOLDER_TAGS_DIRNAME}/&lt;tag&gt;/” inside your "
            "Gallery Favorites.  Tick folders to tag them; right-click (or use "
            "F2 / Del / Ctrl+X / Ctrl+V) to rename, delete, cut and paste.")
        intro.setWordWrap(True)
        intro.setStyleSheet(f"color: {config.FG_MID}; padding: 2px;")
        root.addWidget(intro)

        # ---- left: checkbox filesystem tree ---------------------------------
        self._fs = _CheckFSModel(self)
        self._tree = QTreeView()
        self._tree.setModel(self._fs)
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
        quick, self._type_combo = quick_access_row(self._fs, self._goto)

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

        self._contents = QListView()
        self._contents.setViewMode(QListView.ViewMode.IconMode)
        self._contents.setIconSize(QSize(_TILE_PX, _TILE_PX))
        self._contents.setGridSize(QSize(_TILE_PX + 16, _TILE_PX + 32))
        self._contents.setResizeMode(QListView.ResizeMode.Adjust)
        self._contents.setMovement(QListView.Movement.Static)
        self._contents.setUniformItemSizes(True)
        self._contents.setWordWrap(True)
        self._contents.setSelectionMode(
            QAbstractItemView.SelectionMode.NoSelection)
        self._contents.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers)
        self._contents_model = QStandardItemModel(self)
        self._contents.setModel(self._contents_model)

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
    def _show_contents(self, folder: str) -> None:
        if not folder or folder == self._browsing:
            return
        self._browsing = folder
        self._tile_gen += 1                 # stale `ready` callbacks are dropped
        self._tiles.clear()
        self._contents_model.clear()

        try:
            paths = scan.scan_media(folder).paths
        except Exception:
            paths = []
        shown = paths[:_MAX_TILES]
        cur = foldertags.tags_for(folder)
        extra = f"  ·  more than {_MAX_TILES} shown" if len(paths) > len(shown) else ""
        self._contents_hdr.setText(
            f"{os.path.basename(os.path.normpath(folder)) or folder}  ·  "
            f"{len(paths)} media file(s){extra}"
            + (f"  ·  tags: {', '.join(cur)}" if cur else "  ·  untagged"))

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
        """Redraw the ticked-album panel with each folder's current tags."""
        self._picked_model.clear()
        folders = self._fs.checked_paths()
        for f in folders:
            cur = foldertags.tags_for(f)
            label = (f"{os.path.basename(os.path.normpath(f))}"
                     f"{('  ·  ' + ', '.join(cur)) if cur else '  ·  (untagged)'}")
            item = QStandardItem(label)
            item.setEditable(False)
            item.setToolTip(f)
            self._picked_model.appendRow(item)
        n = len(folders)
        self._tag_lbl.setText(
            "Tick one or more folders to tag them" if n == 0
            else ("Apply tag to 1 ticked album:" if n == 1
                  else f"Apply tag to {n} ticked albums:"))
        for b in self._tag_btns:
            b.setEnabled(n > 0)

    # -- tagging ---------------------------------------------------------------
    def _build_tag_buttons(self) -> None:
        for b in self._tag_btns:
            b.deleteLater()
        self._tag_btns.clear()
        for t in tags.TAGS:
            btn = QPushButton(t)
            btn.setToolTip(f"Toggle “{t}” on the ticked album(s)")
            btn.clicked.connect(lambda _=False, tag=t: self._toggle_tag(tag))
            self._tag_row.addWidget(btn)
            self._tag_btns.append(btn)
        self._tag_row.addStretch(1)

    @staticmethod
    def _is_mirror_folder(folder: str) -> bool:
        # Only the folder-tags mirror tree itself is off-limits (re-mirroring a
        # mirror nests copies).  Ordinary folders — even ones that happen to
        # live under the Gallery Favorites directory — tag fine.
        return foldertags.is_mirror_path(folder)

    def _toggle_tag(self, tag: str) -> None:
        albums = self._checked_albums()
        if not albums:
            return
        # The only folders that can't be tagged are the folder-tags mirror
        # copies themselves; skip those and tell the user, but tag the rest.
        skipped = [a for a in albums if self._is_mirror_folder(a)]
        albums = [a for a in albums if a not in skipped]
        if skipped:
            QMessageBox.information(
                self, "Tag albums",
                f"{len(skipped)} folder(s) are inside the “folder tags” mirror "
                "and can't be tagged again — they were skipped.")
            if not albums:
                return
        if len(albums) > _CONFIRM_ABOVE and QMessageBox.question(
                self, "Tag albums",
                f"Apply “{tag}” to {len(albums)} albums? Each one is mirrored "
                f"into the folder-tags subfolder ({favorites.LINK_MODE}).",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                ) != QMessageBox.StandardButton.Yes:
            return
        # Add to all if any album lacks the tag; otherwise remove from all.
        add = any(tag not in foldertags.tags_for(a) for a in albums)
        for a in albums:
            if (tag in foldertags.tags_for(a)) != add:
                foldertags.toggle_folder_tag(a, tag)
        self._refresh()
        if self._browsing:
            browsed, self._browsing = self._browsing, ""
            self._show_contents(browsed)      # header tag summary may change

    def done(self, result: int) -> None:
        # QDialog.exec() leaves the dialog owned by its parent window, so a
        # closed one lingers for the life of the main window.  Release what
        # actually costs: the shared loader keeps broadcasting `ready` to every
        # dialog ever opened, and each holds a model of up to _MAX_TILES icons.
        self._hover.hide()
        self._release_loader()
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
