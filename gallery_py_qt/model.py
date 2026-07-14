"""Virtualized list model backing the gallery grid.

QListView only calls data() for items it is about to paint, so memory stays
bounded automatically \u2014 no manual hydrate/dehydrate needed.  Thumbnails are
fetched lazily through the threaded loader and cached (LRU-capped) as QPixmaps.

Fork improvements over gallery_qt.model:
  \u2022 _all_set \u2014 O(1) membership test for the master path list (was implicit
    O(n) via list membership checks in the original).
  \u2022 _path_to_row \u2014 O(1) reverse lookup from path \u2192 current row index.
    The original did an O(n) linear scan on *every* thumbnail arrival,
    favourite toggle, and rotation \u2014 producing O(n\u00b2) behaviour when
    thousands of thumbnails arrive in rapid succession.
  \u2022 add_paths_batch / finalize_scan \u2014 progressive streaming population.
    add_paths_batch() appends accepted items via beginInsertRows so the
    view shows media as soon as the first batch arrives from the scanner.
    finalize_scan() re-sorts everything once scanning is complete.
  \u2022 remove_path uses beginRemoveRows / endRemoveRows instead of a full model
    reset, so deleting a single file doesn't force the view to repaint every
    visible cell.
  \u2022 Adaptive pixmap cap \u2014 the in-RAM pixmap budget is expressed in megabytes
    (default 256 MB) and converted to an item count based on the current
    thumbnail size, rather than a fixed 400-item ceiling that left large
    thumbnails wasting far more RAM than intended.
"""
from __future__ import annotations
import os
from collections import OrderedDict

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt, QSize, Signal
from PySide6.QtGui import QImage, QPixmap, QTransform

from . import config
from .engine import media
from .loader import ThumbnailLoader
from .engine.favorites import Favorites

PathRole    = Qt.ItemDataRole.UserRole + 1
IsVideoRole = Qt.ItemDataRole.UserRole + 2
FavRole     = Qt.ItemDataRole.UserRole + 3
LoadedRole  = Qt.ItemDataRole.UserRole + 4
FailedRole  = Qt.ItemDataRole.UserRole + 5   # thumbnail decode failed (corrupt/unreadable)

# Target memory ceiling for the in-process pixmap LRU cache.
# At 320 px thumbnails (? 400 KB RGBA each) this yields ~640 items ? 256 MB.
_PIXMAP_MEM_CAP_MB = 256


def _pixmap_cap(thumb_px: int) -> int:
    """Max number of pixmaps that fits within the memory ceiling."""
    bytes_each = max(1, thumb_px) ** 2 * 4   # worst-case RGBA square
    cap = int(_PIXMAP_MEM_CAP_MB * 1024 * 1024 / bytes_each)
    return max(100, min(cap, 2000))


class GalleryModel(QAbstractListModel):
    # Emitted when per-path (w, h) dimensions are updated without a re-sort.
    # The masonry view listens to this to recompute cell heights.
    dimsChanged = Signal()
    # Emitted when the model changes its own sort mode (e.g. a drag-reorder
    # switches to "manual"), so the toolbar's sort combo can stay in sync.
    sortChanged = Signal(str)

    def __init__(self, favorites: Favorites, loader: ThumbnailLoader,
                 parent=None):
        super().__init__(parent)
        self._favs = favorites
        self._loader = loader
        self._all: list[str] = []           # unfiltered, unsorted master list
        self._all_set: set[str] = set()     # O(1) membership
        self._rows: list[str] = []          # visible (filtered + sorted)
        self._path_to_row: dict[str, int] = {}   # O(1) reverse lookup
        self._pixmaps: "OrderedDict[str, QPixmap]" = OrderedDict()
        self._rotation: dict[str, int] = {}
        self._dims: dict[str, tuple[int, int]] = {}
        self._failed: set[str] = set()       # paths whose thumbnail decode failed
        self._thumb_px = 320
        self._pixmap_cap = _pixmap_cap(self._thumb_px)
        # filters / sort
        self._show_images = True
        self._show_videos = True
        self._favs_only = False
        self._query = ""
        # Sort chain: keys applied in order (first differentiates, later ones
        # break ties).  Default: group by like sizes, then by name.
        self._sort_chain: list[str] = ["like_dims", "name"]
        self._sort = "like_dims"          # primary mode (legacy accessor)
        self._descending = False
        self._loader.ready.connect(self._on_thumb_ready)
        self._loader.failed.connect(self._on_thumb_failed)

    # -- population ------------------------------------------------------------
    def set_paths(self, paths: list[str]) -> None:
        """Replace the full path list and re-index (non-streaming)."""
        self._all = list(paths)
        self._all_set = set(paths)
        self._failed.clear()    # fresh folder — re-evaluate every file
        self._reindex()

    def add_paths_batch(self, paths: list[str]) -> None:
        """Append a batch of new paths during a streaming scan.

        Items that pass the current filter are inserted immediately via
        beginInsertRows so the view updates incrementally.  Call
        finalize_scan() once the scan is done to apply the configured sort.
        """
        new = [p for p in paths if p not in self._all_set]
        if not new:
            return
        self._all.extend(new)
        self._all_set.update(new)
        accepted = [p for p in new if self._passes_filter(p)]
        if not accepted:
            return
        first = len(self._rows)
        self.beginInsertRows(QModelIndex(), first, first + len(accepted) - 1)
        for i, p in enumerate(accepted):
            self._rows.append(p)
            self._path_to_row[p] = first + i
        self.endInsertRows()

    def add_paths_silent(self, paths: list[str]) -> None:
        """Stage paths into _all without updating visible rows.

        Use during scans when a dimension-based sort is active: the view stays
        empty until all dimensions are known, then _reindex() reveals everything
        in correct order — no mid-scan reorder flash.
        """
        new = [p for p in paths if p not in self._all_set]
        if new:
            self._all.extend(new)
            self._all_set.update(new)

    def finalize_scan(self) -> None:
        """Sort and deduplicate after a streaming scan completes."""
        self._reindex()

    def all_paths(self) -> list[str]:
        return list(self._all)

    def thumb_px(self) -> int:
        return self._thumb_px

    def set_thumb_px(self, px: int) -> None:
        px = max(120, int(px))
        if px == self._thumb_px:
            return
        self._thumb_px = px
        self._pixmap_cap = _pixmap_cap(px)
        self._pixmaps.clear()
        if self._rows:
            self.dataChanged.emit(
                self.index(0), self.index(len(self._rows) - 1),
                [Qt.ItemDataRole.DecorationRole])

    # -- filter helpers --------------------------------------------------------
    def _passes_filter(self, p: str) -> bool:
        vid = media.is_video(p)
        if vid and not self._show_videos:
            return False
        if (not vid) and not self._show_images:
            return False
        if self._favs_only and not self._favs.is_fav(p):
            return False
        if self._query and self._query not in os.path.basename(p).lower():
            return False
        return True

    # -- filtering / sorting --------------------------------------------------
    def set_filter(self, images: bool, videos: bool, query: str,
                   favs_only: bool = False) -> None:
        self._show_images, self._show_videos = images, videos
        self._favs_only = favs_only
        self._query = query.strip().lower()
        self._reindex()

    def set_sort(self, mode: str) -> None:
        self.set_sort_chain([mode])

    def set_sort_chain(self, chain: list[str]) -> None:
        """Apply several sort keys in order (later keys break ties)."""
        chain = [m for m in chain if m] or ["name"]
        self._sort_chain = chain
        self._sort = chain[0]
        self._reindex()

    def sort_chain(self) -> list[str]:
        return list(self._sort_chain)

    def set_descending(self, descending: bool) -> None:
        self._descending = bool(descending)
        self._reindex()

    def descending(self) -> bool:
        return self._descending

    def sort_mode(self) -> str:
        return self._sort

    def dim_at(self, path: str) -> tuple[int, int]:
        """Return cached (w, h) for path, or (0, 0) if not yet computed."""
        return self._dims.get(path, (0, 0))

    def row_for_path(self, path: str) -> int:
        """Return current visible row index for path, or -1 if not visible."""
        return self._path_to_row.get(path, -1)

    def needs_dimensions(self) -> bool:
        dims_modes = ("area", "width", "height", "like_dims")
        return any(m in dims_modes for m in self._sort_chain)

    def set_dims(self, dims: dict[str, tuple[int, int]]) -> None:
        """Supply (w, h) per path (from a background scan); re-sort if needed."""
        self._dims.update(dims)
        if self.needs_dimensions():
            self._reindex()          # modelReset already drives a re-layout
        # Always announce that dimensions changed — even for dimension sorts
        # where _reindex() handled the re-layout — so listeners keyed on
        # orientation (e.g. the multi-view's 3×1/2×2 auto-switch) update too.
        self.dimsChanged.emit()

    def _dim_area(self, p: str) -> int:
        w, h = self._dims.get(p, (0, 0))
        return w * h

    def swap_paths(self, a: str, b: str) -> None:
        """Swap two items' positions and switch to manual ordering.

        The first manual reorder freezes the *currently displayed* order into
        the master list (so the gallery doesn't snap back to scan order), then
        exchanges the two items.  Emits sortChanged("manual") so the toolbar
        combo can follow.
        """
        if a == b or a not in self._all_set or b not in self._all_set:
            return
        if self._sort != "manual":
            # Capture the visible order as the new manual baseline; append any
            # currently filtered-out items so they survive a later filter change.
            visible = list(self._rows)
            hidden = [p for p in self._all if p not in self._path_to_row]
            self._all = visible + hidden
            self._sort = "manual"
            self._sort_chain = ["manual"]
            self.sortChanged.emit("manual")
        ia, ib = self._all.index(a), self._all.index(b)
        self._all[ia], self._all[ib] = self._all[ib], self._all[ia]
        self._reindex()

    def _like_dims_key(self, p: str) -> tuple:
        """Component key that groups media by like dimensions.

        Primary:   orientation bucket (0=portrait, 1=square, 2=landscape)
        Secondary: quantized aspect ratio (groups e.g. all 9:16 together)
        Tertiary:  pixel area (larger media last within same ratio group)
        Fallback:  items with unknown dims sort to the end
        """
        w, h = self._dims.get(p, (0, 0))
        if w <= 0 or h <= 0:
            return (3, 0.0, 0)
        ratio = w / h
        if ratio < 0.95:
            bucket = 0      # portrait
        elif ratio > 1.05:
            bucket = 2      # landscape
        else:
            bucket = 1      # square / near-square
        # Quantize the aspect ratio to 0.05 steps so very-similar ratios
        # (e.g. 1920×1080 and 3840×2160) land in the same bucket.
        ar_q = round(max(w, h) / min(w, h) / 0.05) * 0.05
        return (bucket, ar_q, w * h)

    def _key_component(self, mode: str):
        """Return a callable producing one comparable tuple for `mode`."""
        name = lambda p: (os.path.basename(p).lower(),)
        return {
            "name":      name,
            "img_first": lambda p: (media.is_video(p),),
            "vid_first": lambda p: (not media.is_video(p),),
            "favorites": lambda p: (not self._favs.is_fav(p),),
            "area":      lambda p: (self._dim_area(p),),
            "width":     lambda p: (self._dims.get(p, (0, 0))[0],),
            "height":    lambda p: (self._dims.get(p, (0, 0))[1],),
            "like_dims": self._like_dims_key,
        }.get(mode)

    def _reindex(self) -> None:
        self.beginResetModel()
        rows = [p for p in self._all if self._passes_filter(p)]
        if self._sort != "manual":
            # Chain the selected sort keys; a final name component guarantees
            # a stable, deterministic tiebreak.
            chain = [m for m in self._sort_chain if m != "manual"]
            if "name" not in chain:
                chain = chain + ["name"]
            funcs = [f for f in (self._key_component(m) for m in chain) if f]
            rows.sort(key=lambda p: tuple(v for f in funcs for v in f(p)))
        # manual: preserve the current _all order (set by swap_paths)
        if self._descending:
            rows.reverse()
        self._rows = rows
        # Rebuild reverse index in one pass -- O(n), amortised over many lookups.
        self._path_to_row = {p: i for i, p in enumerate(rows)}
        self.endResetModel()

    # -- Qt model interface ----------------------------------------------------
    def rowCount(self, parent=QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def path_at(self, row: int) -> str | None:
        return self._rows[row] if 0 <= row < len(self._rows) else None

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        row = index.row()
        if row >= len(self._rows):
            return None
        path = self._rows[row]
        if role == PathRole:
            return path
        if role == IsVideoRole:
            return media.is_video(path)
        if role == FavRole:
            return self._favs.is_fav(path)
        if role == Qt.ItemDataRole.ToolTipRole:
            return os.path.basename(path)
        if role == LoadedRole:
            return path in self._pixmaps
        if role == FailedRole:
            return path in self._failed
        if role == Qt.ItemDataRole.DecorationRole:
            pm = self._pixmaps.get(path)
            if pm is not None:
                self._pixmaps.move_to_end(path)
                return pm
            # Don't re-queue files we already know are unreadable — otherwise
            # every repaint/scroll re-submits a decode job that just fails again.
            if path not in self._failed:
                self._loader.request(path, self._thumb_px)
            return None
        return None

    # -- thumbnail arrival -----------------------------------------------------
    def _on_thumb_ready(self, path: str, max_px: int, qim: QImage) -> None:
        if max_px != self._thumb_px:
            return
        pm = QPixmap.fromImage(qim)
        rot = self._rotation.get(path, 0)
        if rot:
            pm = pm.transformed(QTransform().rotate(rot),
                                Qt.TransformationMode.SmoothTransformation)
        self._pixmaps[path] = pm
        self._pixmaps.move_to_end(path)
        while len(self._pixmaps) > self._pixmap_cap:
            self._pixmaps.popitem(last=False)
        # A previously-failed file that now decodes (e.g. replaced on disk)
        # should drop its failed flag.
        self._failed.discard(path)
        # O(1) row lookup via reverse index -- was O(n) in the original.
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [Qt.ItemDataRole.DecorationRole])

    def _on_thumb_failed(self, path: str) -> None:
        """Record a decode failure so the file isn't re-requested forever."""
        if path not in self._all_set or path in self._failed:
            return
        self._failed.add(path)
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [FailedRole])

    def reload_path(self, path: str) -> None:
        """Drop cached state for a file changed on disk (e.g. after rotation).

        Clears the in-memory pixmap, any failed flag, and any session display
        rotation, then asks the view to repaint that row — the next paint
        re-requests a fresh thumbnail (the disk cache is keyed by mtime, which
        the on-disk change has bumped, so it decodes the new pixels).
        """
        self._pixmaps.pop(path, None)
        self._failed.discard(path)
        self._rotation.pop(path, None)
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [Qt.ItemDataRole.DecorationRole])

    def rotate_path(self, path: str) -> None:
        """Rotate display 90\u00b0 CW (display-only; persists for this session)."""
        self._rotation[path] = (self._rotation.get(path, 0) + 90) % 360
        pm = self._pixmaps.get(path)
        if pm is not None:
            self._pixmaps[path] = pm.transformed(
                QTransform().rotate(90),
                Qt.TransformationMode.SmoothTransformation)
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [Qt.ItemDataRole.DecorationRole])

    def refresh_fav(self, path: str) -> None:
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [FavRole])

    def remove_path(self, path: str) -> None:
        """Remove a path without a full model reset."""
        if path not in self._all_set:
            return
        self._all.remove(path)       # O(n) list remove, but this is rare
        self._all_set.discard(path)
        self._pixmaps.pop(path, None)
        self._failed.discard(path)
        row = self._path_to_row.pop(path, None)
        if row is None:
            return
        # Proper granular removal -- avoids the full model reset the original used.
        self.beginRemoveRows(QModelIndex(), row, row)
        self._rows.pop(row)
        # Shift subsequent row indices down by one.
        for p, r in self._path_to_row.items():
            if r > row:
                self._path_to_row[p] = r - 1
        self.endRemoveRows()

    def remove_paths(self, paths: list[str]) -> None:
        """Remove many paths in one model reset.

        remove_path() shifts every downstream row index O(n) per call, so
        deleting k items one-by-one is O(n*k).  For a multi-selection trash this
        rebuilds the visible rows and reverse index a single time instead.
        """
        drop = {p for p in paths if p in self._all_set}
        if not drop:
            return
        if len(drop) == 1:
            self.remove_path(next(iter(drop)))
            return
        self.beginResetModel()
        self._all = [p for p in self._all if p not in drop]
        self._all_set -= drop
        for p in drop:
            self._pixmaps.pop(p, None)
            self._failed.discard(p)
        self._rows = [p for p in self._rows if p not in drop]
        self._path_to_row = {p: i for i, p in enumerate(self._rows)}
        self.endResetModel()

    def add_path(self, path: str) -> None:
        """Re-insert a path (e.g. after undo-trash). Triggers a full reindex."""
        if path not in self._all_set:
            self._all.append(path)
            self._all_set.add(path)
            self._reindex()

    def update_video_frame(self, path: str, pm: "QPixmap") -> None:
        """Overwrite the displayed pixmap with a live video preview frame."""
        self._pixmaps[path] = pm
        # Respect the LRU memory cap — live frames were bypassing it, letting
        # the pixmap cache grow past its byte budget while videos were visible.
        self._pixmaps.move_to_end(path)
        while len(self._pixmaps) > self._pixmap_cap:
            self._pixmaps.popitem(last=False)
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [Qt.ItemDataRole.DecorationRole])
