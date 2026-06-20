"""Virtualized list model backing the gallery grid.

QListView only calls data() for items it is about to paint, so memory stays
bounded automatically — no manual hydrate/dehydrate needed.  Thumbnails are
fetched lazily through the threaded loader and cached (LRU-capped) as QPixmaps.

Fork improvements over gallery_qt.model:
  • _all_set — O(1) membership test for the master path list (was implicit
    O(n) via list membership checks in the original).
  • _path_to_row — O(1) reverse lookup from path → current row index.
    The original did an O(n) linear scan on *every* thumbnail arrival,
    favourite toggle, and rotation — producing O(n²) behaviour when
    thousands of thumbnails arrive in rapid succession.
  • add_paths_batch / finalize_scan — progressive streaming population.
    add_paths_batch() appends accepted items via beginInsertRows so the
    view shows media as soon as the first batch arrives from the scanner.
    finalize_scan() re-sorts everything once scanning is complete.
  • remove_path uses beginRemoveRows / endRemoveRows instead of a full model
    reset, so deleting a single file doesn't force the view to repaint every
    visible cell.
  • Adaptive pixmap cap — the in-RAM pixmap budget is expressed in megabytes
    (default 256 MB) and converted to an item count based on the current
    thumbnail size, rather than a fixed 400-item ceiling that left large
    thumbnails wasting far more RAM than intended.
"""
from __future__ import annotations
import os
from collections import OrderedDict

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt, QSize
from PySide6.QtGui import QImage, QPixmap, QTransform

from . import config
from .engine import media
from .loader import ThumbnailLoader
from .engine.favorites import Favorites

PathRole    = Qt.ItemDataRole.UserRole + 1
IsVideoRole = Qt.ItemDataRole.UserRole + 2
FavRole     = Qt.ItemDataRole.UserRole + 3
LoadedRole  = Qt.ItemDataRole.UserRole + 4

# Target memory ceiling for the in-process pixmap LRU cache.
# At 320 px thumbnails (≈ 400 KB RGBA each) this yields ~640 items ≈ 256 MB.
_PIXMAP_MEM_CAP_MB = 256


def _pixmap_cap(thumb_px: int) -> int:
    """Max number of pixmaps that fits within the memory ceiling."""
    bytes_each = max(1, thumb_px) ** 2 * 4   # worst-case RGBA square
    cap = int(_PIXMAP_MEM_CAP_MB * 1024 * 1024 / bytes_each)
    return max(100, min(cap, 2000))


class GalleryModel(QAbstractListModel):
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
        self._thumb_px = 320
        self._pixmap_cap = _pixmap_cap(self._thumb_px)
        # filters / sort
        self._show_images = True
        self._show_videos = True
        self._query = ""
        self._sort = "name"
        self._descending = False
        self._loader.ready.connect(self._on_thumb_ready)

    # ── population ────────────────────────────────────────────────────────────
    def set_paths(self, paths: list[str]) -> None:
        """Replace the full path list and re-index (non-streaming)."""
        self._all = list(paths)
        self._all_set = set(paths)
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

    # ── filter helpers ────────────────────────────────────────────────────────
    def _passes_filter(self, p: str) -> bool:
        vid = media.is_video(p)
        if vid and not self._show_videos:
            return False
        if (not vid) and not self._show_images:
            return False
        if self._query and self._query not in os.path.basename(p).lower():
            return False
        return True

    # ── filtering / sorting ──────────────────────────────────────────────────
    def set_filter(self, images: bool, videos: bool, query: str) -> None:
        self._show_images, self._show_videos = images, videos
        self._query = query.strip().lower()
        self._reindex()

    def set_sort(self, mode: str) -> None:
        self._sort = mode
        self._reindex()

    def set_descending(self, descending: bool) -> None:
        self._descending = bool(descending)
        self._reindex()

    def descending(self) -> bool:
        return self._descending

    def sort_mode(self) -> str:
        return self._sort

    def needs_dimensions(self) -> bool:
        return self._sort in ("area", "width", "height")

    def set_dims(self, dims: dict[str, tuple[int, int]]) -> None:
        """Supply (w, h) per path (from a background scan); re-sort if needed."""
        self._dims.update(dims)
        if self.needs_dimensions():
            self._reindex()

    def _dim_area(self, p: str) -> int:
        w, h = self._dims.get(p, (0, 0))
        return w * h

    def _reindex(self) -> None:
        self.beginResetModel()
        rows = [p for p in self._all if self._passes_filter(p)]
        name = lambda p: os.path.basename(p).lower()
        if self._sort == "name":
            rows.sort(key=name)
        elif self._sort == "img_first":
            rows.sort(key=lambda p: (media.is_video(p), name(p)))
        elif self._sort == "vid_first":
            rows.sort(key=lambda p: (not media.is_video(p), name(p)))
        elif self._sort == "favorites":
            rows.sort(key=lambda p: (not self._favs.is_fav(p), name(p)))
        elif self._sort == "area":
            rows.sort(key=lambda p: (self._dim_area(p), name(p)))
        elif self._sort == "width":
            rows.sort(key=lambda p: (self._dims.get(p, (0, 0))[0], name(p)))
        elif self._sort == "height":
            rows.sort(key=lambda p: (self._dims.get(p, (0, 0))[1], name(p)))
        if self._descending:
            rows.reverse()
        self._rows = rows
        # Rebuild reverse index in one pass — O(n), amortised over many lookups.
        self._path_to_row = {p: i for i, p in enumerate(rows)}
        self.endResetModel()

    # ── Qt model interface ────────────────────────────────────────────────────
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
        if role == Qt.ItemDataRole.DecorationRole:
            pm = self._pixmaps.get(path)
            if pm is not None:
                self._pixmaps.move_to_end(path)
                return pm
            self._loader.request(path, self._thumb_px)
            return None
        return None

    # ── thumbnail arrival ─────────────────────────────────────────────────────
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
        # O(1) row lookup via reverse index — was O(n) in the original.
        row = self._path_to_row.get(path)
        if row is not None:
            idx = self.index(row)
            self.dataChanged.emit(idx, idx, [Qt.ItemDataRole.DecorationRole])

    def rotate_path(self, path: str) -> None:
        """Rotate display 90° CW (display-only; persists for this session)."""
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
        row = self._path_to_row.pop(path, None)
        if row is None:
            return
        # Proper granular removal — avoids the full model reset the original used.
        self.beginRemoveRows(QModelIndex(), row, row)
        self._rows.pop(row)
        # Shift subsequent row indices down by one.
        for p, r in self._path_to_row.items():
            if r > row:
                self._path_to_row[p] = r - 1
        self.endRemoveRows()

    def add_path(self, path: str) -> None:
        """Re-insert a path (e.g. after undo-trash). Triggers a full reindex."""
        if path not in self._all_set:
            self._all.append(path)
            self._all_set.add(path)
            self._reindex()
