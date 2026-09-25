"""Duplicate finder: scan for byte-identical files and offer to trash the
redundant copies.

The scan does NOT require the media to be loaded in the gallery.  Point it at
any folder and it walks that folder itself, so a drive can be de-duplicated
without first importing thousands of files into the grid (which costs a scan,
thumbnails and dimension probes for media you only want to delete).

The heavy lifting (hashing) runs on a worker thread via :mod:`engine.dupes`;
this module is only the results UI.  Each group of duplicates lists its members
with a thumbnail, size, and modified time, and a checkbox.  "Keep newest"
pre-selects every copy except the most recently modified in each group so the
common case — trash the older duplicates — is one click away.  Trashing and
"reveal in gallery" are delegated to the main window through signals, reusing
its existing trash pipeline and selection.
"""
from __future__ import annotations
import datetime
import os

from PySide6.QtCore import Qt, QObject, QRunnable, QThreadPool, Signal
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QScrollArea, QWidget, QFrame, QCheckBox,
                               QPushButton, QProgressBar, QMessageBox,
                               QRadioButton, QButtonGroup, QFileDialog)

from . import config
from .engine import cache, dupes, tags, scan, shell

_THUMB_PX = 72


_fmt_size = config.fmt_bytes


class _ScanSignals(QObject):
    progress = Signal(int, int)
    counted = Signal(int)          # files enumerated so far (folder walk)
    done = Signal(list)


class _ScanJob(QRunnable):
    """Walks any requested folders, then runs find_duplicates() — all off the
    GUI thread, so neither the directory walk nor the hashing blocks the UI."""

    def __init__(self, paths: "list[str]", signals: _ScanSignals,
                 roots: "list[str] | None" = None, recursive: bool = True):
        super().__init__()
        self._paths = list(paths)
        self._roots = list(roots or [])
        self._recursive = recursive
        self._sig = signals
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        paths = list(self._paths)
        # Enumerate the chosen folders here rather than importing them into the
        # gallery first: de-duplicating a drive should not cost a full import.
        for root in self._roots:
            if self._cancelled:
                self._sig.done.emit([])
                return
            self._sig.counted.emit(len(paths))
            try:
                paths.extend(scan.scan_media(root, recursive=self._recursive).paths)
            except Exception as exc:
                print(f"[dupes-scan] {root}: {exc}")
        self._sig.counted.emit(len(paths))
        groups = dupes.find_duplicates(
            paths,
            progress=lambda d, t: self._sig.progress.emit(d, t),
            cancelled=lambda: self._cancelled)
        self._sig.done.emit(groups)


class DuplicatesDialog(QDialog):
    # Ask the main window to act; it owns the model + trash pipeline.
    revealRequested = Signal(str)
    trashRequested = Signal(list)

    def __init__(self, paths: "list[str]" = None, parent=None,
                 roots: "list[str] | None" = None, recursive: bool = True):
        """`paths` are already-loaded files; `roots` are folders to walk.

        Either may be empty — with neither, the dialog opens idle and waits for
        a folder to be added, so the finder is usable with an empty gallery.
        """
        super().__init__(parent)
        self.setWindowTitle("Duplicate finder")
        self.resize(760, 600)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())

        self._checks: "list[QCheckBox]" = []
        self._pool = QThreadPool.globalInstance()
        self._job: "_ScanJob | None" = None
        self._loaded = list(paths or [])
        self._roots: "list[str]" = list(roots or [])
        self._groups: "list[list[str]]" = []
        self._keep_groups = []

        root = QVBoxLayout(self)
        root.addWidget(self._build_source_row(recursive))

        self._header = QLabel("Scanning for duplicates…")
        self._header.setStyleSheet(f"color: {config.FG_MID}; padding: 2px;")
        root.addWidget(self._header)

        self._progress = QProgressBar()
        self._progress.setTextVisible(False)
        root.addWidget(self._progress)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._body = QWidget()
        self._body_lay = QVBoxLayout(self._body)
        self._body_lay.setAlignment(Qt.AlignmentFlag.AlignTop)
        self._scroll.setWidget(self._body)
        root.addWidget(self._scroll, 1)

        btn_row = QHBoxLayout()
        self._keep_newest_btn = QPushButton("Select all but newest")
        self._keep_newest_btn.setToolTip(
            "Tick every copy except the most recently modified in each group")
        self._keep_newest_btn.clicked.connect(lambda: self._select_all_but_newest())
        self._keep_tagged_btn = QPushButton("Select all but best-tagged")
        self._keep_tagged_btn.setToolTip(
            "Tick every copy except the one carrying the most tags in each "
            "group (ties go to the newest) — the same rule the gallery uses "
            "when it hides duplicates")
        self._keep_tagged_btn.clicked.connect(self._select_all_but_best_tagged)
        self._clear_btn = QPushButton("Clear selection")
        self._clear_btn.clicked.connect(self._clear_selection)
        self._trash_btn = QPushButton(f"{config.ICON_TRASH}  Move selected to Trash")
        self._trash_btn.clicked.connect(self._trash_selected)
        btn_row.addWidget(self._keep_newest_btn)
        btn_row.addWidget(self._keep_tagged_btn)
        btn_row.addWidget(self._clear_btn)
        btn_row.addStretch(1)
        btn_row.addWidget(self._trash_btn)
        root.addLayout(btn_row)

        close_row = QHBoxLayout()
        close_row.addStretch(1)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        close_row.addWidget(close_btn)
        root.addLayout(close_row)

        self._set_actions_enabled(False)
        if self._loaded or self._roots:
            self._rescan()
        else:
            self._progress.hide()
            self._header.setText(
                "Nothing to scan yet — add a folder above. "
                "Media does not need to be open in the gallery.")

    # -- where to scan ---------------------------------------------------------
    def _build_source_row(self, recursive: bool) -> QWidget:
        """The strip that chooses WHAT to scan.

        The finder used to scan only what the gallery had already imported,
        which made de-duplicating a drive a two-step chore: import thousands of
        files (paying for thumbnails and dimension probes on media you intend
        to delete), then scan. Folders can be added directly instead.
        """
        box = QFrame()
        box.setObjectName("DupSource")
        box.setStyleSheet("QFrame#DupSource { border: 1px solid %s; "
                          "border-radius: 6px; }" % config.FG_DIM)
        lay = QVBoxLayout(box)
        lay.setContentsMargins(8, 6, 8, 6)

        top = QHBoxLayout()
        self._source_label = QLabel()
        self._source_label.setStyleSheet(f"color: {config.FG_MID};")
        self._source_label.setWordWrap(True)
        top.addWidget(self._source_label, 1)

        add = QPushButton("Add folder…")
        add.setToolTip("Scan a folder directly — it does not need to be open "
                       "in the gallery")
        add.clicked.connect(self._add_folder)
        top.addWidget(add)

        self._clear_roots_btn = QPushButton("Clear folders")
        self._clear_roots_btn.clicked.connect(self._clear_roots)
        top.addWidget(self._clear_roots_btn)

        self._rescan_btn = QPushButton("Rescan")
        self._rescan_btn.clicked.connect(self._rescan)
        top.addWidget(self._rescan_btn)
        lay.addLayout(top)

        opts = QHBoxLayout()
        self._recursive_cb = QCheckBox("Include subfolders")
        self._recursive_cb.setChecked(recursive)
        self._recursive_cb.setToolTip("Walk every folder nested inside the "
                                      "added folders")
        opts.addWidget(self._recursive_cb)

        self._use_loaded_cb = QCheckBox("Also include media open in the gallery")
        self._use_loaded_cb.setChecked(bool(self._loaded))
        self._use_loaded_cb.setEnabled(bool(self._loaded))
        opts.addWidget(self._use_loaded_cb)
        opts.addStretch(1)
        lay.addLayout(opts)

        self._sync_source_label()
        return box

    def _sync_source_label(self) -> None:
        bits = []
        if self._loaded and self._use_loaded_cb.isChecked():
            bits.append(f"{len(self._loaded)} file(s) open in the gallery")
        for r in self._roots:
            bits.append(r)
        self._source_label.setText(
            "Scanning:  " + "\n           ".join(bits) if bits
            else "Scanning:  (nothing selected)")
        self._clear_roots_btn.setEnabled(bool(self._roots))

    def _add_folder(self) -> None:
        start = self._roots[-1] if self._roots else ""
        folder = QFileDialog.getExistingDirectory(
            self, "Choose a folder to scan for duplicates", start)
        if not folder:
            return
        if folder not in self._roots:
            self._roots.append(folder)
        self._sync_source_label()
        self._rescan()

    def _clear_roots(self) -> None:
        self._roots.clear()
        self._sync_source_label()
        self._rescan()

    def _rescan(self) -> None:
        if self._job is not None:
            self._job.cancel()
        self._sync_source_label()
        loaded = self._loaded if self._use_loaded_cb.isChecked() else []
        if not loaded and not self._roots:
            self._progress.hide()
            self._populate([])
            self._header.setText("Nothing to scan — add a folder above.")
            return
        self._progress.show()
        self._header.setText("Scanning for duplicates…")
        self._start_scan(loaded, self._roots,
                         self._recursive_cb.isChecked())

    # -- scan lifecycle --------------------------------------------------------
    def _start_scan(self, paths: "list[str]", roots: "list[str] | None" = None,
                    recursive: bool = True) -> None:
        self._sig = _ScanSignals(self)
        self._sig.progress.connect(self._on_progress)
        self._sig.counted.connect(self._on_counted)
        self._sig.done.connect(self._on_done)
        self._progress.setRange(0, 0)               # indeterminate until stage 3
        self._job = _ScanJob(paths, self._sig, roots, recursive)
        self._pool.start(self._job)

    def _on_counted(self, n: int) -> None:
        self._header.setText(f"Enumerating…  {n} media file(s) found")

    def _on_progress(self, done: int, total: int) -> None:
        if total > 0:
            self._progress.setRange(0, total)
            self._progress.setValue(done)

    def _on_done(self, groups: "list[list[str]]") -> None:
        self._job = None
        self._progress.hide()
        self._populate(groups)

    def reject(self) -> None:
        if self._job is not None:
            self._job.cancel()
        super().reject()

    # -- results UI ------------------------------------------------------------
    def _populate(self, groups: "list[list[str]]") -> None:
        self._checks.clear()
        self._keep_groups = []
        # Clear any prior body content (populate may run again after trashing).
        while self._body_lay.count():
            item = self._body_lay.takeAt(0)
            w = item.widget()
            if w is not None:
                w.deleteLater()

        self._groups = groups
        n_dupes = sum(len(g) - 1 for g in groups)
        # Count only bytes that deleting would ACTUALLY free: hard links are
        # extra names for one file, so trashing them reclaims nothing.
        reclaim = sum(dupes.reclaimable_bytes(g) for g in groups)
        linked = sum(1 for g in groups if dupes.is_all_one_file(g))
        if not groups:
            self._header.setText("No duplicates found.")
            self._set_actions_enabled(False)
            return
        head = (f"{len(groups)} group(s)  ·  {n_dupes} redundant copy(ies)  ·  "
                f"{_fmt_size(reclaim)} reclaimable")
        if linked:
            head += (f"   ({linked} group(s) are hard links to one file — "
                     "deleting those frees nothing)")
        self._header.setText(head)
        self._set_actions_enabled(True)

        for gi, group in enumerate(groups):
            self._body_lay.addWidget(self._group_widget(gi, group))

    def _group_widget(self, gi: int, group: "list[str]") -> QWidget:
        box = QFrame()
        box.setObjectName("DupGroup")
        box.setStyleSheet(
            "QFrame#DupGroup { border: 1px solid %s; border-radius: 6px; "
            "margin: 2px; }" % config.FG_DIM)
        lay = QVBoxLayout(box)
        one_file = dupes.is_all_one_file(group)
        title = QLabel(f"Group {gi + 1}  ·  {len(group)} copies  ·  "
                       f"{_fmt_size(self._size_of(group[0]))} each"
                       + ("  ·  frees "
                          f"{_fmt_size(dupes.reclaimable_bytes(group))}"
                          if not one_file else ""))
        title.setStyleSheet(f"color: {config.FG_MID}; font-weight: bold;")
        lay.addWidget(title)
        if one_file:
            # Under link_mode=hardlink the favourites/tag-folder mirrors are
            # extra NAMES for the same bytes. Byte comparison cannot tell them
            # from real copies, so say it outright rather than let the user
            # "reclaim" space that was never used twice.
            note = QLabel(
                "⛓  These are hard links to a single file on disk — it is "
                "stored once. Deleting any of them frees no space until the "
                "last one goes.")
            note.setStyleSheet(f"color: {config.ACCENT}; font-size: 11px;")
            note.setWordWrap(True)
            lay.addWidget(note)

        # Where the copies live — the practical question when choosing which to
        # keep — plus a warning when they don't share the same tags.
        folders = dupes.locations(group)
        loc = QLabel("Found in:  " + "     ".join(folders))
        loc.setStyleSheet(f"color: {config.FG_DIM}; font-size: 11px;")
        loc.setWordWrap(True)
        loc.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lay.addWidget(loc)
        if dupes.tags_differ(group):
            warn = QLabel(
                "⚠  These copies carry different tags — the copy you keep will "
                f"inherit the fullest set: {', '.join(dupes.richest_tags(group)) or '(none)'}")
            warn.setStyleSheet(f"color: {config.ACCENT}; font-size: 11px;")
            warn.setWordWrap(True)
            lay.addWidget(warn)

        # One "keep" radio per group: choosing a keeper ticks every other copy
        # for deletion, which is how people actually think about duplicates.
        keep_group = QButtonGroup(box)
        keep_group.setExclusive(True)
        self._keep_groups.append((keep_group, list(group)))

        newest = self._newest_in(group)
        for path in group:
            lay.addWidget(self._file_row(path, is_newest=(path == newest),
                                         keep_group=keep_group, group=group))
        return box

    def _file_row(self, path: str, is_newest: bool,
                  keep_group=None, group=None) -> QWidget:
        row = QWidget()
        h = QHBoxLayout(row)
        h.setContentsMargins(4, 2, 4, 2)

        cb = QCheckBox()
        cb.setProperty("path", path)
        self._checks.append(cb)
        h.addWidget(cb)

        if keep_group is not None:
            keep = QRadioButton("Keep")
            keep.setToolTip("Keep this copy and tick the rest for deletion")
            keep.setProperty("path", path)
            keep_group.addButton(keep)
            keep.clicked.connect(
                lambda _=False, p=path, g=list(group): self._choose_keeper(p, g))
            h.addWidget(keep)

        thumb = QLabel()
        thumb.setFixedSize(_THUMB_PX, _THUMB_PX)
        thumb.setAlignment(Qt.AlignmentFlag.AlignCenter)
        pm = self._thumb(path)
        if pm is not None:
            thumb.setPixmap(pm)
        else:
            thumb.setText("?")
        h.addWidget(thumb)

        when = ""
        try:
            when = datetime.datetime.fromtimestamp(
                os.stat(path).st_mtime).strftime("%Y-%m-%d %H:%M")
        except OSError:
            pass
        tag = "   (newest)" if is_newest else ""
        own = tags.tags_for(path)
        tagline = f"  ·  tags: {', '.join(own)}" if own else "  ·  untagged"
        info = QLabel(f"{os.path.basename(path)}{tag}\n"
                      f"    {os.path.dirname(path)}  ·  {when}{tagline}")
        info.setStyleSheet(f"color: {config.FG_MID};")
        info.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        h.addWidget(info, 1)

        reveal = QPushButton("Reveal")
        reveal.setToolTip("Select this file in the gallery")
        reveal.clicked.connect(lambda _=False, p=path: self.revealRequested.emit(p))
        h.addWidget(reveal)

        # Folder-scanned files are usually NOT in the gallery, so "reveal" there
        # would do nothing; the file manager always works.
        show = QPushButton("Open folder")
        show.setToolTip("Show this file in your file manager")
        show.clicked.connect(lambda _=False, p=path: shell.reveal_path(p))
        h.addWidget(show)
        return row

    def _thumb(self, path: str) -> "QPixmap | None":
        try:
            qim = cache.get_thumbnail(path, _THUMB_PX)
        except Exception:
            qim = None
        if qim is None or qim.isNull():
            return None
        return QPixmap.fromImage(qim).scaled(
            _THUMB_PX, _THUMB_PX,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation)

    # -- helpers ---------------------------------------------------------------
    @staticmethod
    def _size_of(path: str) -> int:
        try:
            return os.stat(path).st_size
        except OSError:
            return 0

    @staticmethod
    def _newest_in(group: "list[str]") -> str:
        def mtime(p):
            try:
                return os.stat(p).st_mtime
            except OSError:
                return 0.0
        return max(group, key=mtime)

    def _set_actions_enabled(self, on: bool) -> None:
        for b in (self._keep_newest_btn, self._keep_tagged_btn,
                  self._clear_btn, self._trash_btn):
            b.setEnabled(on)

    def _select_all_but_newest(self) -> None:
        for cb in self._checks:
            path = cb.property("path")
            group = next((g for g in self._groups if path in g), None)
            cb.setChecked(bool(group) and path != self._newest_in(group))

    def _select_all_but_best_tagged(self) -> None:
        for cb in self._checks:
            path = cb.property("path")
            group = next((g for g in self._groups if path in g), None)
            cb.setChecked(bool(group) and path != dupes.best_tagged(group))

    def _choose_keeper(self, keep: str, group: "list[str]") -> None:
        """Keep `keep` and tick every other copy in its group for deletion."""
        for cb in self._checks:
            p = cb.property("path")
            if p in group:
                cb.setChecked(p != keep)

    def _survivors(self, doomed: "set[str]") -> "list[tuple[list[str], list[str]]]":
        """(group, survivors) for every group losing at least one copy."""
        out = []
        for g in self._groups:
            if any(p in doomed for p in g):
                out.append((list(g), [p for p in g if p not in doomed]))
        return out

    def _clear_selection(self) -> None:
        for cb in self._checks:
            cb.setChecked(False)

    def _selected_paths(self) -> "list[str]":
        return [cb.property("path") for cb in self._checks if cb.isChecked()]

    def _trash_selected(self) -> None:
        paths = self._selected_paths()
        if not paths:
            return
        # Guard against emptying a whole group (leaving zero copies): warn only.
        for g in self._groups:
            if all(p in paths for p in g):
                if QMessageBox.question(
                        self, "Delete every copy?",
                        "Your selection would trash every copy in at least one "
                        "group, leaving nothing. Continue anyway?",
                        QMessageBox.StandardButton.Yes
                        | QMessageBox.StandardButton.No
                        ) != QMessageBox.StandardButton.Yes:
                    return
                break
        # Preserve tagging work: before the redundant copies go, give each
        # survivor the fullest tag set found anywhere in its group, so tags
        # applied only to a copy being deleted aren't silently lost.
        gone = set(paths)
        stamped = 0
        for group, survivors in self._survivors(gone):
            stamped += len(dupes.stamp_richest_tags(group, survivors))

        self.trashRequested.emit(paths)
        if stamped:
            self._header.setText(
                f"{self._header.text()}   ·   tags merged onto {stamped} kept file(s)")
        # Drop the trashed paths and re-group what remains.
        remaining = [[p for p in g if p not in gone] for g in self._groups]
        self._populate([g for g in remaining if len(g) > 1])
