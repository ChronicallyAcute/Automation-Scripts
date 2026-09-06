"""Duplicate finder: scan the loaded media for byte-identical files and offer
to trash the redundant copies.

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
                               QRadioButton, QButtonGroup)

from . import config
from .engine import cache, dupes, tags

_THUMB_PX = 72


def _fmt_size(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes} B"
    kb = nbytes / 1024
    if kb < 1024:
        return f"{kb:.0f} KB"
    mb = kb / 1024
    return f"{mb:.1f} MB" if mb < 1024 else f"{mb / 1024:.2f} GB"


class _ScanSignals(QObject):
    progress = Signal(int, int)
    done = Signal(list)


class _ScanJob(QRunnable):
    """Runs find_duplicates() off the GUI thread."""
    def __init__(self, paths: "list[str]", signals: _ScanSignals):
        super().__init__()
        self._paths = list(paths)
        self._sig = signals
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        groups = dupes.find_duplicates(
            self._paths,
            progress=lambda d, t: self._sig.progress.emit(d, t),
            cancelled=lambda: self._cancelled)
        self._sig.done.emit(groups)


class DuplicatesDialog(QDialog):
    # Ask the main window to act; it owns the model + trash pipeline.
    revealRequested = Signal(str)
    trashRequested = Signal(list)

    def __init__(self, paths: "list[str]", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Duplicate finder")
        self.resize(720, 560)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())

        self._checks: "list[QCheckBox]" = []
        self._pool = QThreadPool.globalInstance()
        self._job: "_ScanJob | None" = None

        root = QVBoxLayout(self)

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
        self._clear_btn = QPushButton("Clear selection")
        self._clear_btn.clicked.connect(self._clear_selection)
        self._trash_btn = QPushButton(f"{config.ICON_TRASH}  Move selected to Trash")
        self._trash_btn.clicked.connect(self._trash_selected)
        btn_row.addWidget(self._keep_newest_btn)
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
        self._start_scan(paths)

    # -- scan lifecycle --------------------------------------------------------
    def _start_scan(self, paths: "list[str]") -> None:
        self._sig = _ScanSignals(self)
        self._sig.progress.connect(self._on_progress)
        self._sig.done.connect(self._on_done)
        self._progress.setRange(0, 0)               # indeterminate until stage 3
        self._job = _ScanJob(paths, self._sig)
        self._pool.start(self._job)

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
        reclaim = sum(self._size_of(g[0]) * (len(g) - 1) for g in groups)
        if not groups:
            self._header.setText("No duplicates found.")
            self._set_actions_enabled(False)
            return
        self._header.setText(
            f"{len(groups)} group(s)  ·  {n_dupes} redundant copy(ies)  ·  "
            f"{_fmt_size(reclaim)} reclaimable")
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
        title = QLabel(f"Group {gi + 1}  ·  {len(group)} copies  ·  "
                       f"{_fmt_size(self._size_of(group[0]))} each")
        title.setStyleSheet(f"color: {config.FG_MID}; font-weight: bold;")
        lay.addWidget(title)

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
        for b in (self._keep_newest_btn, self._clear_btn, self._trash_btn):
            b.setEnabled(on)

    def _select_all_but_newest(self) -> None:
        for cb in self._checks:
            path = cb.property("path")
            group = next((g for g in self._groups if path in g), None)
            cb.setChecked(bool(group) and path != self._newest_in(group))

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
