"""Size triage: which folder is eating the drive, biggest first.

Answers the question a file manager makes awkward — "where did the space go?"
— by measuring every child of a folder and listing them largest first, with a
bar so the distribution is readable at a glance. Double-click descends, so a
200GB total can be tracked down to the folder actually responsible in a few
clicks.

Measuring runs on a worker thread and is cancellable: pointing this at the root
of a large drive is a legitimate thing to do and must not freeze the UI.
"""
from __future__ import annotations
import os

from PySide6.QtCore import Qt, QObject, QRunnable, QThreadPool, Signal
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QPushButton, QProgressBar, QTreeWidget,
                               QTreeWidgetItem, QFileDialog, QCheckBox,
                               QHeaderView)

from . import config
from .engine import foldersize, shell


class _Signals(QObject):
    progress = Signal(int, int, str)
    done = Signal(list, str)


class _MeasureJob(QRunnable):
    def __init__(self, folder: str, signals: _Signals):
        super().__init__()
        self._folder = folder
        self._sig = signals
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        rows = foldersize.children_sizes(
            self._folder,
            cancelled=lambda: self._cancelled,
            progress=lambda i, n, name: self._sig.progress.emit(i, n, name))
        if not self._cancelled:
            self._sig.done.emit(rows, self._folder)


class SizeTriageDialog(QDialog):
    """Largest-first listing of a folder's contents."""

    importRequested = Signal(str)          # folder the user wants opened
    dupesRequested = Signal(str)           # folder to scan for duplicates

    def __init__(self, folder: str = "", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Size triage — where the space went")
        self.resize(820, 600)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())

        self._pool = QThreadPool.globalInstance()
        self._job: "_MeasureJob | None" = None
        self._folder = folder or os.path.expanduser("~")
        self._rows: "list[foldersize.Entry]" = []

        root = QVBoxLayout(self)

        nav = QHBoxLayout()
        self._up_btn = QPushButton("↑  Up")
        self._up_btn.clicked.connect(self._go_up)
        nav.addWidget(self._up_btn)
        choose = QPushButton("Choose folder…")
        choose.clicked.connect(self._choose)
        nav.addWidget(choose)
        self._path_label = QLabel()
        self._path_label.setStyleSheet(f"color: {config.FG_MID};")
        self._path_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse)
        nav.addWidget(self._path_label, 1)
        rescan = QPushButton("Rescan")
        rescan.clicked.connect(self._rescan)
        nav.addWidget(rescan)
        root.addLayout(nav)

        self._media_only = QCheckBox("Media files only")
        self._media_only.setToolTip(
            "Hide non-media files from the listing (folder totals still count "
            "everything inside them)")
        self._media_only.toggled.connect(self._repopulate)
        root.addWidget(self._media_only)

        self._progress = QProgressBar()
        self._progress.setTextVisible(True)
        root.addWidget(self._progress)

        self._tree = QTreeWidget()
        self._tree.setColumnCount(4)
        self._tree.setHeaderLabels(["Name", "Size", "Files", "Share"])
        self._tree.setRootIsDecorated(False)
        self._tree.setAlternatingRowColors(True)
        self._tree.itemDoubleClicked.connect(self._on_double)
        hdr = self._tree.header()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        root.addWidget(self._tree, 1)

        self._total_label = QLabel()
        self._total_label.setStyleSheet(f"color: {config.FG_MID};")
        root.addWidget(self._total_label)

        actions = QHBoxLayout()
        open_btn = QPushButton("Open in file manager")
        open_btn.clicked.connect(self._reveal_selected)
        actions.addWidget(open_btn)
        dupes_btn = QPushButton("Find duplicates here")
        dupes_btn.setToolTip("Scan the selected folder for byte-identical "
                             "copies — no import needed")
        dupes_btn.clicked.connect(self._dupes_selected)
        actions.addWidget(dupes_btn)
        imp = QPushButton("Open in gallery")
        imp.clicked.connect(self._import_selected)
        actions.addWidget(imp)
        actions.addStretch(1)
        close = QPushButton("Close")
        close.clicked.connect(self.accept)
        actions.addWidget(close)
        root.addLayout(actions)

        self._rescan()

    # -- navigation ------------------------------------------------------------
    def _go_up(self) -> None:
        parent = os.path.dirname(self._folder.rstrip(os.sep))
        if parent and parent != self._folder and os.path.isdir(parent):
            self._folder = parent
            self._rescan()

    def _choose(self) -> None:
        folder = QFileDialog.getExistingDirectory(
            self, "Choose a folder to measure", self._folder)
        if folder:
            self._folder = folder
            self._rescan()

    def _on_double(self, item: QTreeWidgetItem, _col: int) -> None:
        path = item.data(0, Qt.ItemDataRole.UserRole)
        if path and os.path.isdir(path):
            self._folder = path
            self._rescan()

    # -- measuring -------------------------------------------------------------
    def _rescan(self) -> None:
        if self._job is not None:
            self._job.cancel()
        self._path_label.setText(self._folder)
        self._tree.clear()
        self._total_label.setText("")
        self._progress.setRange(0, 0)
        self._progress.show()
        self._up_btn.setEnabled(
            bool(os.path.dirname(self._folder.rstrip(os.sep))))
        self._sig = _Signals(self)
        self._sig.progress.connect(self._on_progress)
        self._sig.done.connect(self._on_done)
        self._job = _MeasureJob(self._folder, self._sig)
        self._pool.start(self._job)

    def _on_progress(self, i: int, n: int, name: str) -> None:
        self._progress.setRange(0, max(n, 1))
        self._progress.setValue(i)
        self._progress.setFormat(f"Measuring {name}  ({i}/{n})")

    def _on_done(self, rows: list, folder: str) -> None:
        self._job = None
        if folder != self._folder:
            return                     # a newer scan superseded this one
        self._progress.hide()
        self._rows = rows
        self._repopulate()

    def reject(self) -> None:
        if self._job is not None:
            self._job.cancel()
        super().reject()

    # -- listing ---------------------------------------------------------------
    def _visible_rows(self) -> "list":
        if not self._media_only.isChecked():
            return self._rows
        return [e for e in self._rows if e.is_dir
                or os.path.splitext(e.name.lower())[1] in config.SUPPORTED]

    def _repopulate(self) -> None:
        self._tree.clear()
        rows = self._visible_rows()
        biggest = max((e.bytes for e in rows), default=0)
        total = sum(e.bytes for e in rows)
        for e in rows:
            share = (e.bytes / biggest) if biggest else 0
            # A 20-cell bar reads as a distribution at a glance, which a column
            # of numbers does not.
            bar = "█" * max(0, round(share * 20))
            item = QTreeWidgetItem([
                ("📁  " if e.is_dir else "     ") + e.name
                + ("  (partial)" if e.partial else ""),
                foldersize.fmt_size(e.bytes),
                f"{e.files:,}",
                bar,
            ])
            item.setData(0, Qt.ItemDataRole.UserRole, e.path)
            # Sort numerically, not by the formatted string ("9 B" > "10 GB").
            item.setData(1, Qt.ItemDataRole.UserRole, e.bytes)
            item.setTextAlignment(1, Qt.AlignmentFlag.AlignRight)
            item.setTextAlignment(2, Qt.AlignmentFlag.AlignRight)
            self._tree.addTopLevelItem(item)
        for col in (1, 2):
            self._tree.resizeColumnToContents(col)
        self._total_label.setText(
            f"{len(rows)} item(s)  ·  {foldersize.fmt_size(total)} total"
            + ("   —  double-click a folder to go deeper" if rows else
               "   —  this folder is empty"))

    # -- actions ---------------------------------------------------------------
    def selected_path(self) -> str:
        item = self._tree.currentItem()
        if item is not None:
            return item.data(0, Qt.ItemDataRole.UserRole) or ""
        return self._folder

    def _reveal_selected(self) -> None:
        shell.reveal_path(self.selected_path())

    def _dupes_selected(self) -> None:
        p = self.selected_path()
        self.dupesRequested.emit(p if os.path.isdir(p) else self._folder)

    def _import_selected(self) -> None:
        p = self.selected_path()
        self.importRequested.emit(p if os.path.isdir(p) else self._folder)
