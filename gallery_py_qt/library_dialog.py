"""Tidy the tag library: pick the real tags, merge their folders, relink copies.

FAVORITES_DIR holds the app's per-tag folders and the user's own standalone
media folders in the same namespace, so the tag-recovery pass adopted every
directory name as a tag. This dialog separates them, and it never guesses:
the names are listed with a suggestion ticked and nothing happens until the
user confirms.

Every step previews first, and no step deletes user media — folders dropped
from the tag set stay exactly where they are.
"""
from __future__ import annotations
import os

from PySide6.QtCore import Qt, QObject, QRunnable, QThreadPool, Signal
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QPushButton, QTreeWidget, QTreeWidgetItem,
                               QMessageBox, QProgressBar, QFileDialog,
                               QCheckBox, QHeaderView, QWidget)

from . import config
from .engine import favorites, foldersize, taglibrary, tags as _tags


class _Signals(QObject):
    progress = Signal(str)
    done = Signal(object)


class _RelinkJob(QRunnable):
    """Index originals and plan the relink off the GUI thread (walks disk)."""

    def __init__(self, sources: "list[str]", roots: "list[str]",
                 signals: _Signals):
        super().__init__()
        self._sources = list(sources)
        self._roots = list(roots)
        self._sig = signals

    def run(self) -> None:
        try:
            self._sig.progress.emit("Indexing originals by filename…")
            index = taglibrary.index_originals(self._sources,
                                               exclude=self._roots)
            self._sig.progress.emit(
                f"Indexed {len(index)} name(s). Scanning the tag folders…")
            plan = taglibrary.plan_relink(self._roots, index)
            self._sig.done.emit(plan)
        except Exception as exc:                   # pragma: no cover
            self._sig.done.emit(exc)


class TagLibraryDialog(QDialog):
    """Pick the real tags, then merge / prune / relink."""

    changed = Signal()                   # tag set or folders altered

    def __init__(self, media_folders: "list[str] | None" = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Tidy the tag library")
        self.resize(760, 640)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())
        self._media_folders = list(media_folders or [])
        self._pool = QThreadPool.globalInstance()
        self._plan: "list[dict]" = []

        root = QVBoxLayout(self)

        intro = QLabel(
            f"<b>{config.FAVORITES_DIR}</b> holds this app's per-tag folders "
            "and your own media folders side by side, which is why folder "
            "names ended up as tag buttons.<br><br>"
            "Tick the names that are <b>real tags</b>. Their folders move into "
            f"<b>{taglibrary.TAG_FOLDERS_DIRNAME}</b>; everything unticked is "
            "removed from the tag list only — <b>no folder or file is "
            "deleted</b>.")
        intro.setWordWrap(True)
        intro.setStyleSheet(f"color: {config.FG_MID};")
        root.addWidget(intro)

        self._tree = QTreeWidget()
        self._tree.setColumnCount(4)
        self._tree.setHeaderLabels(["Name", "In tag list", "Files", "Folder"])
        self._tree.setRootIsDecorated(False)
        self._tree.header().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch)
        root.addWidget(self._tree, 1)

        picks = QHBoxLayout()
        for label, fn in (("Tick suggested", self._tick_suggested),
                          ("Tick all", lambda: self._set_all(True)),
                          ("Untick all", lambda: self._set_all(False))):
            b = QPushButton(label)
            b.clicked.connect(fn)
            picks.addWidget(b)
        picks.addStretch(1)
        self._count = QLabel()
        self._count.setStyleSheet(f"color: {config.FG_DIM};")
        picks.addWidget(self._count)
        root.addLayout(picks)

        self._relink_cb = QCheckBox(
            "Also replace copies in the tag / Favorites folders with links")
        self._relink_cb.setChecked(True)
        self._relink_cb.setToolTip(
            "Matches each copy to an original by filename and swaps it for a "
            "link, reclaiming the space the copy uses")
        root.addWidget(self._relink_cb)

        self._status = QLabel("")
        self._status.setWordWrap(True)
        self._status.setStyleSheet(f"color: {config.FG_MID};")
        root.addWidget(self._status)
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.hide()
        root.addWidget(self._progress)

        actions = QHBoxLayout()
        actions.addStretch(1)
        cancel = QPushButton("Close")
        cancel.clicked.connect(self.reject)
        actions.addWidget(cancel)
        self._apply_btn = QPushButton("Apply")
        self._apply_btn.setDefault(True)
        self._apply_btn.clicked.connect(self._apply)
        actions.addWidget(self._apply_btn)
        root.addLayout(actions)

        self._populate()

    # -- the list ---------------------------------------------------------------
    def _populate(self) -> None:
        self._tree.clear()
        for e in taglibrary.survey():
            item = QTreeWidgetItem([
                e["name"],
                "yes" if e["in_tag_set"] else "—",
                str(e["files"]) if e["folder"] else "—",
                e["folder"] or "(no folder)",
            ])
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(0, Qt.CheckState.Checked if e["suggested"]
                               else Qt.CheckState.Unchecked)
            item.setData(0, Qt.ItemDataRole.UserRole, e["name"])
            self._tree.addTopLevelItem(item)
        for col in (1, 2):
            self._tree.resizeColumnToContents(col)
        self._tree.itemChanged.connect(lambda *_: self._sync_count())
        self._sync_count()

    def _items(self) -> "list[QTreeWidgetItem]":
        return [self._tree.topLevelItem(i)
                for i in range(self._tree.topLevelItemCount())]

    def checked_names(self) -> "list[str]":
        return [it.data(0, Qt.ItemDataRole.UserRole) for it in self._items()
                if it.checkState(0) == Qt.CheckState.Checked]

    def _set_all(self, on: bool) -> None:
        state = Qt.CheckState.Checked if on else Qt.CheckState.Unchecked
        for it in self._items():
            it.setCheckState(0, state)

    def _tick_suggested(self) -> None:
        for it in self._items():
            name = it.data(0, Qt.ItemDataRole.UserRole)
            it.setCheckState(0, Qt.CheckState.Checked
                             if taglibrary.looks_like_tag(name)
                             else Qt.CheckState.Unchecked)

    def _sync_count(self) -> None:
        keep = len(self.checked_names())
        self._count.setText(
            f"{keep} tag(s) kept  ·  "
            f"{len(self._items()) - keep} removed from the tag list")

    # -- apply -------------------------------------------------------------------
    def _apply(self) -> None:
        keep = self.checked_names()
        drop = [it.data(0, Qt.ItemDataRole.UserRole) for it in self._items()
                if it.checkState(0) != Qt.CheckState.Checked]
        if not keep and QMessageBox.question(
                self, "No tags kept",
                "Nothing is ticked, so the tag list will be emptied. "
                "No folders or files are deleted. Continue?"
                ) != QMessageBox.StandardButton.Yes:
            return
        moves = taglibrary.plan_merge(keep)
        sample = "\n".join(f"  • {os.path.basename(a)}" for a, _b in moves[:10])
        if QMessageBox.question(
                self, "Apply these changes?",
                f"Keep {len(keep)} tag(s); remove {len(drop)} name(s) from the "
                "tag list.\n\n"
                f"Move {len(moves)} folder(s) into "
                f"{taglibrary.TAG_FOLDERS_DIRNAME}:\n{sample}"
                + (f"\n  … and {len(moves) - 10} more" if len(moves) > 10 else "")
                + "\n\nThe dropped names keep their folders and every file in "
                  "them. Their tag records are kept too, so re-ticking a name "
                  "brings it straight back.\n\nProceed?"
                ) != QMessageBox.StandardButton.Yes:
            return

        moved, errors = taglibrary.merge_tag_folders(keep)
        dropped = taglibrary.prune_tag_set(keep)
        self._status.setText(
            f"Moved {moved} folder(s); removed {len(dropped)} name(s) from the "
            "tag list.")
        self.changed.emit()
        if errors:
            QMessageBox.warning(self, "Some folders could not be moved",
                                "\n".join(errors[:15]))
        if self._relink_cb.isChecked():
            self._start_relink(keep)
        else:
            QMessageBox.information(self, "Done", self._status.text())

    # -- relink ------------------------------------------------------------------
    def _start_relink(self, keep: "list[str]") -> None:
        start = (self._media_folders[0] if self._media_folders
                 else os.path.expanduser("~"))
        source = QFileDialog.getExistingDirectory(
            self,
            "Where do the ORIGINAL media files live? (searched recursively)",
            os.path.dirname(start) if os.path.isfile(start) else start)
        if not source:
            QMessageBox.information(
                self, "Skipped",
                self._status.text() + "\n\nRelinking skipped — no source "
                "folder chosen.")
            return
        roots = taglibrary.mirror_roots(self._media_folders, keep)
        if not roots:
            QMessageBox.information(self, "Nothing to relink",
                                    "No tag or Favorites folders were found.")
            return
        self._apply_btn.setEnabled(False)
        self._progress.show()
        self._sig = _Signals(self)
        self._sig.progress.connect(self._status.setText)
        self._sig.done.connect(self._on_plan)
        self._pool.start(_RelinkJob([source], roots, self._sig))

    def _on_plan(self, plan) -> None:
        self._progress.hide()
        self._apply_btn.setEnabled(True)
        if isinstance(plan, Exception):
            QMessageBox.warning(self, "Relink failed", str(plan))
            return
        self._plan = [p for p in plan if p["action"] == "link"]
        orphans = [p for p in plan if p["action"] == "orphan"]
        if not self._plan:
            QMessageBox.information(
                self, "Nothing to relink",
                "No real copies were found — the tag folders already hold "
                "links.\n\n"
                + (f"{len(orphans)} file(s) had no original of that name "
                   "outside the mirrors; they were left alone."
                   if orphans else ""))
            return
        total = sum(p["bytes"] for p in self._plan)
        eff, why = favorites.probe_link_mode(taglibrary.tag_folders_root())
        if eff == "copy":
            QMessageBox.warning(
                self, "Links are not possible here",
                f"{why}\n\nRelinking would replace each copy with another "
                "copy, reclaiming nothing, so it has been stopped.\n\n"
                "Hard links need the tag folders on the SAME drive as your "
                "media; symlinks need Developer Mode on Windows.")
            return
        if QMessageBox.question(
                self, "Replace copies with links?",
                f"{len(self._plan)} copy(ies) would become {eff}s, freeing "
                f"about {foldersize.fmt_size(total)}.\n\n"
                f"{why}\n\n"
                "Matching is by FILENAME — content is not compared, as you "
                "asked. A re-encoded file of the same name would be replaced "
                "by a link to the original.\n\n"
                + (f"{len(orphans)} file(s) have no original elsewhere and "
                   "will be left as they are.\n\n" if orphans else "")
                + "Proceed?"
                ) != QMessageBox.StandardButton.Yes:
            return
        relinked, freed, errors = taglibrary.relink_copies(self._plan)
        self.changed.emit()
        msg = (f"{relinked} copy(ies) replaced with links; about "
               f"{foldersize.fmt_size(freed)} reclaimed.")
        if errors:
            msg += f"\n\n{len(errors)} could not be relinked:\n" + \
                   "\n".join(errors[:10])
        self._status.setText(msg)
        QMessageBox.information(self, "Relink complete", msg)
