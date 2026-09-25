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
                 signals: _Signals, recursive: bool = True):
        super().__init__()
        self._sources = list(sources)
        self._roots = list(roots)
        self._recursive = recursive
        self._sig = signals

    def run(self) -> None:
        try:
            collisions: "dict[str, list[str]]" = {}
            self._sig.progress.emit(
                f"Indexing originals under {len(self._sources)} folder(s)…")
            index = taglibrary.index_originals(
                self._sources, exclude=self._roots,
                recursive=self._recursive, collisions=collisions,
                progress=lambda n, where: self._sig.progress.emit(
                    f"Indexing… {n} name(s) so far  ·  {where}"))
            self._sig.progress.emit(
                f"Indexed {len(index)} name(s). Scanning the tag folders…")
            plan = taglibrary.plan_relink(self._roots, index)
            self._sig.done.emit({"plan": plan, "index": index,
                                 "collisions": collisions})
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
        self._relink_cb.toggled.connect(self._sync_source_enabled)
        root.addWidget(self._relink_cb)

        # Where the ORIGINALS live.  Several folders, each searched to full
        # depth, so originals spread across unrelated directories (or drives)
        # are all matched in one pass.
        src_label = QLabel("Search these folders for the original files:")
        src_label.setStyleSheet(f"color: {config.FG_MID};")
        root.addWidget(src_label)

        self._sources = QTreeWidget()
        self._sources.setHeaderHidden(True)
        self._sources.setRootIsDecorated(False)
        self._sources.setMaximumHeight(110)
        root.addWidget(self._sources)

        src_row = QHBoxLayout()
        add_src = QPushButton("Add folder…")
        add_src.clicked.connect(self._add_source)
        src_row.addWidget(add_src)
        rm_src = QPushButton("Remove")
        rm_src.clicked.connect(self._remove_source)
        src_row.addWidget(rm_src)
        self._recursive_cb = QCheckBox("Search subfolders")
        self._recursive_cb.setChecked(True)
        self._recursive_cb.setToolTip(
            "Walk each folder to its full depth. Off, only files sitting "
            "directly in the chosen folders are matched.")
        src_row.addWidget(self._recursive_cb)
        src_row.addStretch(1)
        root.addLayout(src_row)

        for folder in self._remembered_sources():
            self._add_source_path(folder)
        self._sync_source_enabled(True)

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

    # -- where the originals live -------------------------------------------------
    def _remembered_sources(self) -> "list[str]":
        """Last session's folders, else the folders currently open."""
        try:
            from .engine import prefs as _prefs
            saved = _prefs.load_prefs().get("relink_sources", [])
        except Exception:
            saved = []
        if saved:
            return [p for p in saved if isinstance(p, str)]
        return sorted({os.path.dirname(p) for p in self._media_folders})[:4]

    def _remember_sources(self) -> None:
        try:
            from .engine import prefs as _prefs
            p = _prefs.load_prefs()
            p["relink_sources"] = self.source_folders()
            _prefs.save_prefs(p)
        except Exception:
            pass

    def source_folders(self) -> "list[str]":
        return [self._sources.topLevelItem(i).text(0)
                for i in range(self._sources.topLevelItemCount())]

    def _add_source_path(self, folder: str) -> None:
        if not folder or folder in self.source_folders():
            return
        item = QTreeWidgetItem([folder])
        if not os.path.isdir(folder):
            item.setText(0, folder + "   (missing)")
            item.setDisabled(True)
        self._sources.addTopLevelItem(item)

    def _add_source(self) -> None:
        start = (self.source_folders() or [""])[-1] or (
            self._media_folders[0] if self._media_folders
            else os.path.expanduser("~"))
        if os.path.isfile(start):
            start = os.path.dirname(start)
        folder = QFileDialog.getExistingDirectory(
            self, "Add a folder to search for original files", start)
        if folder:
            self._add_source_path(folder)

    def _remove_source(self) -> None:
        for item in self._sources.selectedItems():
            self._sources.takeTopLevelItem(
                self._sources.indexOfTopLevelItem(item))

    def _sync_source_enabled(self, on: bool) -> None:
        for w in (self._sources, self._recursive_cb):
            w.setEnabled(bool(on))

    # -- relink ------------------------------------------------------------------
    def _start_relink(self, keep: "list[str]") -> None:
        sources = [f for f in self.source_folders() if os.path.isdir(f)]
        if not sources:
            self._add_source()
            sources = [f for f in self.source_folders() if os.path.isdir(f)]
        if not sources:
            QMessageBox.information(
                self, "Skipped",
                self._status.text() + "\n\nRelinking skipped — no folder was "
                "given to search for the originals.")
            return
        roots = taglibrary.mirror_roots(self._media_folders, keep)
        if not roots:
            QMessageBox.information(self, "Nothing to relink",
                                    "No tag or Favorites folders were found.")
            return
        self._remember_sources()
        self._apply_btn.setEnabled(False)
        self._progress.show()
        self._sig = _Signals(self)
        self._sig.progress.connect(self._status.setText)
        self._sig.done.connect(self._on_plan)
        self._pool.start(_RelinkJob(sources, roots, self._sig,
                                    self._recursive_cb.isChecked()))

    def _on_plan(self, result) -> None:
        self._progress.hide()
        self._apply_btn.setEnabled(True)
        if isinstance(result, Exception):
            QMessageBox.warning(self, "Relink failed", str(result))
            return
        plan = result["plan"]
        index = result["index"]
        collisions = result["collisions"]
        self._plan = [p for p in plan if p["action"] == "link"]
        orphans = [p for p in plan if p["action"] == "orphan"]
        searched = (f"Searched {len(self.source_folders())} folder(s)"
                    + (" and their subfolders" if self._recursive_cb.isChecked()
                       else " (top level only)")
                    + f", indexing {len(index)} filename(s).")
        if not self._plan:
            QMessageBox.information(
                self, "Nothing to relink",
                searched + "\n\nNo real copies were found — the tag folders "
                "already hold links.\n\n"
                + (f"{len(orphans)} file(s) had no original of that name in "
                   "those folders; they were left alone. Add the folder the "
                   "originals live in and run it again."
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
        # Filename-only matching across several directories means the same
        # name can exist in more than one of them; the first found wins, so
        # say which names that decided rather than resolving it silently.
        ambiguous = ""
        if collisions:
            picked = {p["original"] for p in self._plan}
            relevant = {n: v for n, v in collisions.items()
                        if any(x in picked for x in v)}
            if relevant:
                sample = "\n".join(
                    f"  • {n} — {len(v)} copies, using {v[0]}"
                    for n, v in list(relevant.items())[:6])
                ambiguous = (
                    f"\n{len(relevant)} filename(s) exist in more than one of "
                    "the searched folders. The first one found is used:\n"
                    f"{sample}\n"
                    + (f"  … and {len(relevant) - 6} more\n"
                       if len(relevant) > 6 else "") + "\n")
        if QMessageBox.question(
                self, "Replace copies with links?",
                searched + "\n\n"
                f"{len(self._plan)} copy(ies) would become {eff}s, freeing "
                f"about {foldersize.fmt_size(total)}.\n\n"
                f"{why}\n\n"
                "Matching is by FILENAME — content is not compared, as you "
                "asked. A re-encoded file of the same name would be replaced "
                "by a link to the original.\n"
                + ambiguous
                + (f"\n{len(orphans)} file(s) have no original in those "
                   "folders and will be left as they are.\n" if orphans else "")
                + "\nProceed?"
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
