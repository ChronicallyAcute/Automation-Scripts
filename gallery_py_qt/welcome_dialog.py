"""First-run welcome / guide screen introducing the program's features.

Shown automatically the first time the app is launched (until dismissed with
the startup checkbox unticked) and re-openable anytime from the "Guide" button.
Deliberately a plain read-only tour — the exhaustive key list lives in the
?/F1 shortcuts overlay; this answers "what can this program do?".

The feature copy lives in the module-level SECTIONS table so it can be tested
and kept in step with the app without parsing widgets.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                               QScrollArea, QWidget, QCheckBox, QPushButton,
                               QFrame)

from . import config

# (section title, [(feature, one-line description), ...]).  Each feature string
# leads with the glyph the app uses for it where there is one, so the guide
# visually matches the buttons the user will look for.
SECTIONS: "list[tuple[str, list[tuple[str, str]]]]" = [
    ("Browse your media", [
        (f"{config.ICON_FOLDER}  Open",
         "Import whole folders or individual files. The picker can filter by "
         "media type — images, GIFs or videos — and remembers recent folders."),
        (f"{config.ICON_GRID}  Grid",
         "Media flows into a justified masonry grid. Change the column count, "
         "or Ctrl+scroll to zoom the thumbnails."),
        ("Sort",
         "Order by name, dimensions, favourites or rating, ascending or "
         "descending."),
    ]),
    ("Organise", [
        (f"{config.ICON_HEART_FULL}  Favourites",
         "Heart any item; favourites are mirrored into a Favorites folder kept "
         "beside the originals."),
        ("Tags",
         "Apply tags from a set you can add to and rename. Tags are embedded "
         "into image metadata where the format supports it."),
        ("★  Ratings",
         "Rate items 0–5 stars in the viewer, then sort or search by rating."),
        ("Tag albums",
         "Tag whole folders at once — every media file inside gets the tag. "
         "Found under the “⋯” menu on the toolbar."),
        ("Tag chips on a tile",
         "A multi-view tile shows only the tags it already carries; hover the "
         "tile to reveal the full row plus 〃 (repeat last), ＋# (new tag) and "
         "the per-tile − / + size buttons."),
    ]),
    ("Find", [
        ("Search",
         "Filter with a small query language — e.g. rating>=4, tag names, and "
         "combinations."),
        ("Duplicates",
         "Scan for byte-identical copies (under “⋯”). Each group shows where "
         "the copies live and flags differing tags; pick which to Keep and the "
         "survivor inherits the fullest tag set."),
        ("⇄  Sync tags by filename",
         "Under “⋯”. Copies of the same filename across a repository — "
         "re-downloaded or re-encoded, so the duplicate finder can't pair them "
         "by content — each receive the union of that name's tags. Purely "
         "additive, and it previews before writing."),
        ("Saved ▾",
         "Store the whole filter bar — media toggles, tags, query — under a "
         "name and recall it in one click."),
    ]),
    ("View", [
        (f"{config.ICON_ENLARGE}  Lightbox",
         "Full-screen viewer with zoom, pan, rotate and video playback (seek + "
         "volume). A Back button returns you to exactly where you were."),
        (f"{config.ICON_GRID}  Multi-view",
         "A 3×1 / 2×2 grid grouped by orientation. Zoom every tile ±10%, "
         "rotate in place, and run a paged or smooth side-scrolling slideshow. "
         "When media overflows a tile, edge arrows reposition it (video too)."),
        ("Hover-scrub",
         "Hover a video in the grid and drag across it to scrub its timeline "
         "without opening it."),
        ("A–B loop",
         "Right-click a video's seek bar to drop loop-in and loop-out points; "
         "playback then repeats only that span. Right-click again to clear."),
        ("↗  Source link",
         "Every tile shows a link that reveals the original file in your "
         "system file manager."),
    ]),
    ("Keep tidy", [
        (f"{config.ICON_TRASH}  Trash",
         "Deletions go to a recoverable trash with Undo (Ctrl+Z, and it keeps "
         "working after the toast fades); browse, restore or empty it under "
         "the “⋯” menu."),
        ("⋯  More",
         "Trash, Duplicates, Tag albums, Theme, Guide and Settings live in the "
         "“⋯” menu at the right of the toolbar, keeping the bar itself short."),
        ("⚙  Settings",
         "Copy-vs-link mode, colour themes, trash auto-purge, and a low-I/O "
         "mode for slow or external drives."),
        ("?  /  F1",
         "Open the full keyboard-shortcuts list on any screen."),
    ]),
]


class WelcomeDialog(QDialog):
    def __init__(self, show_at_startup: bool = True, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Welcome to Gallery")
        self.resize(640, 620)
        if parent is not None:
            self.setStyleSheet(parent.styleSheet())

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # -- header ------------------------------------------------------------
        header = QWidget()
        header.setStyleSheet(f"background: {config.ACCENT_BG};")
        hlay = QVBoxLayout(header)
        hlay.setContentsMargins(24, 20, 24, 18)
        hlay.setSpacing(4)
        title = QLabel("Welcome to Gallery")
        title.setStyleSheet(
            f"color: {config.FG_BRIGHT}; font-size: 22px; font-weight: bold;"
            " background: transparent;")
        hlay.addWidget(title)
        sub = QLabel("A fast, keyboard-friendly viewer for large image and "
                     "video collections. Here's what you can do:")
        sub.setWordWrap(True)
        sub.setStyleSheet(
            f"color: {config.FG_MID}; font-size: 13px; background: transparent;")
        hlay.addWidget(sub)
        root.addWidget(header)

        # -- scrolling feature list -------------------------------------------
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        body = QWidget()
        blay = QVBoxLayout(body)
        blay.setContentsMargins(24, 18, 24, 18)
        blay.setSpacing(18)
        for sec_title, features in SECTIONS:
            blay.addWidget(self._section(sec_title, features))
        blay.addStretch(1)
        scroll.setWidget(body)
        root.addWidget(scroll, 1)

        # -- footer ------------------------------------------------------------
        footer = QWidget()
        flay = QHBoxLayout(footer)
        flay.setContentsMargins(24, 12, 24, 16)
        self._startup_cb = QCheckBox("Show this guide at startup")
        self._startup_cb.setChecked(bool(show_at_startup))
        self._startup_cb.setToolTip(
            "Reopen it anytime from the “Guide” button in the toolbar")
        flay.addWidget(self._startup_cb)
        flay.addStretch(1)
        start_btn = QPushButton("Get started")
        start_btn.setDefault(True)
        start_btn.clicked.connect(self.accept)
        flay.addWidget(start_btn)
        root.addWidget(footer)

    def _section(self, title: str, features: "list[tuple[str, str]]") -> QWidget:
        box = QWidget()
        lay = QVBoxLayout(box)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(6)
        head = QLabel(title)
        head.setStyleSheet(
            f"color: {config.ACCENT}; font-size: 14px; font-weight: bold;")
        lay.addWidget(head)
        rule = QFrame()
        rule.setFrameShape(QFrame.Shape.HLine)
        rule.setStyleSheet(f"color: {config.FG_DIM};")
        lay.addWidget(rule)
        # One table per section so every description starts at the same column
        # (a fixed-width name column) instead of jittering per row.
        rows = "".join(
            f"<tr>"
            f"<td width='150' style='color:{config.FG_BRIGHT};font-weight:bold;"
            f"padding:3px 14px 3px 0;vertical-align:top;'>{name}</td>"
            f"<td style='color:{config.FG_MID};padding:3px 0;'>{desc}</td>"
            f"</tr>"
            for name, desc in features)
        table = QLabel(f"<table width='100%' cellspacing='0'>{rows}</table>")
        table.setTextFormat(Qt.TextFormat.RichText)
        table.setWordWrap(True)
        table.setStyleSheet("background: transparent;")
        lay.addWidget(table)
        return box

    def show_at_startup(self) -> bool:
        """Whether the user wants this shown on future launches."""
        return self._startup_cb.isChecked()
