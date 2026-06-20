"""Central configuration: paths, palette, fonts, icon glyphs, file types."""
from __future__ import annotations
import os

os.environ.setdefault(
    "QT_LOGGING_RULES",
    "qt.multimedia.ffmpeg=false;qt.multimedia.ffmpeg.*=false")
os.environ.setdefault("QT_FFMPEG_DEBUG", "0")

# ── Persistent file locations ───────────────────────────────────────────────
# Favourites / trash / the Downloads mirror are SHARED with gallery_qt so the
# two stay in sync.  Cache + prefs use separate names to avoid format clashes.
HOME          = os.path.expanduser("~")
FAVS_FILE     = os.path.join(HOME, ".gallery_favorites.json")       # shared
TRASH_DIR     = os.path.join(HOME, ".gallery_trash")                # shared
FAVORITES_DIR = os.path.join(HOME, "Downloads", "Gallery Favorites")# shared
CACHE_DIR     = os.path.join(HOME, ".gallery_py_qt_cache")          # own
PREFS_FILE    = os.path.join(HOME, ".gallery_py_qt_prefs.json")     # own
RECENT_FILE   = os.path.join(HOME, ".gallery_py_qt_recent.json")    # own
URL_CACHE_DIR = os.path.join(HOME, ".gallery_py_qt_url_cache")      # own
CRASH_LOG     = os.path.join(HOME, ".gallery_py_qt_crash.log")

# ── File types ──────────────────────────────────────────────────────────────
VIDEO_EXT = {".mp4", ".avi", ".mov", ".mkv", ".webm", ".m4v"}
IMAGE_EXT = {".gif", ".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff", ".tif"}
SUPPORTED = IMAGE_EXT | VIDEO_EXT

# ── Layout defaults ─────────────────────────────────────────────────────────
DEFAULT_COLS = 3
MIN_COLS     = 1
MAX_COLS     = 8
GAP          = 6
MAX_THUMB_PX = 1600
SCROLL_DEF   = 60

# ── Palette ─────────────────────────────────────────────────────────────────
BG        = "#0a0a0a"
PANEL_BG  = "#141414"
BAR_BG    = "#101010"
CARD_BG   = "#000000"
FG_DIM    = "#5a5a5a"
FG_MID    = "#8a8a8a"
FG_BRIGHT = "#e8e8e8"
ACCENT    = "#4a8fd4"
ACCENT_BG = "#13283d"
RED       = "#cc4444"
RED_BRIGHT = "#ff6666"
RED_DIM   = "#8a3030"
GREEN_FG  = "#66bb44"
AMBER_FG  = "#ccaa33"
OVERLAY_FG = "#ffffff"

# ── Icon glyphs ─────────────────────────────────────────────────────────────
ICON_HEART_FULL  = "♥"
ICON_HEART_EMPTY = "♡"
ICON_ROTATE_CW   = "↻"
ICON_ROTATE_CCW  = "↺"
ICON_ENLARGE     = "⛶"
ICON_CLOSE       = "✕"
ICON_PREV        = "‹"
ICON_NEXT        = "›"
ICON_GRID        = "⊞"
ICON_COPY        = "⎘"
ICON_PLAY        = "▶"
ICON_PAUSE       = "⏸"
ICON_INFO        = "ℹ︎"
ICON_TRASH       = "\U0001f5d1︎"
ICON_PIN         = "\U0001f4cc︎"
ICON_FULLSCREEN  = "⛶"
ICON_FOLDER      = "\U0001f4c1︎"

FONT_FAMILY = "Segoe UI"


def ensure_dirs() -> None:
    for d in (CACHE_DIR, TRASH_DIR, URL_CACHE_DIR):
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
