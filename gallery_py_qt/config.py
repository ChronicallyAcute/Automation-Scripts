"""Central configuration: paths, palette, fonts, icon glyphs, file types."""
from __future__ import annotations
import os
import sys

os.environ.setdefault(
    "QT_LOGGING_RULES",
    "qt.multimedia.ffmpeg=false;qt.multimedia.ffmpeg.*=false")
os.environ.setdefault("QT_FFMPEG_DEBUG", "0")

# -- Persistent file locations -----------------------------------------------
# Favourites / trash / the Downloads mirror are SHARED with gallery_qt so the
# two stay in sync.  Cache + prefs use separate names to avoid format clashes.
HOME          = os.path.expanduser("~")
FAVS_FILE     = os.path.join(HOME, ".gallery_favorites.json")       # shared
TRASH_DIR     = os.path.join(HOME, ".gallery_trash")                # shared
FAVORITES_DIR = os.path.join(HOME, "Downloads", "Gallery Favorites")# shared
CACHE_DIR     = os.path.join(HOME, ".gallery_py_qt_cache")          # own
PREFS_FILE    = os.path.join(HOME, ".gallery_py_qt_prefs.json")     # own
RECENT_FILE   = os.path.join(HOME, ".gallery_py_qt_recent.json")    # own
CRASH_LOG     = os.path.join(HOME, ".gallery_py_qt_crash.log")

# -- File types --------------------------------------------------------------
VIDEO_EXT = {".mp4", ".avi", ".mov", ".mkv", ".webm", ".m4v"}
IMAGE_EXT = {".gif", ".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff", ".tif"}
SUPPORTED = IMAGE_EXT | VIDEO_EXT

# -- Layout defaults ---------------------------------------------------------
DEFAULT_COLS = 3
MIN_COLS     = 1
MAX_COLS     = 8
GAP          = 6
MAX_THUMB_PX = 1600
SCROLL_DEF   = 60

# -- Palette / themes --------------------------------------------------------
# Themed colours are set by apply_theme() into module globals, so any code that
# reads e.g. config.BG at runtime follows the active theme.  CANVAS / CELL_BG
# drive the gallery grid background; the immersive lightbox / multiview stay
# black regardless.  Semantic colours (RED*/GREEN/AMBER/OVERLAY_FG) are fixed.
RED        = "#cc4444"
RED_BRIGHT = "#ff6666"
RED_DIM    = "#8a3030"
GREEN_FG   = "#66bb44"
AMBER_FG   = "#ccaa33"
OVERLAY_FG = "#ffffff"

THEMES: dict[str, dict] = {
    "dark": {
        "label": "Dark", "dark_ui": True,
        "BG": "#0a0a0a", "PANEL_BG": "#141414", "BAR_BG": "#101010",
        "CARD_BG": "#000000", "CANVAS": "#000000", "CELL_BG": "#000000",
        "FG_DIM": "#5a5a5a", "FG_MID": "#8a8a8a", "FG_BRIGHT": "#e8e8e8",
        "ACCENT": "#4a8fd4", "ACCENT_BG": "#13283d",
        "OVERLAY_BAR_BG": "rgba(15,15,15,225)",
    },
    "midnight": {
        "label": "Midnight", "dark_ui": True,
        "BG": "#000000", "PANEL_BG": "#0a0a0a", "BAR_BG": "#050505",
        "CARD_BG": "#000000", "CANVAS": "#000000", "CELL_BG": "#000000",
        "FG_DIM": "#4a4a4a", "FG_MID": "#7a7a7a", "FG_BRIGHT": "#f0f0f0",
        "ACCENT": "#5a9fe4", "ACCENT_BG": "#10243a",
        "OVERLAY_BAR_BG": "rgba(0,0,0,235)",
    },
    "light": {
        "label": "Light", "dark_ui": False,
        "BG": "#ececed", "PANEL_BG": "#ffffff", "BAR_BG": "#e2e2e5",
        "CARD_BG": "#d8d8dc", "CANVAS": "#e7e7ea", "CELL_BG": "#d6d6da",
        "FG_DIM": "#9a9a9e", "FG_MID": "#55555a", "FG_BRIGHT": "#1a1a1c",
        "ACCENT": "#2a6fc4", "ACCENT_BG": "#d3e4f7",
        "OVERLAY_BAR_BG": "rgba(245,245,247,235)",
    },
}

_current_theme = "dark"


def theme_name() -> str:
    return _current_theme


def theme_label(name: str) -> str:
    return THEMES.get(name, THEMES["dark"]).get("label", name)


def apply_theme(name: str) -> None:
    """Set the active theme's colours into module globals."""
    global _current_theme, BG, PANEL_BG, BAR_BG, CARD_BG, CANVAS, CELL_BG
    global FG_DIM, FG_MID, FG_BRIGHT, ACCENT, ACCENT_BG, OVERLAY_BAR_BG
    t = THEMES.get(name) or THEMES["dark"]
    _current_theme = name if name in THEMES else "dark"
    BG, PANEL_BG, BAR_BG = t["BG"], t["PANEL_BG"], t["BAR_BG"]
    CARD_BG, CANVAS, CELL_BG = t["CARD_BG"], t["CANVAS"], t["CELL_BG"]
    FG_DIM, FG_MID, FG_BRIGHT = t["FG_DIM"], t["FG_MID"], t["FG_BRIGHT"]
    ACCENT, ACCENT_BG = t["ACCENT"], t["ACCENT_BG"]
    OVERLAY_BAR_BG = t["OVERLAY_BAR_BG"]


# Establish the default palette at import time.
apply_theme("dark")

# -- Icon glyphs -------------------------------------------------------------
ICON_HEART_FULL  = "\u2665"
ICON_HEART_EMPTY = "\u2661"
ICON_ROTATE_CW   = "\u21bb"
ICON_ROTATE_CCW  = "\u21ba"
ICON_ENLARGE     = "\u26f6"
ICON_CLOSE       = "\u2715"
ICON_PREV        = "\u2039"
ICON_NEXT        = "\u203a"
ICON_GRID        = "\u229e"
ICON_COPY        = "\u2398"
ICON_PLAY        = "\u25b6"
ICON_PAUSE       = "\u23f8"
ICON_INFO        = "\u2139\ufe0e"
ICON_TRASH       = "\U0001f5d1\ufe0e"
ICON_PIN         = "\U0001f4cc\ufe0e"
ICON_PIN_OFF     = "\U0001f4cd\ufe0e"   # round pushpin  \u2014 outline / inactive state
ICON_PIN_ON      = "\U0001f4cc\ufe0e"   # filled pushpin \u2014 active / pinned state
ICON_FULLSCREEN  = "\u26f6"
ICON_FOLDER      = "\U0001f4c1\ufe0e"
ICON_MUTE        = "\U0001f507\ufe0e"   # \ud83d\udd07 text variant
ICON_UNMUTE      = "\U0001f50a\ufe0e"   # \ud83d\udd0a text variant
ICON_BACK        = "\u2190"              # \u2190

# Platform-aware UI font.  "Segoe UI" only exists on Windows; on Linux/macOS
# Qt silently fell back to an ugly default.  Pick a native family per platform,
# with a comma-separated fallback stack for the Qt stylesheet (FONT_STACK).
if sys.platform == "darwin":
    FONT_FAMILY = "SF Pro Text"
    FONT_STACK  = '"SF Pro Text", "Helvetica Neue", Arial, sans-serif'
elif sys.platform.startswith("win"):
    FONT_FAMILY = "Segoe UI"
    FONT_STACK  = '"Segoe UI", "Segoe UI Variable", Arial, sans-serif'
else:
    FONT_FAMILY = "Noto Sans"
    FONT_STACK  = ('"Inter", "Noto Sans", "DejaVu Sans", Cantarell,'
                   ' "Liberation Sans", sans-serif')


def ensure_dirs() -> None:
    for d in (CACHE_DIR, TRASH_DIR):
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
