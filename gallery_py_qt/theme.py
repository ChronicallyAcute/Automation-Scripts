"""Application-wide Qt stylesheet built from the config palette."""
from __future__ import annotations
from . import config as c


def stylesheet() -> str:
    return f"""
    QMainWindow, QWidget {{
        background: {c.BG};
        color: {c.FG_BRIGHT};
        font-family: "{c.FONT_FAMILY}";
        font-size: 12px;
    }}
    QFrame#Toolbar {{
        background: {c.BAR_BG};
        border-bottom: 1px solid #1c1c1c;
    }}
    QToolButton, QPushButton {{
        background: transparent;
        color: {c.FG_MID};
        border: none;
        padding: 5px 9px;
        border-radius: 5px;
    }}
    QToolButton:hover, QPushButton:hover {{
        background: #1e1e1e;
        color: {c.FG_BRIGHT};
    }}
    QToolButton:checked, QPushButton:checked {{
        background: {c.ACCENT_BG};
        color: {c.ACCENT};
    }}
    QLineEdit {{
        background: #161616;
        color: {c.FG_BRIGHT};
        border: 1px solid #262626;
        border-radius: 5px;
        padding: 4px 8px;
        selection-background-color: {c.ACCENT};
    }}
    QComboBox {{
        background: #161616;
        color: {c.FG_MID};
        border: 1px solid #262626;
        border-radius: 5px;
        padding: 3px 8px;
    }}
    QComboBox QAbstractItemView {{
        background: {c.PANEL_BG};
        color: {c.FG_BRIGHT};
        selection-background-color: {c.ACCENT_BG};
    }}
    QListView {{
        background: {c.BG};
        border: none;
        outline: none;
    }}
    QListView::item:selected {{
        background: transparent;
        border: 1px solid {c.ACCENT};
    }}
    QScrollBar:vertical {{
        background: {c.BG}; width: 10px; margin: 0;
    }}
    QScrollBar::handle:vertical {{
        background: #2a2a2a; border-radius: 5px; min-height: 30px;
    }}
    QScrollBar::handle:vertical:hover {{ background: #3a3a3a; }}
    QScrollBar::add-line, QScrollBar::sub-line {{ height: 0; }}
    QLabel#StatusBar {{ color: {c.FG_DIM}; padding: 3px 8px; }}
    QToolButton#Overlay {{
        background: rgba(0, 0, 0, 110);
        color: {c.OVERLAY_FG};
        border-radius: 4px;
        font-size: 15px;
        padding: 2px 5px;
    }}
    QToolButton#Overlay:hover {{ background: rgba(0, 0, 0, 180); }}
    """
