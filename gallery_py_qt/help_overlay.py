"""Shared keyboard-shortcuts help panel (matches the lightbox's overlay).

make_help_panel() returns a hidden, centred child panel; toggle_help_panel()
shows/hides it and keeps it centred over its parent.  Clicking the panel
dismisses it.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel

from . import config


class _HelpPanel(QWidget):
    def mousePressEvent(self, e):          # click anywhere on it to dismiss
        self.hide()
        e.accept()


def make_help_panel(parent: QWidget, rows: list[tuple[str, str]],
                    title: str = "Keyboard shortcuts") -> QWidget:
    panel = _HelpPanel(parent)
    panel.setStyleSheet(
        "background: rgba(0,0,0,235); border: 1px solid #333;"
        " border-radius: 10px;")
    lay = QVBoxLayout(panel)
    lay.setContentsMargins(22, 18, 22, 18)
    lay.setSpacing(4)
    tl = QLabel(title)
    tl.setStyleSheet(
        f"color: {config.FG_BRIGHT}; font-size: 15px; font-weight: bold;"
        " background: transparent;")
    lay.addWidget(tl)
    body_rows = "".join(
        f"<tr><td style='color:{config.ACCENT};padding:2px 16px 2px 0;"
        f"white-space:nowrap;'>{k}</td>"
        f"<td style='color:{config.FG_MID};'>{v}</td></tr>"
        for k, v in rows)
    body = QLabel(f"<table>{body_rows}</table>")
    body.setTextFormat(Qt.TextFormat.RichText)
    body.setStyleSheet("background: transparent;")
    lay.addWidget(body)
    panel.hide()
    return panel


def toggle_help_panel(panel: QWidget, host: QWidget) -> None:
    """Show the panel centred over `host`, or hide it if already visible."""
    if panel.isVisible():
        panel.hide()
        return
    panel.adjustSize()
    pw, ph = panel.width(), panel.height()
    panel.move(max(0, (host.width() - pw) // 2),
               max(0, (host.height() - ph) // 2))
    panel.show()
    panel.raise_()
