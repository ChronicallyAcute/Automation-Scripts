"""A layout that wraps its items onto as many rows as they need.

Qt ships no wrapping box layout. QHBoxLayout squeezes its children toward
their minimum size and then simply overflows the widget's bounds, so anything
past the right edge is drawn outside the clip region and is both invisible and
unclickable. For a row of tag chips sitting on a narrow tile that is a silent
failure: the tags are "gone" with nothing to indicate why.

This lays items left to right and starts a new row when the next item would not
fit, so the content is always fully reachable — it just gets taller. Callers
size the container with :meth:`heightForWidth`.

Adapted from Qt's own flow-layout example; the implementation is the
conventional one (Qt's C++ example is BSD-licensed and widely reproduced).
"""
from __future__ import annotations

from PySide6.QtCore import Qt, QMargins, QPoint, QRect, QSize
from PySide6.QtWidgets import QLayout, QSizePolicy


class FlowLayout(QLayout):
    def __init__(self, parent=None, margin: int = 0, spacing: int = -1):
        super().__init__(parent)
        self._items: list = []
        if parent is not None:
            self.setContentsMargins(QMargins(margin, margin, margin, margin))
        self.setSpacing(spacing)

    # -- QLayout plumbing ------------------------------------------------------
    def addItem(self, item) -> None:          # noqa: N802 (Qt naming)
        self._items.append(item)

    def count(self) -> int:
        return len(self._items)

    def itemAt(self, index: int):             # noqa: N802
        if 0 <= index < len(self._items):
            return self._items[index]
        return None

    def takeAt(self, index: int):             # noqa: N802
        if 0 <= index < len(self._items):
            return self._items.pop(index)
        return None

    def expandingDirections(self):            # noqa: N802
        return Qt.Orientation(0)

    def hasHeightForWidth(self) -> bool:      # noqa: N802
        return True

    def heightForWidth(self, width: int) -> int:   # noqa: N802
        return self._layout(QRect(0, 0, width, 0), apply=False)

    def setGeometry(self, rect: QRect) -> None:    # noqa: N802
        super().setGeometry(rect)
        self._layout(rect, apply=True)

    def sizeHint(self) -> QSize:              # noqa: N802
        return self.minimumSize()

    def minimumSize(self) -> QSize:           # noqa: N802
        # The minimum is one item wide, not the whole row: a flow layout can
        # always make progress by wrapping, so demanding the full row width
        # would defeat the point and reintroduce the clipping.
        size = QSize()
        for item in self._items:
            size = size.expandedTo(item.minimumSize())
        m = self.contentsMargins()
        return size + QSize(m.left() + m.right(), m.top() + m.bottom())

    # -- the actual flow -------------------------------------------------------
    def _layout(self, rect: QRect, apply: bool) -> int:
        """Place items (or just measure); returns the total height needed."""
        m = self.contentsMargins()
        eff = rect.adjusted(m.left(), m.top(), -m.right(), -m.bottom())
        x, y = eff.x(), eff.y()
        row_h = 0
        space = self.spacing()
        if space < 0:
            space = 0
        for item in self._items:
            wid = item.widget()
            if wid is not None and wid.isHidden():
                continue          # hidden chips must not reserve a slot
            hint = item.sizeHint()
            nxt = x + hint.width()
            if nxt > eff.right() + 1 and row_h > 0:
                x = eff.x()                      # wrap
                y = y + row_h + space
                nxt = x + hint.width()
                row_h = 0
            if apply:
                item.setGeometry(QRect(QPoint(x, y), hint))
            x = nxt + space
            row_h = max(row_h, hint.height())
        return y + row_h - rect.y() + m.bottom()
