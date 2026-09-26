"""Filtering reuses the cached sort order without changing what's displayed."""
from __future__ import annotations

from PIL import Image

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


def _model(tmp_path, names):
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = []
    for n in names:
        p = str(tmp_path / n)
        Image.new("RGB", (16, 16)).save(p)
        ps.append(p)
    m.set_paths(ps)
    return m, ps


def test_search_preserves_sort_order(tmp_path):
    m, _ = _model(tmp_path, ["c.jpg", "a.jpg", "b.jpg"])
    m.set_sort("name")
    assert [m.path_at(i).split("/")[-1] for i in range(m.rowCount())] == \
        ["a.jpg", "b.jpg", "c.jpg"]
    m.set_filter(True, True, "")                 # no query
    order = [m.path_at(i) for i in range(m.rowCount())]
    m.set_filter(True, True, "b")                # narrowed
    assert [p.split("/")[-1] for p in
            (m.path_at(i) for i in range(m.rowCount()))] == ["b.jpg"]
    m.set_filter(True, True, "")                 # widened back
    assert [m.path_at(i) for i in range(m.rowCount())] == order


def test_filtering_reuses_cache_but_sorting_refreshes_it(tmp_path):
    m, _ = _model(tmp_path, ["c.jpg", "a.jpg", "b.jpg"])
    m.set_sort("name")
    cached = m._sorted_cache
    assert cached is not None
    m.set_filter(True, True, "a")
    assert m._sorted_cache is cached             # filter kept the same order
    m.set_sort("name")                           # a sort change drops it
    assert m._sorted_cache is not cached


def test_new_paths_invalidate_the_cache(tmp_path):
    m, _ = _model(tmp_path, ["b.jpg"])
    m.set_sort("name")
    m.set_filter(True, True, "")
    assert m._sorted_cache is not None
    p = str(tmp_path / "a.jpg")
    Image.new("RGB", (16, 16)).save(p)
    m.add_paths_batch([p])
    assert m._sorted_cache is None               # stale order dropped
    m.set_filter(True, True, "")
    names = [m.path_at(i).split("/")[-1] for i in range(m.rowCount())]
    assert names == ["a.jpg", "b.jpg"]           # newcomer sorted into place


def test_removal_invalidates_the_cache(tmp_path):
    m, ps = _model(tmp_path, ["a.jpg", "b.jpg"])
    m.set_sort("name")
    m.set_filter(True, True, "")
    m.remove_path(ps[0])
    assert m._sorted_cache is None
    m.set_filter(True, True, "")
    assert m.rowCount() == 1


def test_descending_still_applies_through_cache(tmp_path):
    m, _ = _model(tmp_path, ["a.jpg", "b.jpg"])
    m.set_sort("name")
    m.set_descending(True)
    first = m.path_at(0).split("/")[-1]
    m.set_filter(True, True, "")                 # filter pass keeps direction
    assert m.path_at(0).split("/")[-1] == first == "b.jpg"


# -- toolbar wiring -----------------------------------------------------------
def test_tag_menu_button_opens_on_click(qapp):
    """The Tags ▾ button must use InstantPopup, else a click does nothing."""
    from PySide6.QtWidgets import QToolButton
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    assert w._tag_menu_btn.menu() is not None
    assert (w._tag_menu_btn.popupMode()
            == QToolButton.ToolButtonPopupMode.InstantPopup)
    # …and the menu is populated with the current tag set.
    from gallery_py_qt.engine import tags
    labels = {a.text() for a in w._tag_menu.actions()}
    assert set(tags.get_tags()) <= labels
    w.close()


def test_apply_filter_survives_a_broken_predicate(qapp, monkeypatch):
    """A failure mid-filter must be logged, not propagated out of the slot."""
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    monkeypatch.setattr(w._model, "set_filter",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    w._apply_filter()                    # must not raise
    assert "Filter failed" in w._status.text()
    w.close()
