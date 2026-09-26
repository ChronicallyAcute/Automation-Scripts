"""Duplicate finder results dialog + main-window wiring."""
from __future__ import annotations
import os
import time

from PIL import Image

from gallery_py_qt.dupes_dialog import DuplicatesDialog


def _img(path, color=(10, 20, 30)):
    Image.new("RGB", (32, 24), color).save(path)
    return str(path)


def _wait_scan(qapp, dlg, timeout=5.0):
    """Spin the event loop until the background scan populates the dialog."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        qapp.processEvents()
        if dlg._progress.isHidden():
            return
        time.sleep(0.01)


def test_scan_groups_duplicates(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    b = str(tmp_path / "b.jpg")
    import shutil
    shutil.copyfile(a, b)                       # exact byte copy
    c = _img(tmp_path / "c.jpg", color=(200, 100, 50))
    dlg = DuplicatesDialog([a, b, c])
    _wait_scan(qapp, dlg)
    assert len(dlg._groups) == 1
    assert set(dlg._groups[0]) == {a, b}
    assert len(dlg._checks) == 2                # one checkbox per dup file
    dlg.done(0)


def test_select_all_but_newest(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    b = str(tmp_path / "b.jpg")
    import shutil
    shutil.copyfile(a, b)
    # Make 'b' clearly newer so it is the one kept.
    os.utime(b, (time.time() + 100, time.time() + 100))
    dlg = DuplicatesDialog([a, b])
    _wait_scan(qapp, dlg)
    dlg._select_all_but_newest()
    sel = dlg._selected_paths()
    assert sel == [a]                           # older copy selected, newest kept
    dlg.done(0)


def test_no_duplicates_disables_actions(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg", color=(1, 2, 3))
    b = _img(tmp_path / "b.jpg", color=(9, 9, 9))
    dlg = DuplicatesDialog([a, b])
    _wait_scan(qapp, dlg)
    assert dlg._groups == []
    assert not dlg._trash_btn.isEnabled()
    assert "No duplicates" in dlg._header.text()
    dlg.done(0)


def test_trash_request_emits_and_regroups(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    b = str(tmp_path / "b.jpg")
    import shutil
    shutil.copyfile(a, b)
    dlg = DuplicatesDialog([a, b])
    _wait_scan(qapp, dlg)
    got = []
    dlg.trashRequested.connect(lambda ps: got.append(list(ps)))
    # Select just the older one and trash it.
    dlg._checks[0].setChecked(True)
    target = dlg._checks[0].property("path")
    dlg._trash_selected()
    assert got == [[target]]
    # The remaining single copy is no longer a group.
    assert dlg._groups == []
    dlg.done(0)


def test_reveal_signal(qapp, tmp_path):
    a = _img(tmp_path / "a.jpg")
    b = str(tmp_path / "b.jpg")
    import shutil
    shutil.copyfile(a, b)
    dlg = DuplicatesDialog([a, b])
    _wait_scan(qapp, dlg)
    revealed = []
    dlg.revealRequested.connect(revealed.append)
    # Find and click a "Reveal" button.
    from PySide6.QtWidgets import QPushButton
    btns = [w for w in dlg.findChildren(QPushButton) if w.text() == "Reveal"]
    assert btns
    btns[0].click()
    assert len(revealed) == 1 and revealed[0] in (a, b)
    dlg.done(0)


def test_main_window_reveal_selects_row(qapp, tmp_path):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    w.resize(1000, 700)
    w.show()
    qapp.processEvents()
    paths = [str(tmp_path / f"i{i}.jpg") for i in range(3)]
    for p in paths:
        Image.new("RGB", (32, 32)).save(p)
    w._model.set_paths(paths)
    w._model.set_dims({p: (100, 100) for p in paths})
    qapp.processEvents()
    w._reveal_path(paths[2])
    assert w._view.selected_rows() == [2]


def test_main_window_has_dupes_button(qapp):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    assert w._dupes_btn.text() == "Duplicates"
