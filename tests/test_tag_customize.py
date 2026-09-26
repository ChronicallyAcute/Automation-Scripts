"""Custom / renamable tag set: persistence + store + folder migration."""
from __future__ import annotations
import json
import os

from gallery_py_qt import config
from gallery_py_qt.engine import tags


def _tagpath(t, name):
    return os.path.join(config.FAVORITES_DIR, t, name)


def test_default_tagset():
    assert tags.get_tags() == tags.DEFAULT_TAGS


def test_add_and_persist():
    assert tags.add_tag("Xy")
    assert "Xy" in tags.get_tags()
    assert not tags.add_tag("Xy")                 # no duplicates
    assert "Xy" in json.load(open(tags._TAGSET_FILE))


def test_set_tags_dedup_and_strip():
    tags.set_tags(["A", "A", " B ", "", "C"])
    assert tags.get_tags() == ("A", "B", "C")


def test_remove_tag_purges_store_and_folder(make_image, flush):
    img = make_image("a.jpg")
    tags.toggle_tag(img, "BT")
    tags.sync_tag_folders(img, True)
    flush()
    assert os.path.exists(_tagpath("BT", "a.jpg"))
    tags.remove_tag("BT")
    flush()
    assert "BT" not in tags.get_tags()
    assert "BT" not in tags.tags_for(img)         # stripped from store
    assert not os.path.isdir(os.path.join(config.FAVORITES_DIR, "BT"))


def test_rename_migrates_store(make_image):
    img = make_image("a.jpg")
    tags.toggle_tag(img, "WAM")
    assert tags.rename_tag("WAM", "Wham")
    assert "Wham" in tags.get_tags() and "WAM" not in tags.get_tags()
    assert tags.tags_for(img) == ["Wham"]


def test_rename_migrates_tag_folder(make_image, flush):
    img = make_image("a.jpg")
    tags.toggle_tag(img, "Az")
    tags.sync_tag_folders(img, True)
    flush()
    assert os.path.exists(_tagpath("Az", "a.jpg"))
    tags.rename_tag("Az", "Azz")
    flush()
    assert not os.path.isdir(os.path.join(config.FAVORITES_DIR, "Az"))
    assert os.path.exists(_tagpath("Azz", "a.jpg"))
    # manifest repointed, so a later unfavourite still removes the copy
    tags.sync_tag_folders(img, False)
    flush()
    assert not os.path.exists(_tagpath("Azz", "a.jpg"))


def test_rename_rejects_collision():
    assert not tags.rename_tag("BT", "HT")        # HT already exists
    assert not tags.rename_tag("nope", "x")       # source absent


def test_renamed_tag_drives_new_folder(make_image, flush):
    img = make_image("a.jpg")
    tags.rename_tag("Jz", "Jazz")
    tags.toggle_tag(img, "Jazz")
    tags.sync_tag_folders(img, True)
    flush()
    assert os.path.exists(_tagpath("Jazz", "a.jpg"))


# -- UI rebuild ---------------------------------------------------------------
def test_filter_menu_rebuilds_on_tagset_change(qapp):
    from gallery_py_qt.main_window import MainWindow
    w = MainWindow()
    w.show()
    qapp.processEvents()
    assert set(w._tag_filter_actions) == set(tags.DEFAULT_TAGS)
    tags.add_tag("New1")
    w._rebuild_tag_filter_menu()
    assert "New1" in w._tag_filter_actions
    tags.remove_tag("New1")
    w._rebuild_tag_filter_menu()
    assert "New1" not in w._tag_filter_actions


def test_multiview_tag_buttons_rebuild(qapp, tmp_path):
    from PySide6.QtWidgets import QStackedWidget
    from PIL import Image
    from gallery_py_qt.engine.favorites import Favorites
    from gallery_py_qt.loader import ThumbnailLoader
    from gallery_py_qt.model import GalleryModel
    from gallery_py_qt.multiview import MultiView
    m = GalleryModel(Favorites(), ThumbnailLoader())
    ps = [str(tmp_path / f"v{i}.jpg") for i in range(3)]
    for p in ps:
        Image.new("RGB", (600, 1000)).save(p)
    m.set_paths(ps)
    m.set_dims({p: (600, 1000) for p in ps})
    mv = MultiView(m, Favorites())
    st = QStackedWidget()
    st.addWidget(mv)
    st.setCurrentWidget(mv)
    st.resize(1400, 900)
    st.show()
    qapp.processEvents()
    mv.open(0)
    qapp.processEvents()
    assert sorted(mv._slots[0]._tag_btns) == sorted(tags.DEFAULT_TAGS)
    tags.set_tags(["Alpha", "Beta"])
    mv.rebuild_tag_buttons()
    assert sorted(mv._slots[0]._tag_btns) == ["Alpha", "Beta"]

