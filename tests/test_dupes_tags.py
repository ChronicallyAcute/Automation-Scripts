"""Duplicate groups: locations, differing tag sets, keep-selection, tag merge."""
from __future__ import annotations
import os
import time

from PIL import Image

from gallery_py_qt.dupes_dialog import DuplicatesDialog
from gallery_py_qt.engine import dupes, tags


def _same_image(*paths):
    """Write byte-identical images to every path."""
    first = paths[0]
    os.makedirs(os.path.dirname(first), exist_ok=True)
    Image.new("RGB", (16, 16), (7, 7, 7)).save(first)
    data = open(first, "rb").read()
    for p in paths[1:]:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "wb").write(data)
    return list(paths)


def _dlg(qapp, paths):
    dlg = DuplicatesDialog([])
    for _ in range(200):
        qapp.processEvents()
        time.sleep(0.005)
        if dlg._job is None:
            break
    dlg._populate([list(paths)])
    return dlg


# -- engine -------------------------------------------------------------------
def test_locations_lists_distinct_folders(tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    locs = dupes.locations([a, b])
    assert locs == sorted([str(tmp_path / "x"), str(tmp_path / "y")])


def test_tags_differ_and_richest(tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    assert not dupes.tags_differ([a, b])          # both untagged
    tags.toggle_tag(a, "Az")
    assert dupes.tags_differ([a, b])
    tags.toggle_tag(b, "Bp")
    tags.toggle_tag(b, "HT")
    assert dupes.richest_tags([a, b]) == ["Bp", "HT"]   # b has more


def test_richest_is_deterministic_on_ties(tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(b, "Bp")
    assert dupes.richest_tags([a, b]) == dupes.richest_tags([b, a])


def test_stamp_richest_merges_onto_survivor(tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    tags.toggle_tag(a, "Az")                      # keeper's own tag
    tags.toggle_tag(b, "Bp")                      # doomed copy has more
    tags.toggle_tag(b, "HT")
    changed = dupes.stamp_richest_tags([a, b], [a])
    assert changed == [a]
    assert set(tags.tags_for(a)) == {"Az", "Bp", "HT"}   # union, nothing lost


def test_stamp_is_a_noop_when_survivor_already_richest(tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    tags.toggle_tag(a, "Az")
    tags.toggle_tag(a, "Bp")
    assert dupes.stamp_richest_tags([a, b], [a]) == []


# -- dialog -------------------------------------------------------------------
def test_choose_keeper_ticks_the_others(qapp, tmp_path):
    ps = _same_image(str(tmp_path / "x" / "a.jpg"),
                     str(tmp_path / "y" / "b.jpg"),
                     str(tmp_path / "z" / "c.jpg"))
    dlg = _dlg(qapp, ps)
    dlg._choose_keeper(ps[1], ps)
    checked = {cb.property("path") for cb in dlg._checks if cb.isChecked()}
    assert checked == {ps[0], ps[2]}              # keeper left unticked
    dlg.done(0)


def test_survivors_reports_group_and_keepers(qapp, tmp_path):
    ps = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    dlg = _dlg(qapp, ps)
    out = dlg._survivors({ps[1]})
    assert out == [(ps, [ps[0]])]
    dlg.done(0)


def test_trashing_merges_tags_onto_the_kept_copy(qapp, tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    tags.toggle_tag(b, "Bp")                      # only the doomed copy is tagged
    tags.toggle_tag(b, "HT")
    dlg = _dlg(qapp, [a, b])
    got = []
    dlg.trashRequested.connect(lambda ps: got.append(list(ps)))
    dlg._choose_keeper(a, [a, b])
    dlg._trash_selected()
    assert got == [[b]]                            # only the redundant copy goes
    assert set(tags.tags_for(a)) == {"Bp", "HT"}   # its tags moved to the keeper
    dlg.done(0)


def test_group_widget_shows_locations_and_tag_warning(qapp, tmp_path):
    a, b = _same_image(str(tmp_path / "x" / "a.jpg"), str(tmp_path / "y" / "b.jpg"))
    tags.toggle_tag(a, "Az")                      # makes the sets differ
    dlg = _dlg(qapp, [a, b])
    texts = [w.text() for w in dlg.findChildren(type(dlg._header))
             if hasattr(w, "text")]
    blob = "\n".join(texts)
    assert "Found in:" in blob
    assert str(tmp_path / "x") in blob and str(tmp_path / "y") in blob
    assert "different tags" in blob
    dlg.done(0)
