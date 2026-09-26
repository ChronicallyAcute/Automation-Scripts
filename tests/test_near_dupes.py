"""Near-duplicate detection and the "not a duplicate" verdict."""
from __future__ import annotations
import os
import shutil

import pytest
from PIL import Image

from gallery_py_qt.engine import dupes, notdupes


def _img(path, colour=(9, 9, 9), size=(400, 300)):
    os.makedirs(os.path.dirname(str(path)), exist_ok=True)
    Image.new("RGB", size, colour).save(str(path))
    return str(path)


def _noisy(path, seed=0, size=(400, 300)):
    """An image that does not compress to nothing.

    A solid colour lands under the size floor below which the finder refuses
    to treat size as evidence at all, so signature tests need real content.
    """
    import random
    os.makedirs(os.path.dirname(str(path)), exist_ok=True)
    rnd = random.Random(seed)
    im = Image.new("RGB", size)
    im.putdata([(rnd.randrange(256), rnd.randrange(256), rnd.randrange(256))
                for _ in range(size[0] * size[1])])
    im.save(str(path))
    return str(path)


def _names(group):
    return sorted(os.path.basename(p) for p in group["paths"])


# -- name normalisation ----------------------------------------------------------

@pytest.mark.parametrize("name", [
    "YYYY_1.mp4", "YYYY(1).mp4", "YYYY (1).mp4", "YYYY [1].mp4",
    "YYYY - Copy.mp4", "YYYY - Copy (2).mp4", "YYYY copy.mp4",
    "YYYY copy 2.mp4", "YYYY_99.mp4",
])
def test_copy_suffixes_reduce_to_the_original(name):
    assert dupes.base_name(name) == dupes.base_name("YYYY.mp4")


@pytest.mark.parametrize("name", [
    "IMG_2024.jpg", "DSC_0001.jpg", "PXL_20240101.jpg",
])
def test_camera_sequence_numbers_are_not_stripped(name):
    """Stripping "_2024" would reduce a whole camera roll to one name and
    report every photo as a copy of every other."""
    assert dupes.base_name(name) != dupes.base_name("IMG.jpg")


def test_consecutive_camera_photos_stay_distinct():
    assert dupes.base_name("IMG_2024.jpg") != dupes.base_name("IMG_2025.jpg")


def test_a_numbered_sequence_is_left_alone():
    assert dupes.base_name("clip 2.mp4") != dupes.base_name("clip.mp4")


def test_a_year_in_brackets_survives():
    assert dupes.base_name("Photo (2019).jpg") != dupes.base_name("Photo.jpg")


def test_case_is_ignored():
    assert dupes.base_name("CLIP (1).MP4") == dupes.base_name("clip.mp4")


# -- signatures -------------------------------------------------------------------

def test_a_tiny_file_has_no_signature(tmp_path):
    p = str(tmp_path / "tiny.png")
    with open(p, "wb") as f:
        f.write(b"x" * 16)
    assert dupes.signature(p) is None, "size means nothing at this scale"


def test_an_image_signature_includes_its_dimensions(tmp_path):
    a = _noisy(tmp_path / "a.png", 1, (300, 200))
    sig = dupes.signature(a)
    assert sig is not None and sig[0] == "i" and sig[2:] == (300, 200)


def test_images_of_different_dimensions_do_not_share_a_signature(tmp_path):
    a = _noisy(tmp_path / "a.png", 1, (300, 200))
    b = _noisy(tmp_path / "b.png", 1, (200, 300))
    assert dupes.signature(a) != dupes.signature(b)


def test_a_solid_colour_image_is_below_the_floor(tmp_path):
    """Placeholders and flat graphics collide on size for no reason; the
    finder must not treat that as evidence."""
    assert dupes.signature(_img(tmp_path / "flat.png")) is None


def test_a_missing_file_has_no_signature(tmp_path):
    assert dupes.signature(str(tmp_path / "gone.png")) is None


# -- grouping -----------------------------------------------------------------------

def test_a_renamed_copy_is_found(tmp_path):
    a = _img(tmp_path / "YYYY.png")
    shutil.copy(a, str(tmp_path / "YYYY_1.png"))
    groups = dupes.find_near_duplicates(
        [a, str(tmp_path / "YYYY_1.png")])
    assert len(groups) == 1
    assert _names(groups[0]) == ["YYYY.png", "YYYY_1.png"]


def test_a_same_named_file_with_different_bytes_is_found(tmp_path):
    a = _img(tmp_path / "YYYY.png", (9, 9, 9))
    b = _img(tmp_path / "YYYY (1).png", (200, 3, 3))
    groups = dupes.find_near_duplicates([a, b])
    assert len(groups) == 1 and len(groups[0]["paths"]) == 2


def test_unrelated_images_are_not_grouped(tmp_path):
    """Size alone matches any two similar pictures; it needs corroboration."""
    a = _img(tmp_path / "one.png", (9, 90, 9), (400, 300))
    b = _img(tmp_path / "two.png", (7, 70, 7), (640, 480))
    assert dupes.find_near_duplicates([a, b]) == []


def test_a_camera_roll_is_not_one_big_group(tmp_path):
    roll = [_img(tmp_path / f"IMG_{2000 + i}.jpg", (i * 9, 40, 40))
            for i in range(6)]
    groups = dupes.find_near_duplicates(roll)
    assert all(len(g["paths"]) < len(roll) for g in groups)


def test_the_group_names_every_signal_that_bound_it(tmp_path):
    a = _img(tmp_path / "YYYY.png")
    shutil.copy(a, str(tmp_path / "YYYY_1.png"))
    g = dupes.find_near_duplicates([a, str(tmp_path / "YYYY_1.png")])[0]
    assert "identical bytes" in g["reason"]
    assert "near-identical name" in g["reason"]


def test_signals_union_rather_than_rank(tmp_path):
    """Ranking stranded a third copy: once two paired on bytes, one sharing
    only their name had nobody left to pair with."""
    a = _img(tmp_path / "YYYY.png")
    b = str(tmp_path / "YYYY_1.png")
    shutil.copy(a, b)
    c = _img(tmp_path / "YYYY (1).png", (200, 3, 3))
    groups = dupes.find_near_duplicates([a, b, c])
    assert len(groups) == 1 and len(groups[0]["paths"]) == 3


def test_exact_only_mode_ignores_names(tmp_path):
    a = _img(tmp_path / "YYYY.png", (9, 9, 9))
    b = _img(tmp_path / "YYYY (1).png", (200, 3, 3))
    assert dupes.find_near_duplicates([a, b], by_name=False,
                                      by_signature=False) == []


# -- "these are not duplicates" -------------------------------------------------------

def test_a_marked_pair_is_not_grouped_again(tmp_path):
    a = _img(tmp_path / "YYYY.png", (9, 9, 9))
    b = _img(tmp_path / "YYYY (1).png", (200, 3, 3))
    assert dupes.find_near_duplicates([a, b])
    notdupes.mark([a, b])
    assert dupes.find_near_duplicates([a, b]) == []


def test_marking_is_remembered_across_a_reload(tmp_path):
    a = _img(tmp_path / "a.png")
    b = _img(tmp_path / "b.png", (200, 3, 3))
    notdupes.mark([a, b])
    notdupes._store = None                     # simulate a fresh launch
    assert notdupes.is_marked(a, b)


def test_unmarking_lets_them_flag_again(tmp_path):
    a = _img(tmp_path / "YYYY.png", (9, 9, 9))
    b = _img(tmp_path / "YYYY (1).png", (200, 3, 3))
    notdupes.mark([a, b])
    assert notdupes.unmark([a, b]) == 1
    assert dupes.find_near_duplicates([a, b])


def test_marking_a_group_records_every_pair(tmp_path):
    paths = [_img(tmp_path / f"{i}.png", (i * 40, 3, 3)) for i in range(3)]
    assert notdupes.mark(paths) == 3           # 3 choose 2
    assert notdupes.count() == 3


def test_marking_twice_adds_nothing(tmp_path):
    a = _img(tmp_path / "a.png")
    b = _img(tmp_path / "b.png", (200, 3, 3))
    notdupes.mark([a, b])
    assert notdupes.mark([a, b]) == 0


def test_a_different_spelling_of_the_path_is_the_same_pair(tmp_path):
    a = _img(tmp_path / "a.png")
    b = _img(tmp_path / "b.png", (200, 3, 3))
    notdupes.mark([a, b])
    assert notdupes.is_marked(os.path.join(str(tmp_path), ".", "a.png"), b)


def test_clear_forgets_everything(tmp_path):
    a = _img(tmp_path / "a.png")
    b = _img(tmp_path / "b.png", (200, 3, 3))
    notdupes.mark([a, b])
    assert notdupes.clear() == 1
    assert not notdupes.is_marked(a, b)


def test_an_unresolvable_exclusion_is_reported_not_hidden(tmp_path):
    """If a==b and b==c but the user says a!=c, no partition holds all three.
    Whoever is left out must be named, not silently dropped."""
    a = _img(tmp_path / "YYYY.png")
    b = str(tmp_path / "YYYY_1.png")
    shutil.copy(a, b)
    c = str(tmp_path / "YYYY (1).png")
    shutil.copy(a, c)
    notdupes.mark([a, c])
    groups = dupes.find_near_duplicates([a, b, c])
    assert groups, "the remaining true pair must still be reported"
    assert any(g["held_back"] for g in groups)


def test_an_unreadable_store_is_not_overwritten(tmp_path):
    with open(notdupes._FILE, "w", encoding="utf-8") as f:
        f.write("{ not json")
    notdupes._store = None
    notdupes._load()
    assert notdupes._load_failed
    notdupes.mark([str(tmp_path / "a.png"), str(tmp_path / "b.png")])
    notdupes.flush()
    with open(notdupes._FILE, encoding="utf-8") as f:
        assert f.read() == "{ not json"
