"""GalleryModel: media-class filter, chained multi-sort, batch removal."""
from __future__ import annotations
import os

import pytest

from gallery_py_qt.engine.favorites import Favorites
from gallery_py_qt.loader import ThumbnailLoader
from gallery_py_qt.model import GalleryModel


@pytest.fixture
def model(qapp):
    return GalleryModel(Favorites(), ThumbnailLoader())


def _names(m):
    return [os.path.basename(m.path_at(i)) for i in range(m.rowCount())]


def test_filter_three_media_classes(model, tmp_path):
    paths = [str(tmp_path / n) for n in ("a.jpg", "b.gif", "c.mp4")]
    for p in paths:
        open(p, "wb").write(b"x")
    model.set_paths(paths)
    model.set_dims({p: (100, 100) for p in paths})

    model.set_filter(images=True, videos=True, query="", gifs=False)
    assert not any(n.endswith(".gif") for n in _names(model))

    model.set_filter(images=False, videos=False, query="", gifs=True)
    assert _names(model) == ["b.gif"]

    model.set_filter(images=True, videos=False, query="", gifs=False)
    assert _names(model) == ["a.jpg"]


def test_sort_chain_like_dims_then_name(model, tmp_path):
    # two portraits (same aspect bucket) out of name order + one landscape
    specs = {"b.jpg": (600, 1000), "a.jpg": (600, 1000), "c.jpg": (1400, 700)}
    paths = [str(tmp_path / n) for n in specs]
    for p in paths:
        open(p, "wb").write(b"x")
    model.set_paths(paths)
    model.set_dims({str(tmp_path / n): d for n, d in specs.items()})
    model.set_sort_chain(["like_dims", "name"])
    # portraits first (grouped), tie-broken by name; landscape last
    assert _names(model) == ["a.jpg", "b.jpg", "c.jpg"]


def test_sort_chain_secondary_breaks_ties(model, tmp_path):
    specs = {"b.jpg": (600, 1000), "a.jpg": (600, 1000)}
    paths = [str(tmp_path / n) for n in specs]
    for p in paths:
        open(p, "wb").write(b"x")
    model.set_paths(paths)
    model.set_dims({str(tmp_path / n): d for n, d in specs.items()})
    model._favs._paths.add(str(tmp_path / "b.jpg"))
    model.set_sort_chain(["like_dims", "favorites"])
    assert _names(model)[0] == "b.jpg"          # favourite wins the tie


def test_needs_dimensions_reflects_chain(model):
    model.set_sort_chain(["name"])
    assert not model.needs_dimensions()
    model.set_sort_chain(["name", "like_dims"])
    assert model.needs_dimensions()


def test_remove_paths_batch(model, tmp_path):
    paths = [str(tmp_path / f"i{i}.jpg") for i in range(8)]
    for p in paths:
        open(p, "wb").write(b"x")
    model.set_paths(paths)
    model.set_dims({p: (100, 100) for p in paths})
    model.remove_paths(paths[:5])
    assert model.rowCount() == 3
    # reverse index stays consistent
    assert all(model._path_to_row[model.path_at(i)] == i
               for i in range(model.rowCount()))
