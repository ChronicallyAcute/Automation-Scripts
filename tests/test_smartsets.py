"""Saved searches: persistence, overwrite, rename/delete, and round-tripping."""
from __future__ import annotations

from gallery_py_qt.engine import smartsets


def _state(**kw):
    base = {"images": True, "gifs": False, "videos": True,
            "favs_only": True, "query": "rating>=4",
            "tags": ["Az", "Bp"], "match_all": True}
    base.update(kw)
    return base


def test_save_and_get_roundtrip():
    assert smartsets.save("Faves", _state())
    got = smartsets.get("Faves")
    assert got["name"] == "Faves"
    assert got["videos"] is True and got["gifs"] is False
    assert got["query"] == "rating>=4"
    assert got["tags"] == ["Az", "Bp"] and got["match_all"] is True


def test_save_rejects_blank_name():
    assert not smartsets.save("   ", _state())
    assert smartsets.names() == []


def test_save_overwrites_in_place_keeping_order():
    smartsets.save("A", _state())
    smartsets.save("B", _state())
    smartsets.save("A", _state(query="changed"))
    assert smartsets.names() == ["A", "B"]          # order preserved
    assert smartsets.get("A")["query"] == "changed"


def test_normalise_fills_defaults_and_dedups_tags():
    out = smartsets.normalise({"tags": ["Az", "Az", "Bp"], "query": "  x  "})
    assert out["tags"] == ["Az", "Bp"]              # deduped + sorted
    assert out["query"] == "x"                      # trimmed
    assert out["images"] is True and out["favs_only"] is False


def test_delete_and_rename():
    smartsets.save("A", _state())
    smartsets.save("B", _state())
    assert smartsets.rename("A", "A2")
    assert smartsets.names() == ["A2", "B"]
    assert not smartsets.rename("A2", "B")          # target name taken
    assert smartsets.delete("A2")
    assert smartsets.names() == ["B"]
    assert not smartsets.delete("gone")


def test_sets_persist_across_reload():
    smartsets.save("Keep", _state(query="q"))
    smartsets._store = None                          # force a re-read from disk
    assert smartsets.get("Keep")["query"] == "q"
