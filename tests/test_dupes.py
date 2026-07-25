"""Exact-duplicate detection engine."""
from __future__ import annotations
import os

from gallery_py_qt.engine import dupes


def _write(p, data: bytes):
    with open(p, "wb") as f:
        f.write(data)
    return str(p)


def test_finds_identical_files(tmp_path):
    a = _write(tmp_path / "a.jpg", b"hello world" * 10)
    b = _write(tmp_path / "b.jpg", b"hello world" * 10)
    c = _write(tmp_path / "c.jpg", b"something else entirely")
    groups = dupes.find_duplicates([a, b, c])
    assert groups == [[a, b]]                       # sorted, c excluded


def test_same_size_different_bytes_not_grouped(tmp_path):
    # Identical length, one byte apart -> not duplicates.
    a = _write(tmp_path / "a.bin", b"A" * 5000)
    b = _write(tmp_path / "b.bin", b"A" * 4999 + b"B")
    assert dupes.find_duplicates([a, b]) == []


def test_multiple_groups_ordered_by_size(tmp_path):
    big1 = _write(tmp_path / "big1", b"X" * 200000)
    big2 = _write(tmp_path / "big2", b"X" * 200000)
    small1 = _write(tmp_path / "s1", b"y" * 50)
    small2 = _write(tmp_path / "s2", b"y" * 50)
    groups = dupes.find_duplicates([small1, small2, big1, big2])
    assert groups == [[big1, big2], [small1, small2]]   # biggest first


def test_large_files_partial_head_tail(tmp_path):
    # Differ only in the middle, beyond the head chunk and before the tail
    # chunk -> the full hash must still separate them.
    base = bytearray(b"Z" * (dupes._CHUNK * 3))
    a = _write(tmp_path / "a", bytes(base))
    base[dupes._CHUNK + 100] = ord("Q")
    b = _write(tmp_path / "b", bytes(base))
    assert dupes.find_duplicates([a, b]) == []


def test_dedups_repeated_and_missing_paths(tmp_path):
    a = _write(tmp_path / "a", b"data" * 100)
    b = _write(tmp_path / "b", b"data" * 100)
    groups = dupes.find_duplicates([a, a, b, str(tmp_path / "ghost")])
    assert groups == [[a, b]]                       # 'a' not paired with itself


def test_progress_and_cancel(tmp_path):
    a = _write(tmp_path / "a", b"q" * 1000)
    b = _write(tmp_path / "b", b"q" * 1000)
    seen = []
    dupes.find_duplicates([a, b], progress=lambda d, t: seen.append((d, t)))
    assert seen and seen[-1][0] == seen[-1][1]      # ends at done == total

    # Cancelled before any full hash -> no groups.
    assert dupes.find_duplicates([a, b], cancelled=lambda: True) == []


def test_directories_and_empty_input(tmp_path):
    os.mkdir(tmp_path / "d1")
    assert dupes.find_duplicates([str(tmp_path / "d1")]) == []
    assert dupes.find_duplicates([]) == []
