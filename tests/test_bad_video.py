"""Corrupt / unreadable videos are memoized so they aren't re-opened forever.

A truncated MP4 ('moov atom not found') costs the FFmpeg backend hundreds of
ms to seconds to fail on *every* open.  media.py remembers the failure keyed by
mtime, so hovers, thumbnails and dimension sorts short-circuit instead of
re-hanging — and a file that is later replaced is probed afresh.
"""
from __future__ import annotations
import os
import time

import pytest

from gallery_py_qt.engine import media


def _bogus_mp4(tmp_path, name="broken.mp4"):
    p = str(tmp_path / name)
    # Valid extension, but no moov atom / not a decodable stream.
    with open(p, "wb") as f:
        f.write(b"\x00\x00\x00\x18ftypmp42" + b"not a real video stream" * 8)
    return p


# -- cache primitives (no cv2 needed) -----------------------------------------
def test_cache_primitives_and_mtime_invalidation(tmp_path):
    p = str(tmp_path / "clip.mp4")
    open(p, "wb").write(b"x")
    assert not media.is_bad_video(p)
    media._mark_bad_video(p)
    assert media.is_bad_video(p)
    media.forget_bad_video(p)
    assert not media.is_bad_video(p)
    # Re-mark, then change the file — a replaced file must re-probe.
    media._mark_bad_video(p)
    assert media.is_bad_video(p)
    time.sleep(0.01)
    os.utime(p, (time.time() + 5, time.time() + 5))
    assert not media.is_bad_video(p)


def test_missing_file_is_not_falsely_bad(tmp_path):
    p = str(tmp_path / "gone.mp4")
    media._mark_bad_video(p)          # stamped with mtime 0.0 (missing)
    open(p, "wb").write(b"x")         # now it exists with a real mtime
    assert not media.is_bad_video(p)  # stale 0.0 stamp no longer matches


# -- real cv2 path ------------------------------------------------------------
@pytest.mark.skipif(not media.HAS_CV2, reason="opencv not installed")
def test_corrupt_video_marks_bad_and_short_circuits(tmp_path):
    p = _bogus_mp4(tmp_path)
    assert media.peek_size(p) == (0, 0)
    assert media.is_bad_video(p)          # remembered after first failed open
    # Subsequent probes short-circuit (no second cv2 open).
    assert media.peek_duration(p) == 0.0
    assert media._video_frame(p) is None


@pytest.mark.skipif(not media.HAS_CV2, reason="opencv not installed")
def test_short_circuit_is_fast(tmp_path):
    p = _bogus_mp4(tmp_path)
    media._mark_bad_video(p)
    t0 = time.perf_counter()
    for _ in range(50):
        media.peek_size(p)
        media._video_frame(p)
    assert time.perf_counter() - t0 < 0.5   # never touches cv2
