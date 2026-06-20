"""Folder scanning — faithfully includes hidden files / OneDrive reparse points.

Fork improvements over gallery_qt.engine.scan:
  • scan_iter() — a generator that yields (batch, running_ScanResult) pairs so
    the UI can populate progressively while the scan is still running, rather
    than blocking until every file is enumerated.
  • Optional recursive scanning (off by default; matches original behaviour).
"""
from __future__ import annotations
import os
import sys
from dataclasses import dataclass, field
from typing import Generator

from .. import config
from . import media


@dataclass
class ScanResult:
    paths: list[str] = field(default_factory=list)
    total: int = 0
    hidden: int = 0
    skipped: int = 0
    videos_no_cv2: int = 0

    def summary(self) -> str:
        msg = f"{len(self.paths)} media file(s) from {self.total} entries"
        if self.hidden:
            msg += f"  ·  {self.hidden} hidden included"
        if self.skipped:
            msg += f"  ·  {self.skipped} skipped (access denied)"
        if self.videos_no_cv2:
            msg += f"  ·  {self.videos_no_cv2} video(s) skipped (no opencv)"
        return msg


def _iter_folder(folder: str, result: ScanResult,
                 recursive: bool = False) -> Generator[str, None, None]:
    """Internal generator — yields accepted file paths, mutates *result* stats."""
    try:
        it = os.scandir(folder)
    except OSError as exc:
        print(f"[scan] {folder}: {exc}", file=sys.stderr)
        return
    with it:
        for entry in it:
            result.total += 1
            try:
                if entry.is_dir(follow_symlinks=True):
                    if recursive:
                        yield from _iter_folder(entry.path, result,
                                                recursive=True)
                    continue
                if not entry.is_file():
                    continue
                try:
                    if sys.platform.startswith("win"):
                        st = entry.stat(follow_symlinks=False)
                        if getattr(st, "st_file_attributes", 0) & 0x02:
                            result.hidden += 1
                    elif entry.name.startswith("."):
                        result.hidden += 1
                except OSError:
                    pass
                ext = os.path.splitext(entry.name.lower())[1]
                if ext not in config.SUPPORTED:
                    continue
                if ext in config.VIDEO_EXT and not media.HAS_CV2:
                    result.videos_no_cv2 += 1
                    continue
                yield entry.path
            except OSError as exc:
                result.skipped += 1
                print(f"[scan-skip] {getattr(entry, 'path', '?')}: {exc}",
                      file=sys.stderr)


def scan_iter(folders: list[str], batch_size: int = 200,
              recursive: bool = False,
              ) -> Generator[tuple[list[str], "ScanResult"], None, None]:
    """Scan *folders* incrementally, yielding (batch, running_result) pairs.

    Each batch contains up to *batch_size* accepted paths.  The caller receives
    the first batch — and can begin populating the UI — before scanning of
    remaining folders is complete.

    The yielded *result* object is shared and mutated in place; do not store it
    across yields unless you copy it.  A final summary (with complete counts)
    is emitted by the ``done`` signal in the calling QRunnable.
    """
    result = ScanResult()
    batch: list[str] = []
    for folder in folders:
        for path in _iter_folder(folder, result, recursive=recursive):
            result.paths.append(path)
            batch.append(path)
            if len(batch) >= batch_size:
                yield batch[:], result
                batch.clear()
    if batch:
        yield batch, result


def scan_media(folder: str) -> ScanResult:
    """Non-streaming scan of a single folder (backward-compatible API)."""
    res = ScanResult()
    res.paths = list(_iter_folder(folder, res))
    res.paths.sort()
    return res
