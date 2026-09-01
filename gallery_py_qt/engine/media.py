"""Decoding helpers producing thread-safe QImages.

QImage (unlike QPixmap) is safe to build off the GUI thread, so the threaded
thumbnail loader calls into here and the GUI thread only does the cheap
QImage -> QPixmap hand-off.

Fork improvements over gallery_qt.engine.media:
  \u2022 pil_to_qimage() uses Format_RGB888 for fully-opaque images (JPEG, BMP,
    non-alpha PNG) instead of always converting to RGBA.  This saves 25 % of
    decode memory and the conversion step for the most common image type.
  \u2022 load_thumbnail() / load_full_qimage() detect alpha presence before
    converting, skipping the unconditional im.convert("RGBA").
"""
from __future__ import annotations
import os
import sys
import threading

os.environ.setdefault("OPENCV_LOG_LEVEL", "ERROR")
os.environ.setdefault("OPENCV_FFMPEG_LOGLEVEL", "-8")

from PySide6.QtGui import QImage
from PIL import Image, ImageSequence

from .. import config

# OpenCV's VideoCapture is not reliably thread-safe; decoding video frames from
# several loader threads at once can segfault.  Serialise all cv2 access.
_CV2_LOCK = threading.Lock()

try:
    import cv2  # type: ignore
    HAS_CV2 = True
    for _p in ("setLogLevel", "utils.logging.setLogLevel"):
        try:
            _o = cv2
            for _seg in _p.split("."):
                _o = getattr(_o, _seg)
            _o(0)
            break
        except Exception:
            continue
except ImportError:
    cv2 = None
    HAS_CV2 = False
    print("[gallery-py-qt] opencv-python not installed \u2014 videos disabled. "
          "Install with: pip install opencv-python", file=sys.stderr)

# PIL modes that carry an alpha channel.
_ALPHA_MODES = {"RGBA", "LA", "PA"}

# Videos that failed to open (truncated / "moov atom not found" / unsupported
# codec) are remembered so we don't re-open them with cv2 on every hover,
# thumbnail request or dimension sort — each failed open costs the FFmpeg
# backend hundreds of ms to seconds and floods stderr.  Keyed by the file's
# mtime so a file that is later replaced (e.g. a download that finishes) is
# re-probed rather than staying blacklisted forever.
_BAD_VIDEOS: dict[str, float] = {}
_BAD_LOCK = threading.Lock()


def _file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def is_bad_video(path: str) -> bool:
    """True if `path` previously failed to open and hasn't changed since."""
    with _BAD_LOCK:
        stamp = _BAD_VIDEOS.get(path)
    return stamp is not None and stamp == _file_mtime(path)


def _mark_bad_video(path: str) -> None:
    with _BAD_LOCK:
        _BAD_VIDEOS[path] = _file_mtime(path)


def forget_bad_video(path: str) -> None:
    """Drop `path` from the unreadable-video cache so it is probed afresh."""
    with _BAD_LOCK:
        _BAD_VIDEOS.pop(path, None)


def is_video(path: str) -> bool:
    return os.path.splitext(path.lower())[1] in config.VIDEO_EXT


def is_gif(path: str) -> bool:
    return os.path.splitext(path.lower())[1] == ".gif"


def screen_max_px(fallback: int = 2560) -> int:
    """Largest useful decode size = the biggest screen's long side (device px),
    clamped.  A viewer never needs more pixels than the display can show, so
    this keeps full-image decodes/retained pixmaps far below source resolution."""
    try:
        from PySide6.QtGui import QGuiApplication
        best = 0
        for s in QGuiApplication.screens():
            sz = s.size()
            best = max(best,
                       int(max(sz.width(), sz.height()) * s.devicePixelRatio()))
        if best > 0:
            return max(1280, min(best, 5120))
    except Exception:
        pass
    return fallback


def _has_alpha(im: "Image.Image") -> bool:
    """Return True if the PIL image has (or may have) a transparency channel."""
    if im.mode in _ALPHA_MODES:
        return True
    if im.mode == "P" and "transparency" in im.info:
        return True
    return False


# -- PIL <-> QImage -----------------------------------------------------------

def pil_to_qimage(im: "Image.Image") -> QImage:
    """Convert a PIL image to an owned QImage.

    Uses Format_RGB888 for opaque images (saves 25 % memory vs. RGBA).
    Always returns a deep copy so the backing Python buffer can be freed.
    """
    if _has_alpha(im):
        if im.mode != "RGBA":
            im = im.convert("RGBA")
        data = im.tobytes("raw", "RGBA")
        qim = QImage(data, im.width, im.height, QImage.Format.Format_RGBA8888)
    else:
        if im.mode != "RGB":
            im = im.convert("RGB")
        data = im.tobytes("raw", "RGB")
        qim = QImage(data, im.width, im.height, 3 * im.width,
                     QImage.Format.Format_RGB888)
    return qim.copy()   # detach from the Python bytes buffer


# -- Permanent (on-disk) rotation ---------------------------------------------

def rotate_image_file(path: str, degrees: int) -> bool:
    """Rotate an image file *in place* by `degrees` clockwise (90/180/270).

    Uses PIL's transpose, which is an exact pixel remap for right-angle turns
    (no resampling blur).  Any existing EXIF orientation is baked in first so
    the result is unambiguous, and the orientation tag is dropped so viewers
    don't double-rotate.  ICC colour profiles and remaining EXIF are preserved
    where possible.  Returns True on success.
    """
    deg = degrees % 360
    if deg == 0:
        return True
    if deg % 45 != 0:
        return False
    try:
        from PIL import Image, ImageOps, ImageSequence
        # Right angles use transpose (exact pixel remap, no resampling).
        # PIL ROTATE_n is counter-clockwise, so clockwise 90 == ROTATE_270.
        right = {90: Image.Transpose.ROTATE_270,
                 180: Image.Transpose.ROTATE_180,
                 270: Image.Transpose.ROTATE_90}

        def _turn(img: "Image.Image") -> "Image.Image":
            if deg in right:
                return img.transpose(right[deg])
            # Diagonal (45/135/225/315): the canvas grows; corners fill with
            # transparency where the format supports it, black otherwise.
            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGBA" if _has_alpha(img) else "RGB")
            fill = (0, 0, 0, 0) if img.mode == "RGBA" else (0, 0, 0)
            return img.rotate(-deg, expand=True,
                              resample=Image.Resampling.BICUBIC,
                              fillcolor=fill)

        with Image.open(path) as im:
            fmt = im.format
            # Animated GIF / multi-page TIFF: rotate EVERY frame and re-save
            # with save_all, otherwise we'd silently flatten it to frame 0.
            if getattr(im, "is_animated", False) and getattr(im, "n_frames", 1) > 1:
                frames, durations = [], []
                for fr in ImageSequence.Iterator(im):
                    durations.append(fr.info.get("duration", 80))
                    frames.append(_turn(fr.convert("RGBA")))
                save_kw = {"save_all": True, "append_images": frames[1:],
                           "loop": im.info.get("loop", 0), "duration": durations}
                if fmt == "GIF":
                    save_kw["disposal"] = 2
                try:
                    frames[0].save(path, format=fmt, **save_kw)
                except Exception:
                    frames[0].save(path, save_all=True,
                                   append_images=frames[1:], duration=durations)
                return True
            im.load()
            im = ImageOps.exif_transpose(im)      # normalise existing rotation
            rotated = _turn(im)
            params = {}
            icc = im.info.get("icc_profile")
            if icc:
                params["icc_profile"] = icc
            exif = im.info.get("exif")
            if exif:
                params["exif"] = exif              # orientation tag already cleared
            if fmt == "JPEG":
                params["quality"] = 95
            try:
                rotated.save(path, format=fmt, **params)
            except Exception:
                # Fall back to letting PIL infer the format / drop odd params.
                rotated.save(path)
        return True
    except Exception as exc:
        print(f"[rotate] {os.path.basename(path)}: {exc}", file=sys.stderr)
        return False


# -- Size / duration peeks (cheap, header-only where possible) ----------------

def peek_size(path: str) -> tuple[int, int]:
    if is_video(path):
        if not HAS_CV2 or is_bad_video(path):
            return (0, 0)
        try:
            with _CV2_LOCK:
                cap = cv2.VideoCapture(path)
                opened = cap.isOpened()
                w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                cap.release()
            if not opened or (w <= 0 and h <= 0):
                _mark_bad_video(path)
                return (0, 0)
            return (w, h)
        except Exception:
            _mark_bad_video(path)
            return (0, 0)
    try:
        with Image.open(path) as im:
            return im.size
    except Exception:
        return (0, 0)


def peek_duration(path: str) -> float:
    if not (is_video(path) and HAS_CV2) or is_bad_video(path):
        return 0.0
    try:
        with _CV2_LOCK:
            cap = cv2.VideoCapture(path)
            opened = cap.isOpened()
            frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            fps = cap.get(cv2.CAP_PROP_FPS)
            cap.release()
        if not opened:
            _mark_bad_video(path)
            return 0.0
        return frames / fps if fps > 0 else 0.0
    except Exception:
        _mark_bad_video(path)
        return 0.0


# -- Video keyframe extraction -------------------------------------------------

def _video_frame(path: str, frac: float = 0.1) -> QImage | None:
    if not HAS_CV2 or is_bad_video(path):
        return None
    # Serialise cv2 access across loader threads (VideoCapture isn't thread-safe).
    with _CV2_LOCK:
        cap = cv2.VideoCapture(path)
        try:
            if not cap.isOpened():
                _mark_bad_video(path)
                return None
            fps = cap.get(cv2.CAP_PROP_FPS)
            total = max(1, int(cap.get(cv2.CAP_PROP_FRAME_COUNT)))
            if fps and fps > 0:
                cap.set(cv2.CAP_PROP_POS_MSEC,
                        max(0.0, (total / fps) * 1000.0 * frac))
            else:
                cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, int(total * frac)))
            ok, frame = cap.read()
            if not ok:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                ok, frame = cap.read()
            if not ok:
                _mark_bad_video(path)
                return None
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            h, w, _ = rgb.shape
            qim = QImage(rgb.data, w, h, 3 * w, QImage.Format.Format_RGB888)
            return qim.copy()
        except Exception:
            _mark_bad_video(path)
            return None
        finally:
            cap.release()


# -- Thumbnail + full-image loading -------------------------------------------

def frame_at_ms(path: str, position_ms: float) -> QImage | None:
    """Decode the video frame nearest `position_ms` at full resolution.

    Used by "save this frame": the on-screen video is scaled to the widget, so
    grabbing pixels from the view would save a downscaled, letterboxed image —
    seeking the source gives the true frame.
    """
    if not (is_video(path) and HAS_CV2) or is_bad_video(path):
        return None
    with _CV2_LOCK:
        cap = cv2.VideoCapture(path)
        try:
            if not cap.isOpened():
                _mark_bad_video(path)
                return None
            cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, float(position_ms)))
            ok, frame = cap.read()
            if not ok:                     # past the end / undecodable — retry at 0
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                ok, frame = cap.read()
            if not ok:
                return None
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            h, w, _ = rgb.shape
            return QImage(rgb.data, w, h, 3 * w,
                          QImage.Format.Format_RGB888).copy()
        except Exception:
            return None
        finally:
            cap.release()


def save_frame(path: str, position_ms: float, dest: str) -> bool:
    """Write the frame nearest `position_ms` to `dest` (format from extension)."""
    qim = frame_at_ms(path, position_ms)
    if qim is None or qim.isNull():
        return False
    try:
        return bool(qim.save(dest))
    except Exception:
        return False


def probe_integrity(path: str) -> "tuple[bool, str]":
    """Is this video fully readable?  Returns (ok, reason).

    A truncated download keeps a valid header claiming the full duration, so
    the file opens and plays — until the decoder runs past the real end of the
    data.  FFmpeg then reports "partial file" and scans BACKWARDS looking for a
    readable sample, which on a 100MB+ file is slow, blocking work repeated
    every time the file is touched.  Checking the tail up front lets the app
    quarantine such files instead of paying that cost repeatedly.
    """
    if not is_video(path):
        return True, ""
    if not HAS_CV2:
        return True, "not checked (opencv unavailable)"
    with _CV2_LOCK:
        cap = cv2.VideoCapture(path)
        try:
            if not cap.isOpened():
                return False, "cannot be opened"
            frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            fps = cap.get(cv2.CAP_PROP_FPS)
            if not fps or fps <= 0 or frames <= 0:
                return False, "no readable duration"
            # Seek close to the declared end and try to decode there.  A
            # truncated file declares frames it does not actually contain.
            target = max(0, int(frames) - 3)
            cap.set(cv2.CAP_PROP_POS_FRAMES, target)
            ok, _frame = cap.read()
            if not ok:
                return False, "truncated (data ends before the declared end)"
            return True, ""
        except Exception as exc:
            return False, f"probe failed: {exc}"
        finally:
            cap.release()


def load_thumbnail(path: str, max_px: int) -> QImage | None:
    """Load `path` scaled to <= max_px on its longest side. Returns QImage."""
    if is_video(path):
        qim = _video_frame(path)
        if qim is None:
            return None
        return _scaled_qimage(qim, max_px)
    try:
        with Image.open(path) as im:
            return _decode_scaled(im, max_px)
    except Exception as exc:
        print(f"[thumb] {os.path.basename(path)}: {exc}", file=sys.stderr)
        return None


def _decode_scaled(im: "Image.Image", max_px: int) -> QImage:
    """Decode `im` to a QImage no larger than max_px, minimising peak memory.

    Order matters: draft() lets the JPEG decoder produce a reduced-scale
    bitmap, but it is a NO-OP for PNG/TIFF/WebP — and converting those at
    source resolution duplicated the full-size bitmap before any downscale
    (the main residual OOM on big-PNG folders).  So for natively resizable
    modes we downscale FIRST and only convert the small result; odd modes
    (palette, 1-bit, CMYK…) still convert first for resize quality, but their
    native bitmaps are 1 byte/px so the full-size copy is far smaller anyway.
    """
    im.draft(None, (max_px, max_px))            # JPEG: reduced-scale decode
    alpha = _has_alpha(im)
    if im.mode not in ("RGB", "RGBA", "L", "LA"):
        im = im.convert("RGBA" if alpha else "RGB")
    w, h = im.size
    scale = min(max_px / max(w, 1), max_px / max(h, 1), 1.0)
    if scale < 1.0:
        im = im.resize((max(1, int(w * scale)),
                        max(1, int(h * scale))), Image.LANCZOS)
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGBA" if alpha else "RGB")   # small now — cheap
    return pil_to_qimage(im)


def load_full_qimage(path: str, max_px: int = 4096) -> QImage | None:
    """Full still for the lightbox / multiview, capped to max_px on the long
    side.  Callers pass a screen- or tile-derived size so the retained pixmap
    never balloons to the source resolution."""
    try:
        with Image.open(path) as im:
            return _decode_scaled(im, max_px)
    except Exception as exc:
        print(f"[full] {os.path.basename(path)}: {exc}", file=sys.stderr)
        return None


def load_gif_frames(path: str, max_px: int) -> tuple[list[QImage], list[int]]:
    """Return (frames, delays_ms) for an animated GIF, scaled to max_px."""
    frames: list[QImage] = []
    delays: list[int] = []
    try:
        with Image.open(path) as im:
            for fr in ImageSequence.Iterator(im):
                rgba = fr.convert("RGBA")
                w, h = rgba.size
                scale = min(max_px / max(w, 1), max_px / max(h, 1), 1.0)
                if scale < 1.0:
                    rgba = rgba.resize(
                        (max(1, int(w * scale)), max(1, int(h * scale))),
                        Image.LANCZOS)
                frames.append(pil_to_qimage(rgba))
                delays.append(max(int(fr.info.get("duration", 80)), 20))
    except Exception:
        pass
    return frames, delays


def _scaled_qimage(qim: QImage, max_px: int) -> QImage:
    w, h = qim.width(), qim.height()
    if max(w, h) <= max_px:
        return qim
    from PySide6.QtCore import Qt
    return qim.scaled(max_px, max_px, Qt.AspectRatioMode.KeepAspectRatio,
                      Qt.TransformationMode.SmoothTransformation)
