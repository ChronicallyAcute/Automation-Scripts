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


def is_video(path: str) -> bool:
    return os.path.splitext(path.lower())[1] in config.VIDEO_EXT


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
    if deg not in (90, 180, 270):
        return False
    try:
        from PIL import Image, ImageOps
        # PIL ROTATE_n is counter-clockwise, so clockwise 90 == ROTATE_270.
        op = {90: Image.Transpose.ROTATE_270,
              180: Image.Transpose.ROTATE_180,
              270: Image.Transpose.ROTATE_90}[deg]
        with Image.open(path) as im:
            fmt = im.format
            im.load()
            im = ImageOps.exif_transpose(im)      # normalise existing rotation
            rotated = im.transpose(op)
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
        if not HAS_CV2:
            return (0, 0)
        try:
            with _CV2_LOCK:
                cap = cv2.VideoCapture(path)
                w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                cap.release()
            return (w, h)
        except Exception:
            return (0, 0)
    try:
        with Image.open(path) as im:
            return im.size
    except Exception:
        return (0, 0)


def peek_duration(path: str) -> float:
    if not (is_video(path) and HAS_CV2):
        return 0.0
    try:
        with _CV2_LOCK:
            cap = cv2.VideoCapture(path)
            frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            fps = cap.get(cv2.CAP_PROP_FPS)
            cap.release()
        return frames / fps if fps > 0 else 0.0
    except Exception:
        return 0.0


# -- Video keyframe extraction -------------------------------------------------

def _video_frame(path: str, frac: float = 0.1) -> QImage | None:
    if not HAS_CV2:
        return None
    # Serialise cv2 access across loader threads (VideoCapture isn't thread-safe).
    with _CV2_LOCK:
        cap = cv2.VideoCapture(path)
        try:
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
                return None
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            h, w, _ = rgb.shape
            qim = QImage(rgb.data, w, h, 3 * w, QImage.Format.Format_RGB888)
            return qim.copy()
        except Exception:
            return None
        finally:
            cap.release()


# -- Thumbnail + full-image loading -------------------------------------------

def load_thumbnail(path: str, max_px: int) -> QImage | None:
    """Load `path` scaled to <= max_px on its longest side. Returns QImage."""
    if is_video(path):
        qim = _video_frame(path)
        if qim is None:
            return None
        return _scaled_qimage(qim, max_px)
    try:
        with Image.open(path) as im:
            # draft() lets the JPEG decoder load at 1/2..1/8 scale directly,
            # so a 24 MP photo never fully expands in RAM just to be thumbnailed
            # (the main defence against OOM on big-photo folders).  No-op for
            # non-JPEG formats.
            im.draft(None, (max_px, max_px))
            alpha = _has_alpha(im)
            im = im.convert("RGBA" if alpha else "RGB")
            w, h = im.size
            scale = min(max_px / max(w, 1), max_px / max(h, 1), 1.0)
            if scale < 1.0:
                im = im.resize((max(1, int(w * scale)),
                                max(1, int(h * scale))), Image.LANCZOS)
            return pil_to_qimage(im)
    except Exception as exc:
        print(f"[thumb] {os.path.basename(path)}: {exc}", file=sys.stderr)
        return None


def load_full_qimage(path: str, max_px: int = 6000) -> QImage | None:
    """Full-resolution still for the lightbox (clamped to a sane ceiling)."""
    try:
        with Image.open(path) as im:
            im.draft(None, (max_px, max_px))   # cheap down-scale for huge JPEGs
            alpha = _has_alpha(im)
            im = im.convert("RGBA" if alpha else "RGB")
            w, h = im.size
            scale = min(max_px / max(w, 1), max_px / max(h, 1), 1.0)
            if scale < 1.0:
                im = im.resize((max(1, int(w * scale)),
                                max(1, int(h * scale))), Image.LANCZOS)
            return pil_to_qimage(im)
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
