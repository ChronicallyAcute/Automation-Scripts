# Installing gallery_py_qt

## Quick start

**Windows**

```powershell
git clone https://github.com/ChronicallyAcute/Automation-Scripts.git
cd Automation-Scripts
.\gallery_py_qt\install.ps1
```

If PowerShell refuses to run the script:
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`

**Linux / macOS**

```bash
git clone https://github.com/ChronicallyAcute/Automation-Scripts.git
cd Automation-Scripts
./gallery_py_qt/install.sh
```

Both create `.venv/` beside the repository, install the dependencies, and
print the command that starts the gallery. Pass `-Dev` / `--dev` to also
install the test tools. Re-running is safe.

## Doing it by hand

Requires **Python 3.10+** (CI runs 3.12).

```bash
python -m venv .venv
.venv/bin/python -m pip install -r gallery_py_qt/requirements.txt   # Windows: .venv\Scripts\python.exe
.venv/bin/python gallery_py_qt.py
```

On Linux, Qt also needs system libraries that PySide6 does not bundle:

```bash
sudo apt-get install -y libegl1 libgl1 libxkbcommon0 libdbus-1-3 \
                        libpulse0 libfontconfig1 libxcb-cursor0
```

> **Do not install the `requirements.txt` in the repository root.** It is a
> `pip freeze` of an unrelated environment (PyQt5, biopython, pandas, scipy)
> and has nothing to do with the gallery. The list you want is
> `gallery_py_qt/requirements.txt` — four packages.

## Running the tests

```bash
./gallery_py_qt/install.sh --dev
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest tests -q
```

## Moving your library to another computer

None of your data lives in the repository — it is all in your home directory,
so a fresh clone starts empty:

| File | Holds |
| --- | --- |
| `.gallery_py_qt_tags.json` | tags per file |
| `.gallery_py_qt_tagset.json` | your tag names |
| `.gallery_py_qt_tagcolors.json` | tag colours |
| `.gallery_py_qt_ratings.json` | ratings |
| `.gallery_py_qt_smartsets.json` | saved searches |
| `.gallery_py_qt_prefs.json` | settings |
| `.gallery_favorites.json`, `.gallery_trash/` | favourites and trash |

**If the media sits at the same path on both machines**, copying those files
across is enough.

**If the path differs** (`D:\Media` becoming `E:\Media`), copying will not
work — the store is keyed by path. Use **`⋯` → Export tags & ratings…** on the
old machine and **Import tags & ratings…** on the new one instead: that
manifest is keyed relative to the folder you export, so it survives the move.

The remaining `.gallery_py_qt_*` files are caches (`_dims`, `_health`,
`_cache/`, `_recent`) and rebuild themselves. There is no need to copy them.

## Known rough edge

`config.py` hardcodes the favourites/tag-folder location to `G:\X` on Windows:

```python
if sys.platform.startswith("win"):
    FAVORITES_DIR = "G:\\X"
```

On a machine with no `G:` drive, favouriting and tag-folder mirroring will
fail. The comment above that line claims a `favorites_dir` preference key
overrides it; nothing currently reads that key, and Settings has no field for
it. Edit `config.py` directly for now.
