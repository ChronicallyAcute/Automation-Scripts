"""Cross-platform reveal / open helpers backing the media source link."""
from __future__ import annotations
import sys

from gallery_py_qt.engine import shell


def test_empty_path_is_rejected():
    assert shell.reveal_path("") is False
    assert shell.open_path("") is False


def test_reveal_uses_a_native_command(tmp_path, monkeypatch):
    p = tmp_path / "a.jpg"
    p.write_bytes(b"\x00")
    calls = []
    monkeypatch.setattr(shell, "_run", lambda cmd: calls.append(cmd) or True)
    monkeypatch.setattr(shell.shutil, "which", lambda _n: "/usr/bin/stub")
    assert shell.reveal_path(str(p)) is True
    assert calls, "expected a reveal command to be launched"
    # Whichever platform branch ran, the file's own path must be in the command.
    assert any(str(p) in " ".join(c) for c in calls)


def test_reveal_falls_back_to_opening_the_folder(tmp_path, monkeypatch):
    p = tmp_path / "sub" / "a.jpg"
    p.parent.mkdir()
    p.write_bytes(b"\x00")
    monkeypatch.setattr(shell, "_run", lambda cmd: False)      # every tool fails
    monkeypatch.setattr(shell.shutil, "which", lambda _n: None)
    opened = []
    monkeypatch.setattr(shell, "open_path",
                        lambda x: opened.append(x) or True)
    assert shell.reveal_path(str(p)) is True
    assert opened == [str(p.parent)]               # containing folder, not file


def test_reveal_of_a_directory_targets_itself(tmp_path, monkeypatch):
    d = tmp_path / "album"
    d.mkdir()
    monkeypatch.setattr(shell, "_run", lambda cmd: False)
    monkeypatch.setattr(shell.shutil, "which", lambda _n: None)
    opened = []
    monkeypatch.setattr(shell, "open_path",
                        lambda x: opened.append(x) or True)
    shell.reveal_path(str(d))
    assert opened == [str(d)]


def test_run_never_raises_on_a_missing_binary():
    assert shell._run(["definitely-not-a-real-binary-xyz"]) is False
