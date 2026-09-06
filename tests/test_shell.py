"""Cross-platform reveal / open helpers backing the media source link."""
from __future__ import annotations
import os
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


def test_dbus_uri_is_percent_encoded(tmp_path, monkeypatch):
    """A path with spaces / '#' / '%' / non-ASCII must reach ShowItems as a
    valid percent-encoded URI, else the file manager selects nothing."""
    monkeypatch.setattr(shell.sys, "platform", "linux")
    d = tmp_path / "My Pictures"
    d.mkdir()
    p = d / "beach day #1 100%.jpg"
    p.write_bytes(b"\x00")
    calls = []
    monkeypatch.setattr(shell, "_run", lambda cmd: calls.append(cmd) or True)
    monkeypatch.setattr(shell.shutil, "which",
                        lambda n: "/usr/bin/dbus-send" if n == "dbus-send" else None)
    assert shell.reveal_path(str(p)) is True
    arg = next(a for a in calls[0] if a.startswith("array:string:"))
    assert " " not in arg                       # every space encoded
    assert "%20" in arg and "%23" in arg and "%25" in arg
    assert arg.startswith("array:string:file:///")


def test_windows_select_passes_a_single_quoted_path(tmp_path, monkeypatch):
    """explorer /select must be one command string so only the path is quoted;
    an argv list gets the whole token quoted and Explorer ignores it."""
    monkeypatch.setattr(shell.sys, "platform", "win32")
    p = tmp_path / "My Photos" / "pic 1.jpg"
    p.parent.mkdir()
    p.write_bytes(b"\x00")
    calls = []
    monkeypatch.setattr(shell, "_run", lambda cmd: calls.append(cmd) or True)
    assert shell.reveal_path(str(p)) is True
    cmd = calls[0]
    assert isinstance(cmd, str)                 # string, not a list
    assert cmd.startswith('explorer /select,"') and cmd.endswith('"')
    assert str(os.path.abspath(p)) in cmd


def test_run_accepts_a_command_string():
    # Popen(str) form used by the Windows reveal must not raise here either.
    assert shell._run("definitely-not-a-real-binary-xyz --nope") is False
