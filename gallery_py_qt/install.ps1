<#
.SYNOPSIS
    Set up gallery_py_qt on a Windows machine.

.DESCRIPTION
    Creates a virtual environment beside the repository, installs the four
    packages the gallery needs, and checks that they import.  Safe to re-run:
    an existing environment is reused and brought up to date.

.EXAMPLE
    .\gallery_py_qt\install.ps1
    .\gallery_py_qt\install.ps1 -Dev     # also install pytest, for the suite
#>
[CmdletBinding()]
param([switch]$Dev)

$ErrorActionPreference = "Stop"

# Locate the repository from this script's own path, so it does not matter
# which directory the user runs it from.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPy   = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$ReqName  = if ($Dev) { "requirements-dev.txt" } else { "requirements.txt" }
$Reqs     = Join-Path $PSScriptRoot $ReqName

# -- 1. find Python 3.10+ -----------------------------------------------------
# Prefer the Windows launcher: it finds Python even when it is not on PATH.
$Py = if (Get-Command py -ErrorAction SilentlyContinue) { "py" }
      elseif (Get-Command python -ErrorAction SilentlyContinue) { "python" }
      else { $null }

if (-not $Py) {
    Write-Host "No Python found. Install 3.10 or newer from" -ForegroundColor Red
    Write-Host "https://www.python.org/downloads/ (tick 'Add python.exe to PATH')."
    exit 1
}

$Ver = & $Py -c "import sys; print('%d.%d' % sys.version_info[:2])"
Write-Host "==> Using Python $Ver ($Py)" -ForegroundColor Cyan
$Major, $Minor = $Ver.Trim().Split(".")
if ([int]$Major -lt 3 -or [int]$Minor -lt 10) {
    Write-Host "Python 3.10 or newer is required; found $Ver." -ForegroundColor Red
    exit 1
}

# -- 2. virtual environment ---------------------------------------------------
if (Test-Path $VenvPy) {
    Write-Host "==> Reusing the environment at $RepoRoot\.venv" -ForegroundColor Cyan
} else {
    Write-Host "==> Creating a virtual environment at $RepoRoot\.venv" -ForegroundColor Cyan
    & $Py -m venv (Join-Path $RepoRoot ".venv")
    if ($LASTEXITCODE -ne 0) { throw "could not create the virtual environment" }
}

# -- 3. dependencies ----------------------------------------------------------
# NOTE: the requirements.txt in the REPOSITORY ROOT is a freeze of an unrelated
# environment and must not be used here. The gallery's own list is this one.
Write-Host "==> Installing $ReqName" -ForegroundColor Cyan
& $VenvPy -m pip install --upgrade pip --quiet
& $VenvPy -m pip install -r $Reqs
if ($LASTEXITCODE -ne 0) { throw "dependency installation failed" }

# -- 4. verify ----------------------------------------------------------------
# Importing is the real test: a wheel can install cleanly and still fail to
# load if a Visual C++ runtime or Qt plugin is missing.
Write-Host "==> Verifying" -ForegroundColor Cyan
& $VenvPy -c "import PySide6, PIL, cv2, piexif; print('    all four packages import cleanly')"
if ($LASTEXITCODE -ne 0) { throw "the installed packages do not import" }

Write-Host "`nDone. Start the gallery with:`n" -ForegroundColor Green
Write-Host "    $VenvPy `"$RepoRoot\gallery_py_qt.py`"`n"
if ($Dev) {
    Write-Host "Run the tests with:`n"
    Write-Host "    `$env:QT_QPA_PLATFORM='offscreen'; $VenvPy -m pytest tests -q`n"
}
