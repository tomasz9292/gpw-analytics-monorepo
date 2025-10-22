# -*- mode: python ; coding: utf-8 -*-

import base64
from pathlib import Path

block_cipher = None

# ``__file__`` is not defined when the spec is executed via ``runpy`` on
# Windows PowerShell (see build.ps1).  Fall back to ``__spec__.origin`` or the
# current working directory so the build can succeed regardless of the entry
# point PyInstaller uses.
def _resolve_base_path() -> Path:
    if "__file__" in globals():
        return Path(__file__).resolve().parent

    spec = globals().get("__spec__")
    if spec and getattr(spec, "origin", None):
        return Path(spec.origin).resolve().parent

    return Path.cwd()


base_path = _resolve_base_path()
app_path = base_path / "app.py"
resources_path = base_path / "resources"
icon_base64_path = resources_path / "gpw_agent_icon.b64"
icon_binary_path = resources_path / "gpw-agent.tmp.ico"

icon_file_for_exe = None
if icon_base64_path.exists():
    payload = "".join(icon_base64_path.read_text(encoding="utf-8").split())
    if payload:
        icon_binary_path.write_bytes(base64.b64decode(payload))
        icon_file_for_exe = icon_binary_path

pathex = [str(base_path), str(base_path.parent)]

a = Analysis(
    [str(app_path)],
    pathex=pathex,
    binaries=[],
    datas=[(str(icon_base64_path), "resources")],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="GPWAnalyticsAgent",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(icon_file_for_exe) if icon_file_for_exe else None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="GPWAnalyticsAgent",
)
