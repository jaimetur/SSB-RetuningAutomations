#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
compile.py
----------
Build a standalone executable for the main module `RetuningAutomations.py` using PyInstaller.

Features:
- Reads VERSION (without leading 'v') from the first lines of RetuningAutomations.py.
- Generates an output binary named: RetuningAutomations_v{VERSION}.exe (Windows) or .run (Linux/macOS).
- Works on each OS natively (PyInstaller cannot cross-compile OSes).
- Optional Windows-only arch selection via env: TARGET_ARCH={32|64|ARM64}.
- Cleans previous build artifacts (build/, dist/, *.spec) before building.
- Emits GitHub Actions outputs (BINARY_NAME, BINARY_PATH) if $GITHUB_OUTPUT is present.
- Suitable for matrix builds in GitHub Actions across OS/architectures.

Usage examples:
    python compile.py
    python compile.py --main RetuningAutomations.py --onefile
    TARGET_ARCH=ARM64 python compile.py       # Windows-only

Notes:
- Keep RetuningAutomations.py with a top-level variable: VERSION = "0.2.1"
- The filename will include a leading 'v' (e.g., RetuningAutomations_v0.2.1.exe)
"""

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path


def detect_platform_and_ext() -> tuple[str, str]:
    """Return (os_key, binary_extension)."""
    sysplat = sys.platform
    if sysplat.startswith("win"):
        return "windows", ".exe"
    if sysplat == "darwin":
        return "macos", ".run"
    if sysplat.startswith("linux"):
        return "linux", ".run"
    # Fallback
    return sysplat, ".run"


def read_version_from_main(main_path: Path) -> str:
    """Extract VERSION from the main module. VERSION must not include the 'v'."""
    text = main_path.read_text(encoding="utf-8", errors="ignore")
    # Look for a top-level assignment like: VERSION = "0.2.1"
    m = re.search(r'^\s*TOOL_VERSION\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    if not m:
        raise RuntimeError(f"VERSION variable not found in {main_path}")
    version = m.group(1).strip()
    if not version:
        raise RuntimeError(f"VERSION is empty in {main_path}")
    return version


def ensure_pyinstaller_available():
    """Check that PyInstaller is available in PATH; raise helpful error otherwise."""
    try:
        subprocess.run([sys.executable, "-m", "PyInstaller", "--version"],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except Exception as exc:
        raise RuntimeError(
            "PyInstaller is not available. Install it first, e.g.: "
            "pip install pyinstaller"
        ) from exc


def clean_previous_artifacts(name_stem: str):
    """Remove build/, dist/, and any *.spec related to this build name."""
    build_dir = Path("build")
    dist_dir = Path("dist")
    spec_file = Path(f"{name_stem}.spec")

    for p in [build_dir, dist_dir]:
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
    if spec_file.exists():
        spec_file.unlink(missing_ok=True)


def run_pyinstaller(entry_file: Path, name_stem: str, target_arch: str | None, onefile: bool = True):
    """Invoke PyInstaller to build the binary."""
    cmd = [sys.executable, "-m", "PyInstaller", str(entry_file)]
    # Common flags
    if onefile:
        cmd += ["--onefile"]
    cmd += ["--name", name_stem]

    # Windows-only target arch support
    os_key, _ = detect_platform_and_ext()
    if os_key == "windows" and target_arch:
        # Valid values: 32, 64, ARM64 (per PyInstaller docs)
        cmd += ["--target-arch", target_arch]

    # You may add icons or extra data here if needed in the future:
    # cmd += ["--icon", "path/to/icon.ico"]

    print(f"[INFO] Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def finalize_binary_name(name_stem: str, ext: str) -> Path:
    """
    After PyInstaller finishes:
    - Windows: dist/{name_stem}.exe already exists → return it.
    - Linux/macOS: dist/{name_stem} → rename to dist/{name_stem}.run and chmod +x.
    """
    dist_dir = Path("dist")
    # PyInstaller --onefile produces: dist/<name_stem>[.exe on Win]
    os_key, _ = detect_platform_and_ext()
    if os_key == "windows":
        produced = dist_dir / f"{name_stem}.exe"
        if not produced.exists():
            # Some PyInstaller versions could place it differently; fail clearly if missing
            raise FileNotFoundError(f"Expected {produced} not found")
        return produced

    # Linux / macOS
    produced = dist_dir / name_stem
    if not produced.exists():
        # Some variants may already have an extension; check .run just in case
        alt = dist_dir / f"{name_stem}.run"
        if alt.exists():
            produced = alt
        else:
            raise FileNotFoundError(f"Expected {produced} not found")
    # Ensure .run suffix
    target = produced if produced.suffix == ".run" else produced.with_suffix(".run")
    if target != produced:
        if target.exists():
            target.unlink()
        produced.rename(target)
    # Ensure executable bit
    try:
        mode = target.stat().st_mode
        target.chmod(mode | 0o111)
    except Exception:
        # Non-fatal on systems without chmod semantics
        pass
    return target


def write_github_outputs(binary_path: Path):
    """If running in GitHub Actions, write BINARY_NAME and BINARY_PATH outputs."""
    gha_out = os.environ.get("GITHUB_OUTPUT")
    if not gha_out:
        return
    with open(gha_out, "a", encoding="utf-8") as f:
        f.write(f"BINARY_NAME={binary_path.name}\n")
        # Normalize to POSIX-like path for consistency in logs
        f.write(f"BINARY_PATH={binary_path.as_posix()}\n")


def main():
    parser = argparse.ArgumentParser(description="Build executable with PyInstaller for RetuningAutomations.py")
    parser.add_argument("--main", default="./src/RetuningAutomations.py",
                        help="Entry-point main module filename (default: RetuningAutomations.py)")
    parser.add_argument("--onefile", action="store_true", default=True,
                        help="Force PyInstaller onefile build (default: True)")
    parser.add_argument("--no-onefile", dest="onefile", action="store_false",
                        help="Disable onefile build (rarely needed)")
    parser.add_argument("--target-arch", default=os.environ.get("TARGET_ARCH"),
                        help="Windows-only: 32 | 64 | ARM64 (can also be set via env TARGET_ARCH)")
    args = parser.parse_args()

    entry_file = Path(args.main).resolve()
    if not entry_file.exists():
        raise FileNotFoundError(f"Main entry file not found: {entry_file}")

    # Read version from the main module (without leading 'v')
    version = read_version_from_main(entry_file)
    os_key, ext = detect_platform_and_ext()

    # Compute final file name stem: RetuningAutomations_v{VERSION}
    # (Always add leading 'v' in the filename, as requested)
    module_stem = "RetuningAutomations"
    name_stem = f"{module_stem}_v{version}"

    print(f"[INFO] Detected OS: {os_key} ({platform.platform()})")
    print(f"[INFO] VERSION found in {entry_file.name}: {version}")
    print(f"[INFO] Target binary base name: {name_stem}{ext}")

    ensure_pyinstaller_available()
    clean_previous_artifacts(name_stem)

    # Build
    run_pyinstaller(entry_file=entry_file, name_stem=name_stem,
                    target_arch=args.target_arch, onefile=args.onefile)

    # Finalize filename/suffix and permissions
    final_path = finalize_binary_name(name_stem, ext)

    print(f"[SUCCESS] Binary created: {final_path.resolve()}")
    write_github_outputs(final_path)


if __name__ == "__main__":
    main()
