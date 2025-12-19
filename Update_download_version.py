#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Update version references inside DOWNLOAD.md based on TOOL_VERSION
from src/SSB-RetuningAutomations.py.

Safe replacement that preserves OS/arch suffixes like '_linux_x64.zip'.
"""

import os
import re
import sys

# ---------------------------------------------------------------------
# Import TOOL_VERSION dynamically from src/RetuningAutomations
# ---------------------------------------------------------------------
try:
    current_dir = os.path.dirname(__file__)
    src_path = os.path.abspath(os.path.join(current_dir, "src"))
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    from RetuningAutomations import TOOL_VERSION
except Exception as e:
    print(f"❌ ERROR: Unable to import TOOL_VERSION from SSB-RetuningAutomations.py\n{e}")
    sys.exit(1)

download_md = os.path.join(os.path.dirname(__file__), "DOWNLOAD.md")
if not os.path.isfile(download_md):
    print(f"❌ File not found: {download_md}")
    sys.exit(1)

with open(download_md, "r", encoding="utf-8") as f:
    content = f.read()

# Versión actual detectada en el fichero (primer match)
# Acepta pre-releases tipo '-beta', '-rc.1', etc. (NO incluye '_' posteriores)
VERSION_RE = r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z\.]+)?"
m = re.search(VERSION_RE, content)
current_version = m.group(0) if m else "not found"
print(f"📄 Current version in DOWNLOAD.md: {current_version}")

new_version = f"v{TOOL_VERSION}"
print(f"🔍 Detected TOOL_VERSION = {TOOL_VERSION}  →  Using {new_version}")

updated = content
total_replacements = 0

# 1) Reemplazar el segmento de ruta del release: /vX.Y.Z.../
#    (asegura que sólo cambia la parte entre barras)
pattern_release_path = re.compile(rf"/{VERSION_RE}/")
updated, n1 = pattern_release_path.subn(f"/{new_version}/", updated)
total_replacements += n1

# 2) Reemplazar en nombres de archivo con prefijo ' _vX.Y.Z...'
#    (no toca lo que venga después del versión: _linux_x64.zip, etc.)
pattern_filename = re.compile(rf"_({VERSION_RE})")
updated, n2 = pattern_filename.subn(f"_{new_version}", updated)
total_replacements += n2

# 3) (Opcional) Reemplazar versiones sueltas que no estén seguidas por '_' ni alfanumérico
#    Evita comerse sufijos como '_linux_x64'
pattern_standalone = re.compile(rf"(?<![_0-9A-Za-z])({VERSION_RE})(?![_0-9A-Za-z])")
updated, n3 = pattern_standalone.subn(new_version, updated)
total_replacements += n3

if updated != content:
    with open(download_md, "w", encoding="utf-8") as f:
        f.write(updated)
    print(f"✅ Updated DOWNLOAD.md to {new_version} ({total_replacements} replacements)")
else:
    print("ℹ️ No version changes were needed — file already up to date.")
