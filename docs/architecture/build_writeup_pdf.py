#!/usr/bin/env python3
"""Build 'Manthan-Strategy-Writeup-v1.0.pdf' from writeup-body.html.

The IBM Plex faces are inlined from writeup-fonts.css (base64 woff2) so the
document renders identically offline, then headless Chrome prints it to PDF —
the same pipeline that produced the original v1.0.
"""
import pathlib, shutil, subprocess, sys

D    = pathlib.Path(__file__).resolve().parent
BODY = D / "writeup-body.html"
CSS  = D / "writeup-fonts.css"
OUT  = D / "Manthan-Strategy-Writeup-v1.0.html"
PDF  = D / "Manthan-Strategy-Writeup-v1.0.pdf"

html = BODY.read_text().replace("/*__FONTS__*/", CSS.read_text())
OUT.write_text(html)
print(f"HTML written: {len(html)} bytes -> {OUT.name}")

chrome = shutil.which("google-chrome") or shutil.which("google-chrome-stable")
if not chrome:
    sys.exit("google-chrome not found")

subprocess.run([
    chrome, "--headless", "--disable-gpu", "--no-sandbox",
    "--no-pdf-header-footer", "--run-all-compositor-stages-before-draw",
    "--virtual-time-budget=20000", f"--print-to-pdf={PDF}", OUT.as_uri(),
], check=True, capture_output=True)
print(f"PDF written: {PDF.stat().st_size} bytes -> {PDF.name}")
