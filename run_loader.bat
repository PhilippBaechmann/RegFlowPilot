@echo off
rem ── activate the virtual‑env ─────────────────────────────
call "%~dp0\.venv\Scripts\activate.bat"

rem ── run the incremental loader ──────────────────────────
python "%~dp0\src\load_incremental.py"
