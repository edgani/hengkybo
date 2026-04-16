from pathlib import Path
import shutil

legacy = Path(__file__).resolve().parents[1] / "app" / "pages"
if legacy.exists():
    shutil.rmtree(legacy)
    print(f"Removed legacy pages dir: {legacy}")
else:
    print(f"No legacy pages dir found: {legacy}")
