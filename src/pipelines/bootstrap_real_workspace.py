from __future__ import annotations

from pathlib import Path
import shutil


def main() -> None:
    root = Path.cwd()
    raw = root / 'data' / 'raw'
    templates = root / 'data' / 'templates'
    raw.mkdir(parents=True, exist_ok=True)
    for src in templates.glob('*.csv'):
        dst = raw / src.name.replace('_template', '')
        if not dst.exists():
            shutil.copy2(src, dst)
            print(f'Created {dst}')
    print('Workspace bootstrapped. Fill data/raw/*.csv with real data as needed.')


if __name__ == '__main__':
    main()
