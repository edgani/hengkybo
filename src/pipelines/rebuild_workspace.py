from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / 'data' / 'raw'


def run(module: str, extra: list[str] | None = None) -> None:
    cmd = [sys.executable, '-m', module] + (extra or [])
    print('RUN', ' '.join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    prices_real = RAW / 'prices_daily_real.csv'
    if prices_real.exists() and prices_real.stat().st_size > 0:
        run('src.pipelines.run_real_eod_smoke')
    else:
        run('src.pipelines.run_v4_pipeline')
    run('src.pipelines.audit_raw_data')


if __name__ == '__main__':
    main()
