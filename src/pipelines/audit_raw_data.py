from __future__ import annotations

from pathlib import Path
import json
from src.utils.app_data import load_workspace, data_quality_report, broker_coverage_report

ROOT = Path(__file__).resolve().parents[2]
FEATURES = ROOT / 'data' / 'features'


def main() -> None:
    ws = load_workspace()
    qa = data_quality_report(ws.prices, ws.brokers, ws.broker_master)
    FEATURES.mkdir(parents=True, exist_ok=True)
    (FEATURES / 'raw_data_audit.json').write_text(json.dumps(qa, indent=2))
    cov = broker_coverage_report(ws.brokers, ws.broker_master)
    if not cov.empty:
        cov.to_csv(FEATURES / 'broker_coverage_report.csv', index=False)
    print(json.dumps(qa, indent=2))
    if not cov.empty:
        print('broker coverage report rows:', len(cov))


if __name__ == '__main__':
    main()
