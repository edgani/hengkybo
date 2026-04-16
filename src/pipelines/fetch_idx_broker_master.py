from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

URLS = [
    'https://www.idx.co.id/en/members-and-participants/exchange-members-profiles',
    'https://www.idx.co.id/id/anggota-bursa-dan-partisipan/profil-anggota-bursa',
]


def scrape_tables() -> pd.DataFrame:
    last_error = None
    for url in URLS:
        try:
            tables = pd.read_html(url)
        except Exception as exc:
            last_error = exc
            continue
        frames = []
        for t in tables:
            mapping = {}
            for c in t.columns:
                lc = str(c).strip().lower()
                if lc in {'code', 'kode', 'broker code'}:
                    mapping[c] = 'broker_code'
                elif lc in {'name', 'nama', 'exchange member', 'company'}:
                    mapping[c] = 'broker_name'
                elif 'type' in lc or 'jenis' in lc:
                    mapping[c] = 'member_type'
            if mapping:
                x = t.rename(columns=mapping).copy()
                if 'broker_code' in x.columns and 'broker_name' in x.columns:
                    frames.append(x)
        if frames:
            out = pd.concat(frames, ignore_index=True)
            out['broker_code'] = out['broker_code'].astype(str).str.upper().str.strip()
            out['broker_name'] = out['broker_name'].astype(str).str.strip()
            out = out[out['broker_code'].str.len().between(1, 4)]
            out = out.drop_duplicates(subset=['broker_code']).sort_values('broker_code').reset_index(drop=True)
            return out
    raise RuntimeError(f'Could not scrape IDX broker master. Last error: {last_error}')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='data/raw/broker_master.csv')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    df = scrape_tables()
    df.to_csv(output, index=False)
    print(f'wrote {len(df)} rows to {output}')


if __name__ == '__main__':
    main()
