from __future__ import annotations
import pandas as pd


def build_explanations(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    reasons=[]; invalid=[]
    for row in out.itertuples(index=False):
        items=[]
        if row.accumulation_quality_score >= 65: items.append('broker accumulation kuat')
        if row.breakout_integrity_score >= 65: items.append('struktur breakout matang')
        if getattr(row, 'microstructure_strength_score', 50) >= 60: items.append('tape/orderbook mendukung')
        if row.dry_score >= 60: items.append('barang relatif kering')
        if row.foreign_alignment_score >= 60: items.append('foreign flow mendukung')
        if row.distribution_risk_score >= 70: items.append('risiko distribusi tinggi')
        if getattr(row, 'transfer_suspicion', 50) >= 65: items.append('ada flow yang terlihat mencurigakan')
        reasons.append('; '.join(items[:4]) if items else 'sinyal campuran, edge belum kuat')
        inv = []
        if pd.notna(row.institutional_support): inv.append(f'close di bawah {row.institutional_support:.2f}')
        if pd.notna(row.institutional_resistance): inv.append(f'gagal tembus {row.institutional_resistance:.2f}')
        invalid.append(' atau '.join(inv[:2]) if inv else 'belum ada invalidation kuat')
    out['why_now'] = reasons
    out['invalidation'] = invalid
    return out
