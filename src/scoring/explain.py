from __future__ import annotations

import pandas as pd


def build_explanations(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    reasons = []
    invalidations = []
    event_notes = []

    for row in out.itertuples(index=False):
        items: list[str] = []
        if row.accumulation_quality_score >= 65:
            items.append('broker accumulation kuat')
        if row.breakout_integrity_score >= 65:
            items.append('struktur breakout matang')
        if getattr(row, 'microstructure_strength_score', 50) >= 60:
            items.append('tape/orderbook mendukung')
        if getattr(row, 'bullish_burst_score', 0) >= 65:
            items.append('gulungan atas valid dan follow-through sehat')
        if getattr(row, 'bear_trap_score', 0) >= 65:
            items.append('flush bawah terlihat diserap, rawan rebound')
        if row.dry_score >= 60:
            items.append('barang relatif kering')
        if getattr(row, 'bull_trap_score', 0) >= 65:
            items.append('gulungan atas rawan jebakan / distribusi')
        if getattr(row, 'bearish_burst_score', 0) >= 65:
            items.append('gulung bawah valid, tekanan jual dominan')
        if row.distribution_risk_score >= 70:
            items.append('risiko distribusi tinggi')
        if getattr(row, 'transfer_suspicion', 50) >= 65:
            items.append('ada flow yang terlihat mencurigakan')
        reasons.append('; '.join(items[:4]) if items else 'sinyal campuran, edge belum kuat')

        inv = []
        if pd.notna(getattr(row, 'institutional_support', None)):
            inv.append(f'close di bawah {row.institutional_support:.2f}')
        if pd.notna(getattr(row, 'institutional_resistance', None)):
            inv.append(f'gagal tembus {row.institutional_resistance:.2f}')
        if getattr(row, 'bull_trap_score', 0) >= 65:
            inv.append('burst atas langsung kehilangan follow-through')
        if getattr(row, 'bearish_burst_score', 0) >= 65:
            inv.append('break support dengan follow-through jual lanjut')
        invalidations.append(' atau '.join(inv[:3]) if inv else 'belum ada invalidation kuat')

        label = getattr(row, 'dominant_burst_label_context', 'NO_BURST')
        mapping = {
            'UP_CONTINUATION_BURST': 'gulung atas sehat, continuation bias',
            'UP_FALSE_BREAKOUT_RISK': 'gulung atas ada trap risk',
            'UP_CLIMAX_RISK': 'gulung atas berpotensi climax / jual ke strength',
            'DOWN_CONTINUATION_BREAK': 'gulung bawah sehat, breakdown bias',
            'DOWN_CAPITULATION_RISK': 'gulung bawah berpotensi capitulation',
            'DOWN_INITIATIVE_SWEEP': 'ada sapuan jual agresif',
            'UP_INITIATIVE_SWEEP': 'ada sapuan beli agresif',
            'NO_BURST': 'tidak ada burst dominan',
        }
        event_notes.append(mapping.get(label, str(label)))

    out['why_now'] = reasons
    out['invalidation'] = invalidations
    out['burst_note'] = event_notes
    return out
