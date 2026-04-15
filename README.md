# IDX Flow Engine V3

V3 menambahkan layer yang belum ada di V2:
- adaptive broker reclassification
- regime-aware weighting
- drift monitor
- final verdict engine yang tidak statis

## Yang baru di V3

### 1. Broker reclassification
Broker tidak lagi di-hardcode sebagai institusi / retail permanen. Sistem mengelompokkan broker berdasarkan perilaku rolling:
- activity rank
- breadth rank
- directionality
- stability

Output utama:
- `adaptive_broker_label`
- `institutional_like_score`
- `retail_like_score`
- `hybrid_score`
- `broker_profile_confidence`

### 2. Regime engine
Sistem membaca state pasar menjadi:
- `BULL`
- `CHOP`
- `BEAR`

### 3. Adaptive weights
Bobot long / sell berubah mengikuti regime. Di bull market, breakout lebih penting. Di bear market, distribution penalty lebih besar.

### 4. Drift monitor
Monitor ini mengecek apakah distribusi score recent mulai bergeser jauh dari reference window. Kalau iya, sinyal long akan diberi penalty.

## Jalankan

### EOD baseline
```bash
python -m src.pipelines.run_eod_pipeline
```

### Intraday layer
```bash
python -m src.pipelines.run_intraday_pipeline
```

### V3 adaptive verdict
```bash
python -m src.pipelines.run_v3_pipeline
```

### Dashboard
```bash
streamlit run app/streamlit_app.py
```

## Output V3
`data/features/`
- `ticker_scores_v3.parquet`
- `latest_watchlist_v3.csv`
- `broker_profiles_latest.csv`
- `regime_daily.csv`
- `drift_report.csv`

## Catatan penting
- V3 tetap inference engine, bukan alat yang tahu niat pelaku 100%
- crossing / transfer masih probabilistic
- institutional level tetap proxy, bukan custody truth
- paling cocok dijadikan research engine dulu, baru production setelah data feed lu matang
