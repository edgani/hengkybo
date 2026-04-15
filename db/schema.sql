CREATE TABLE IF NOT EXISTS prices_daily (
    date DATE,
    ticker TEXT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume_shares DOUBLE,
    volume_lots DOUBLE,
    turnover_value DOUBLE,
    vwap_day DOUBLE,
    market_cap DOUBLE,
    free_float_shares DOUBLE,
    sector TEXT,
    industry TEXT,
    board TEXT,
    is_suspended BOOLEAN,
    corporate_action_flag TEXT
);

CREATE TABLE IF NOT EXISTS broker_summary_daily (
    date DATE,
    ticker TEXT,
    broker_code TEXT,
    buy_lot DOUBLE,
    buy_value DOUBLE,
    sell_lot DOUBLE,
    sell_value DOUBLE,
    buy_avg DOUBLE,
    sell_avg DOUBLE,
    net_lot DOUBLE,
    net_value DOUBLE,
    gross_activity_lot DOUBLE,
    gross_activity_value DOUBLE
);

CREATE TABLE IF NOT EXISTS foreign_daily (
    date DATE,
    ticker TEXT,
    foreign_buy_lot DOUBLE,
    foreign_sell_lot DOUBLE,
    foreign_net_lot DOUBLE,
    foreign_buy_value DOUBLE,
    foreign_sell_value DOUBLE,
    foreign_net_value DOUBLE,
    foreign_ownership_pct DOUBLE
);

CREATE TABLE IF NOT EXISTS ticker_scores_daily (
    date DATE,
    ticker TEXT,
    phase TEXT,
    phase_confidence DOUBLE,
    accumulation_quality_score DOUBLE,
    breakout_integrity_score DOUBLE,
    distribution_risk_score DOUBLE,
    microstructure_strength_score DOUBLE,
    macro_alignment_score DOUBLE,
    dry_score DOUBLE,
    wet_score DOUBLE,
    institutional_support DOUBLE,
    institutional_resistance DOUBLE,
    false_breakout_risk DOUBLE,
    verdict TEXT,
    verdict_confidence DOUBLE,
    why_now TEXT,
    invalidation TEXT
);
