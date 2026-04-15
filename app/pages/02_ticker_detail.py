from pathlib import Path
import polars as pl
import streamlit as st

st.title("Ticker Detail")
path = Path("data/features/ticker_scores_daily.parquet")
if not path.exists():
    st.warning("Run pipeline first.")
else:
    df = pl.read_parquet(path)
    tickers = sorted(df["ticker"].unique().to_list())
    ticker = st.selectbox("Ticker", tickers)
    out = df.filter(pl.col("ticker") == ticker).sort("date")
    st.dataframe(out.tail(30).to_pandas(), use_container_width=True, hide_index=True)
