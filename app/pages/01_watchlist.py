from pathlib import Path
import polars as pl
import streamlit as st

st.title("Watchlist")
path = Path("data/features/latest_watchlist.csv")
if not path.exists():
    st.warning("No watchlist file found.")
else:
    df = pl.read_csv(path)
    st.dataframe(df.to_pandas(), use_container_width=True, hide_index=True)
