from pathlib import Path
import streamlit as st
import pandas as pd

st.set_page_config(page_title="IDX Flow Engine V3", layout="wide")
st.title("IDX Flow Engine V3")
st.caption("Adaptive broker-flow scaffold: EOD + intraday + regime-aware verdict")

watchlist_path = Path("data/features/latest_watchlist_v3.csv")
if watchlist_path.exists():
    df = pd.read_csv(watchlist_path)
    st.subheader("Latest V3 Watchlist")
    st.dataframe(df, use_container_width=True)
else:
    st.info("Run python -m src.pipelines.run_v3_pipeline first.")
