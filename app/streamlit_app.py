from pathlib import Path
import polars as pl
import streamlit as st

st.set_page_config(page_title="IDX Flow Engine v1", layout="wide")

st.title("IDX Flow Engine v1")
st.caption("EOD-first adaptive broker-flow watchlist scaffold")

watchlist_path = Path("data/features/latest_watchlist.csv")
if not watchlist_path.exists():
    st.warning("Run `python -m src.pipelines.run_eod_pipeline` first.")
else:
    df = pl.read_csv(watchlist_path)
    verdicts = sorted(df["verdict"].unique().to_list()) if "verdict" in df.columns else []
    selected = st.multiselect("Verdict filter", verdicts, default=verdicts)
    if selected:
        df = df.filter(pl.col("verdict").is_in(selected))
    st.dataframe(df.to_pandas(), use_container_width=True, hide_index=True)


st.caption("v2 scaffold includes EOD + intraday microstructure starter modules.")
