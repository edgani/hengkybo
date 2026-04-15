from pathlib import Path
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Microstructure", layout="wide")
st.title("Intraday Microstructure")

path = Path("data/features/latest_intraday_signals.csv")
if not path.exists():
    st.info("Run `python -m src.pipelines.run_intraday_pipeline` first.")
else:
    df = pd.read_csv(path)
    st.dataframe(df, use_container_width=True)
