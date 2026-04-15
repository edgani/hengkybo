from pathlib import Path
import streamlit as st
import pandas as pd

st.title("Broker Profiles")
path = Path("data/features/broker_profiles_latest.csv")
if path.exists():
    df = pd.read_csv(path)
    st.dataframe(df, use_container_width=True)
else:
    st.info("Run python -m src.pipelines.run_v3_pipeline first.")
