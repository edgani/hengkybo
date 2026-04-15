from pathlib import Path
import streamlit as st
import pandas as pd

st.title("Adaptive Monitor")
left, right = st.columns(2)

regime_path = Path("data/features/regime_daily.csv")
drift_path = Path("data/features/drift_report.csv")

with left:
    st.subheader("Regime")
    if regime_path.exists():
        st.dataframe(pd.read_csv(regime_path).tail(20), use_container_width=True)
    else:
        st.info("No regime file yet.")

with right:
    st.subheader("Drift")
    if drift_path.exists():
        st.dataframe(pd.read_csv(drift_path), use_container_width=True)
    else:
        st.info("No drift report yet.")
