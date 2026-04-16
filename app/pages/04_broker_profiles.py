import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace, broker_coverage_report

st.set_page_config(page_title='Broker Profiles', layout='wide')
st.title('Broker Profiles')
ws = load_workspace()

cov = broker_coverage_report(ws.brokers, ws.broker_master)

left, right = st.columns(2)
with left:
    st.subheader('Latest broker profiles')
    if ws.profiles.empty:
        st.warning('No broker profile file yet.')
    else:
        st.dataframe(ws.profiles, use_container_width=True, hide_index=True)
with right:
    st.subheader('Broker coverage vs master')
    if cov.empty:
        st.warning('No broker master or broker summary coverage yet.')
    else:
        st.dataframe(cov, use_container_width=True, hide_index=True)
