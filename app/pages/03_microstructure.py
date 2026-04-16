import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace

st.set_page_config(page_title='Intraday Microstructure', layout='wide')
st.title('Intraday Microstructure')
ws = load_workspace()

if ws.intraday.empty:
    st.info('No intraday feature file detected yet. Run intraday pipeline when done-detail/orderbook real data sudah ada.')
else:
    st.caption(f'Rows: {len(ws.intraday)}')
    st.dataframe(ws.intraday, use_container_width=True, hide_index=True)
