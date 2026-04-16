# Streamlit deploy fix

Root cause: old deployments may still contain `app/pages/` from earlier versions. If you run `streamlit run app/streamlit_app.py`, Streamlit auto-detects sibling pages and may crash before your app code runs.

Use one of these fixes:

1. Preferred: run the root entrypoint

```bash
streamlit run streamlit_app.py
```

2. Clean old legacy pages first

```bash
python scripts/cleanup_legacy_pages.py
```

3. If deploying on Streamlit Cloud, make sure the app file path is set to `streamlit_app.py` at the repository root.

Do not keep using `app/streamlit_app.py` if your workspace still has `app/pages/` from older builds.
