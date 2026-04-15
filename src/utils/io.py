from pathlib import Path
import polars as pl


def read_table(path: str | Path) -> pl.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path, try_parse_dates=True)
    if suffix in {".parquet", ".pq"}:
        return pl.read_parquet(path)
    raise ValueError(f"Unsupported file format: {path}")


def ensure_dir(path: str | Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path
