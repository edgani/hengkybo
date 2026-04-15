import polars as pl


def build_macro_alignment(features: pl.DataFrame) -> pl.DataFrame:
    # Placeholder until a true market regime feed is wired in.
    return features.with_columns(
        pl.lit(50.0).alias("macro_alignment_score")
    )
