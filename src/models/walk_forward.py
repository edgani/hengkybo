from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator
import polars as pl


@dataclass
class WalkForwardWindow:
    train_start: str
    train_end: str
    validate_start: str
    validate_end: str
    test_start: str
    test_end: str


def generate_walk_forward_windows(dates: list[str], train_len: int = 504, validate_len: int = 126, test_len: int = 63) -> Iterator[WalkForwardWindow]:
    total = len(dates)
    i = 0
    while i + train_len + validate_len + test_len <= total:
        yield WalkForwardWindow(
            train_start=dates[i],
            train_end=dates[i + train_len - 1],
            validate_start=dates[i + train_len],
            validate_end=dates[i + train_len + validate_len - 1],
            test_start=dates[i + train_len + validate_len],
            test_end=dates[i + train_len + validate_len + test_len - 1],
        )
        i += test_len
