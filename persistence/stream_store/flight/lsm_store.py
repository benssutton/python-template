import polars as pl

ORDER_COLUMN = "seqno"


def _merge_frame(frames: tuple[pl.DataFrame, ...],
                 key_columns: list[str]) -> pl.DataFrame | None:
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical")
    # Window function: rank rows within each key partition by recency
    # (newest seqno first). key_columns is the single extension point:
    # ["id"] today, ["id", "version"] later, with no other change.
    winners = (
        combined
        .with_columns(
            pl.col(ORDER_COLUMN)
            .rank("ordinal", descending=True)
            .over(key_columns)
            .alias("_rn")
        )
        .filter(pl.col("_rn") == 1)
        .drop("_rn")
    )
    return winners


def _merge_to_rows(frames: tuple[pl.DataFrame, ...],
                   key_columns: list[str],
                   limit: int | None) -> tuple[list[dict], int]:
    winners = _merge_frame(frames, key_columns)
    if winners is None:
        return [], 0
    live = winners.filter(pl.col("op") != "delete").sort(key_columns)
    total = live.height
    if limit is not None:
        live = live.head(limit)
    return live.select(["id", "name", "value"]).to_dicts(), total
