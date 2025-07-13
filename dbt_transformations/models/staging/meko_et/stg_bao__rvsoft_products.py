import polars as pl
from dbfread import DBF
import os


def normalize_values(lazyframe, column_name: str):
    return lazyframe.with_columns(
        pl.col(column_name)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"\s|[-/._]", "")
        .alias(f"{column_name}_normalized")
    )


def model(dbt, session):
    dbf_file_path = f"{os.getenv('DBT_EXTERNAL')}/bao/LM_MAT.DBF"

    dbf = DBF(
        dbf_file_path,
        ignore_missing_memofile=True,
        char_decode_errors="ignore",
        encoding="cp775",
        lowernames=True,
    )

    df: pl.DataFrame = (
        pl.LazyFrame(
            iter(dbf),
            infer_schema_length=1000_000,
            strict=False,
        )
        .pipe(normalize_values, "tkood")
        .collect()
    )

    result = df.to_pandas()

    return result
