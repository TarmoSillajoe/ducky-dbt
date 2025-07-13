import polars as pl
from dbfread import DBF
import os


def model(dbt, session):
    dbf_file_path = f"{os.getenv('DBT_EXTERNAL')}/bao/LM_MAT.DBF"

    dbf = DBF(
        dbf_file_path,
        ignore_missing_memofile=True,
        char_decode_errors="ignore",
        encoding="cp775",
        lowernames=True,
    )

    df: pl.DataFrame = pl.LazyFrame(
        iter(dbf),
        infer_schema_length=1000_000,
        strict=False,
    ).collect()

    result = df.to_pandas()

    return result
