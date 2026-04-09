from typing import List, Dict, Optional, Sequence
import re
import logging
import pandas as pd


def _normalize_column_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def normalize_column_names(df: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """
    Lowercase, strip and replace non-alphanum characters with underscores.
    """
    if not inplace:
        df = df.copy()
    mapping = {c: _normalize_column_name(c) for c in df.columns}
    df.rename(columns=mapping, inplace=True)
    return df


def infer_and_cast_types(
    df: pd.DataFrame,
    date_columns: Optional[Sequence[str]] = None,
    numeric_columns: Optional[Sequence[str]] = None,
    dtype_casts: Optional[Dict[str, str]] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Convert types:
      - date_columns: list of cols to parse as datetime (coerce errors to NaT)
      - numeric_columns: list of cols to convert to numeric (coerce errors to NaN)
      - dtype_casts: dict of {col: dtype} for explicit casting (pandas dtypes)
    """
    if not inplace:
        df = df.copy()

    if date_columns:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    if numeric_columns:
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    if dtype_casts:
        for col, dtype in dtype_casts.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as exc:
                    logging.warning("Failed to cast %s to %s: %s", col, dtype, exc)

    return df


def fill_nulls(
    df: pd.DataFrame,
    strategy: str = "auto",
    fill_values: Optional[Dict[str, object]] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Fill nulls in dataframe.
      - strategy:
          'auto'  : numeric -> median, object/category -> mode (or empty string), datetime -> leave
          'zero'  : numeric -> 0, object -> ''
          'ffill' : forward fill
          'bfill' : backward fill
      - fill_values: explicit per-column fill values that override strategy
    """
    if not inplace:
        df = df.copy()

    if fill_values:
        df.fillna(value=fill_values, inplace=True)

    if strategy == "ffill":
        df.fillna(method="ffill", inplace=True)
        return df
    if strategy == "bfill":
        df.fillna(method="bfill", inplace=True)
        return df

    for col in df.columns:
        if col in (fill_values or {}):
            continue
        ser = df[col]
        if strategy == "zero" and pd.api.types.is_numeric_dtype(ser):
            df[col].fillna(0, inplace=True)
            continue
        if pd.api.types.is_numeric_dtype(ser):
            if strategy == "auto":
                try:
                    median = ser.median(skipna=True)
                    df[col].fillna(median, inplace=True)
                except Exception:
                    df[col].fillna(0, inplace=True)
        elif pd.api.types.is_datetime64_any_dtype(ser):
            # leave datetime NaT by default on 'auto' to preserve missingness
            if strategy == "zero":
                df[col].fillna(pd.Timestamp(0), inplace=True)
        else:
            # object / categorical / string
            if strategy in ("auto", "zero"):
                # try mode
                try:
                    mode = ser.mode(dropna=True)
                    if not mode.empty:
                        df[col].fillna(mode.iloc[0], inplace=True)
                    else:
                        df[col].fillna("", inplace=True)
                except Exception:
                    df[col].fillna("", inplace=True)
    return df


def trim_string_columns(df: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """
    Strip leading/trailing whitespace from string/object columns.
    """
    if not inplace:
        df = df.copy()

    for col in df.select_dtypes(include=["object", "string"]).columns:
        try:
            df[col] = df[col].astype("string").str.strip().replace(pd.NA, None)
        except Exception:
            # fallback: attempt elementwise strip
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def clean_dataframe(
    df: pd.DataFrame,
    *,
    normalize_cols: bool = True,
    trim_strings: bool = True,
    drop_duplicates: bool = True,
    drop_columns: Optional[Sequence[str]] = None,
    rename_columns: Optional[Dict[str, str]] = None,
    date_columns: Optional[Sequence[str]] = None,
    numeric_columns: Optional[Sequence[str]] = None,
    dtype_casts: Optional[Dict[str, str]] = None,
    fill_strategy: str = "auto",
    fill_values: Optional[Dict[str, object]] = None,
) -> pd.DataFrame:
    """
    Perform common cleaning steps and return a cleaned DataFrame.
    """
    df = df.copy()

    if normalize_cols:
        df = normalize_column_names(df, inplace=True)

    if rename_columns:
        df.rename(columns=rename_columns, inplace=True)

    if trim_strings:
        df = trim_string_columns(df, inplace=True)

    if date_columns or numeric_columns or dtype_casts:
        df = infer_and_cast_types(
            df,
            date_columns=date_columns,
            numeric_columns=numeric_columns,
            dtype_casts=dtype_casts,
            inplace=True,
        )

    df = fill_nulls(df, strategy=fill_strategy, fill_values=fill_values, inplace=True)

    if drop_duplicates:
        try:
            df.drop_duplicates(inplace=True)
        except Exception as exc:
            logging.warning("drop_duplicates failed: %s", exc)

    if drop_columns:
        try:
            df.drop(columns=[c for c in drop_columns if c in df.columns], inplace=True)
        except Exception as exc:
            logging.warning("drop columns failed: %s", exc)

    return df


def clean_dataframes(
    dfs: List[pd.DataFrame],
    **clean_kwargs,
) -> List[pd.DataFrame]:
    """
    Apply clean_dataframe to a list of DataFrames and return cleaned list.
    """
    return [clean_dataframe(df, **clean_kwargs) for df in dfs]