from typing import Optional
import logging
import math
import pandas as pd


def _sql_type_from_series(s: pd.Series) -> str:
    # conservative mappings
    if pd.api.types.is_integer_dtype(s):
        return "BIGINT"
    if pd.api.types.is_float_dtype(s):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(s):
        return "BIT"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "DATETIME2"
    # fallback for all objects/strings/categories
    return "NVARCHAR(MAX)"


def _normalize_name(name: str) -> str:
    return f"[{name.replace(']', ']]')}]".replace("[]", "")


def load_dataframe_to_sqlserver(
    df: pd.DataFrame,
    table: str,
    *,
    conn_str: Optional[str] = None,
    conn=None,
    if_exists: str = "append",  # 'append' | 'replace' | 'fail'
    create_table: bool = True,
    chunksize: int = 1000,
    fast_executemany: bool = True,
) -> int:
    """
    Load a pandas DataFrame into a SQL Server table using pyodbc.

    Parameters:
    - df: DataFrame to load
    - table: table name, optionally 'schema.table'. If schema omitted, 'dbo' used.
    - conn_str: pyodbc connection string (used if conn not provided)
    - conn: existing pyodbc connection (preferred)
    - if_exists: 'append' (default), 'replace' (drop/create), 'fail' (raise if exists)
    - create_table: if True and table doesn't exist (or replace), create table from dtypes
    - chunksize: number of rows per executemany batch
    - fast_executemany: set cursor.fast_executemany = True (faster on MS drivers)

    Returns:
    - number of rows inserted
    """
    import pyodbc

    if conn is None and not conn_str:
        raise ValueError("Either conn (pyodbc.Connection) or conn_str must be provided")

    own_conn = False
    if conn is None:
        conn = pyodbc.connect(conn_str)
        own_conn = True

    try:
        schema = "dbo"
        tbl = table
        if "." in table:
            parts = table.split(".", 1)
            schema, tbl = parts[0], parts[1]

        full_table_name = f"{_normalize_name(schema)}.{_normalize_name(tbl)}"
        cursor = conn.cursor()

        # check existence
        exists_query = """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        cursor.execute(exists_query, (schema, tbl))
        exists = cursor.fetchone()[0] > 0

        if exists and if_exists == "fail":
            raise RuntimeError(f"Table {full_table_name} already exists (if_exists='fail')")

        if exists and if_exists == "replace":
            cursor.execute(f"DROP TABLE {full_table_name}")
            conn.commit()
            exists = False

        if not exists and create_table:
            cols_defs = []
            for col in df.columns:
                col_name = _normalize_name(col)
                dtype = _sql_type_from_series(df[col])
                cols_defs.append(f"{col_name} {dtype} NULL")

            cols_sql = ",\n  ".join(cols_defs)
            create_sql = f"CREATE TABLE {full_table_name} (\n  {cols_sql}\n)"
            cursor.execute(create_sql)
            conn.commit()

        # prepare insert
        cols = [f"{_normalize_name(c)}" for c in df.columns]
        placeholders = ",".join("?" for _ in df.columns)
        insert_sql = f"INSERT INTO {full_table_name} ({', '.join(cols)}) VALUES ({placeholders})"

        if fast_executemany:
            try:
                cursor.fast_executemany = True
            except Exception:
                pass

        total = 0
        # convert DataFrame rows to tuples with None for NaN/NaT
        def row_iter(df_chunk: pd.DataFrame):
            for row in df_chunk.itertuples(index=False, name=None):
                converted = []
                for v in row:
                    if v is None:
                        converted.append(None)
                        continue
                    # pandas NA / numpy nan
                    try:
                        if pd.isna(v):
                            converted.append(None)
                            continue
                    except Exception:
                        pass
                    # convert NaT
                    if isinstance(v, pd.Timestamp) and pd.isnull(v):
                        converted.append(None)
                        continue
                    # python bool/int/float/str/datetime are fine
                    converted.append(v)
                yield tuple(converted)

        for start in range(0, len(df), chunksize):
            chunk = df.iloc[start : start + chunksize]
            params = list(row_iter(chunk))
            if not params:
                continue
            cursor.executemany(insert_sql, params)
            total += len(params)
            # commit per chunk to avoid long transactions
            conn.commit()

        return total
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        if own_conn:
            conn.close()