import os
import logging
from pathlib import Path
import pyodbc
from typing import Dict

from pipeline.extract import read_all_csv_from_folder
from pipeline.transform import clean_dataframe
from pipeline.load import load_dataframe_to_sqlserver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# adjust if you want to use env var for connection
DEFAULT_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-TRPCHH4\\SQLEXPRESS;"
    "DATABASE=DataWarehouse;"
    "Trusted_Connection=yes;"
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATASETS_ROOT = PROJECT_ROOT / "datasets"

# expected dataset files (stems lower-cased)
EXPECTED = {
    "source_crm": ["cust_info", "prd_info", "sales_details"],
    "source_erp": ["cust_az12", "loc_a101", "px_cat_g1v2"],
}

# map expected file stem -> target SQL table (schema.table)
TARGET_TABLE_MAP: Dict[str, str] = {
    # CRM -> bronze layer tables
    "cust_info": "bronze.crm_cust_info",
    "prd_info": "bronze.crm_prd_info",
    "sales_details": "bronze.crm_sales_details",
    # ERP -> bronze layer tables (names can be adjusted)
    "cust_az12": "bronze.erp_cust_az12",
    "loc_a101": "bronze.erp_loc_a101",
    "px_cat_g1v2": "bronze.erp_px_cat_g1v2",
}


def _map_dfs_by_stem(dfs):
    """
    Return dict mapping lowercase file-stem -> dataframe using df.attrs['source_file'] when present.
    """
    mapping = {}
    for df in dfs:
        src = getattr(df, "attrs", {}).get("source_file")
        if src:
            stem = Path(src).stem.lower()
        else:
            # fallback: try to guess from DataFrame (not ideal)
            stem = None
        if stem:
            mapping[stem] = df
    return mapping


def etl_run(conn_str: str = DEFAULT_CONN_STR, commit: bool = True):
    conn = pyodbc.connect(conn_str)
    logging.info("Connected to SQL Server")

    try:
        # iterate dataset folders that exist in repo
        for folder_name, expected_files in EXPECTED.items():
            folder_path = DATASETS_ROOT / folder_name
            if not folder_path.exists():
                logging.warning("Dataset folder not found: %s", folder_path)
                continue

            logging.info("Reading CSVs from %s", folder_path)
            dfs = read_all_csv_from_folder(str(folder_path), recursive=False)
            mapped = _map_dfs_by_stem(dfs)

            for expected in expected_files:
                if expected not in mapped:
                    logging.warning("Expected file '%s' not found under %s", expected, folder_path)
                    continue

                df = mapped[expected]
                logging.info("Cleaning dataframe for %s (rows=%d)", expected, len(df))

                # generic cleaning - tweak columns lists for each file if you know schemas
                cleaned = clean_dataframe(
                    df,
                    normalize_cols=True,
                    trim_strings=True,
                    drop_duplicates=True,
                    fill_strategy="auto",
                )

                target_table = TARGET_TABLE_MAP.get(expected)
                if not target_table:
                    logging.warning("No target table mapping for %s, skipping load", expected)
                    continue

                logging.info("Loading '%s' -> %s (rows=%d)", expected, target_table, len(cleaned))
                rows = load_dataframe_to_sqlserver(
                    cleaned,
                    target_table,
                    conn=conn,
                    if_exists="append",
                    create_table=True,
                    chunksize=1000,
                    fast_executemany=True,
                )
                logging.info("Inserted %d rows into %s", rows, target_table)

        if commit:
            # run downstream stored procedures if present
            try:
                cur = conn.cursor()
                logging.info("Executing bronze.load_bronze and silver.load_silver stored procedures")
                cur.execute("EXEC bronze.load_bronze")
                cur.execute("EXEC silver.load_silver")
                conn.commit()
                logging.info("Stored procedures executed")
            except Exception as exc:
                logging.warning("Failed to execute stored procedures: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        logging.info("Connection closed")

if __name__ == "__main__":
    print("ETL STARTED")
    etl_run()       
    