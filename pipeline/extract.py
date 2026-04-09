from pathlib import Path
from typing import List, Union
import pandas as pd
import logging

def read_all_csv_from_folder(folder: Union[str, Path],
                             recursive: bool = False,
                             encoding: str = "utf-8",
                             **pd_read_csv_kwargs) -> List[pd.DataFrame]:
    """
    Read all CSV files from a folder and return a list of DataFrames.
    - folder: path to the folder containing CSV files
    - recursive: if True, search subfolders as well
    - encoding: file encoding for pd.read_csv
    - pd_read_csv_kwargs: extra kwargs passed to pandas.read_csv

    Returns: list of pandas.DataFrame (in sorted filename order).
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    files = sorted(folder_path.rglob("*.csv") if recursive else folder_path.glob("*.csv"))
    dfs: List[pd.DataFrame] = []
    for file_path in files:
        try:
            df = pd.read_csv(file_path, encoding=encoding, **pd_read_csv_kwargs)
            # optionally store source file path on the DataFrame
            try:
                df.attrs["source_file"] = str(file_path)
            except Exception:
                pass
            dfs.append(df)
        except Exception as exc:
            logging.warning("Failed to read CSV %s: %s", file_path, exc)

    return dfs