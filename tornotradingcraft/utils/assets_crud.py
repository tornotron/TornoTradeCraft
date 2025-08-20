import pandas as pd
from pathlib import Path

def parse_asset_file(file_path: str, file_type: str) -> pd.DataFrame:
    """Parse a CSV or Excel ticker file into a normalized DataFrame.

    Reads the source file without assuming an existing header row. The first
    row of the file is treated as the column names and removed from the
    returned DataFrame. Rows that are entirely empty are dropped and all
    values are converted to strings.

    Args:
        file_path (str): Path to the CSV or Excel file to parse.
        file_type (str): File type indicator, either "csv" or "xlsx".

    Returns:
        pd.DataFrame: Normalized DataFrame with string-typed values and
            header taken from the file's first row.

    Raises:
        ValueError: If `file_type` is not one of "csv" or "xlsx".
    """
    if file_type == "csv":
        # Read the CSV file without header
        df = pd.read_csv(file_path, header=None)

        # Extract the first row to use as column names
        new_column_names = df.iloc[0].tolist()

        # Set the new column names
        df.columns = new_column_names

        # Drop the first row which was used as column names
        df = df.drop(df.index[0])
    elif file_type == "xlsx":
        # Read the Excel file without header
        df = pd.read_excel(file_path, header=None, dtype=str)

        # Extract the first row to use as column names
        new_column_names = df.iloc[0].tolist()

        # Set the new column names
        df.columns = new_column_names

        # Drop the first row which was used as column names
        df = df.drop(df.index[0]).reset_index(drop=True)
    else:
        raise ValueError("Unsupported file type")

    # Drop rows where all values are null
    df = df.dropna(axis=0, how="all")

    # Convert all data types to string
    df = df.astype(str)

    return df


def convert_to_parquet_and_store(df: pd.DataFrame, parquet_name: str | None = None, assets_dir: Path | None = None) -> str:
    """Store a DataFrame as a parquet file inside the package assets folder (or an explicit folder).

    Args:
        df (pd.DataFrame): DataFrame to write.
        parquet_name (str | None): Filename for the parquet file. If None, caller should
            provide one; this function will raise ValueError when missing.
        assets_dir (Path | None): Optional directory to store the asset. If not provided,
            defaults to the package's `assets` directory under `tornotradingcraft`.

    Returns:
        str: Absolute path to the written parquet file.

    Raises:
        ValueError: If `parquet_name` is not provided.
        RuntimeError: If writing the parquet file fails (pyarrow/fastparquet not installed).
    """
    if assets_dir is None:
        package_root = Path(__file__).resolve().parent.parent  # tornotradingcraft/
        assets_dir = package_root / "assets"
    else:
        assets_dir = Path(assets_dir)

    # Ensure the directory exists
    assets_dir.mkdir(parents=True, exist_ok=True)

    if parquet_name is None:
        raise ValueError("parquet_name must be provided to store the asset file")

    out_path = assets_dir / parquet_name

    try:
        # pandas accepts a string path; convert Path to str for compatibility with type checkers
        df.to_parquet(str(out_path), index=False)
    except Exception as exc:
        raise RuntimeError(
            "Failed to write parquet. Install 'pyarrow' or 'fastparquet' (e.g. pip install pyarrow) and retry."
        ) from exc

    return str(out_path)


def update_asset_file(file_path: str, parquet_name: str | None = None) -> str:
    """Convert a CSV or Excel file to a parquet file and store it in the package assets.

    The function detects the input file type by extension (supports .csv, .xls, .xlsx),
    parses the file with `parse_asset_file`, and delegates writing the parquet file to
    `store_asset_file` which stores the file in the package `assets` directory by default.

    Args:
        file_path (str): Path to the source CSV/XLSX file.
        parquet_name (str | None): Optional output filename for the parquet file. If None,
            the source file stem with a `.parquet` suffix is used.

    Returns:
        str: Absolute path to the written parquet file.

    Raises:
        ValueError: If the input file extension is unsupported.
        RuntimeError: If writing the parquet file fails (propagated from store_asset_file).
    """

    p = Path(file_path)
    suffix = p.suffix.lower()

    if suffix == ".csv":
        file_type = "csv"
    elif suffix in (".xls", ".xlsx"):
        file_type = "xlsx"
    else:
        raise ValueError(f"Unsupported file extension: {suffix}")

    # Parse using the existing helper
    df = parse_asset_file(str(p), file_type)

    if parquet_name is None:
        parquet_name = p.stem + ".parquet"
    else:
        parquet_name = parquet_name if parquet_name.endswith(".parquet") else parquet_name + ".parquet"

    # Delegate actual storage to the helper
    return convert_to_parquet_and_store(df, parquet_name)
