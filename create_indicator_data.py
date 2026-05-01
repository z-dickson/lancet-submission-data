#!/usr/bin/env python3
"""
create_indicator_data.py
========================
Builds aggregated indicator CSVs for Indicator 5.3.3.

Steps:
  1. Load press-release parquet (the main dataframe).
  2. Merge with parlgov_parties.xlsx on parlgov_party_id / party_id to attach
     family_name and other party metadata.
  3. Group by family_name × year × CAP_issue1 to produce sum and mean counts.

Usage
-----
  python create_indicator_data.py [--input <parquet>] [--parlgov <xlsx>]
                                  [--year 2025] [--out <dir>]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def load_data(parquet_path: Path, parlgov_path: Path) -> pd.DataFrame:
    """Load press-release dataframe and merge with parlgov party metadata."""
    df = pd.read_parquet(parquet_path)

    parlgov = pd.read_excel(parlgov_path)
    parlgov["party_id"] = pd.to_numeric(parlgov["party_id"], errors="coerce")
    df["parlgov_party_id"] = pd.to_numeric(df["parlgov_party_id"], errors="coerce")

    df = df.merge(
        parlgov[["party_id", "family_name", "family_name_short", "country_name",
                  "party_name_english", "left_right", "family_id"]],
        left_on="parlgov_party_id",
        right_on="party_id",
        how="left",
    )
    return df


def add_year(df: pd.DataFrame, year_max: int) -> pd.DataFrame:
    """Parse date column and derive year, dropping rows dated beyond year_max."""
    df["date"] = pd.to_datetime(df["date"], format="mixed", utc=True, errors="coerce")
    df["year"] = df["date"].dt.year
    return df


METRIC_COLS = ["environment_climate_issue1", "healthcare_issue1", "climate_health"]

# Maps raw parlgov family_name values to display names used in figures/CSVs.
# Values of None are excluded from the output.
FAMILY_NAME_MAP = {
    "Green/Ecologist":     "Green",
    "Right-wing":          "Radical Right-Wing",
    "Social democracy":    "Social Democracy",
    "Christian democracy": "Christian Democracy",
    "no family":           None,
    "to be coded":         None,
}


def add_issue_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add binary issue columns used by both family- and country-level aggregations."""
    top3_cols = ["CAP_issue1", "CAP_issue2", "CAP_issue3"]
    has_env    = df[top3_cols].isin(["environment and climate"]).any(axis=1)
    has_health = df[top3_cols].isin(["healthcare"]).any(axis=1)

    df["environment_climate_issue1"] = (df["CAP_issue1"] == "environment and climate").astype(int)
    df["healthcare_issue1"]          = (df["CAP_issue1"] == "healthcare").astype(int)
    df["climate_health"]             = (has_env & has_health).astype(int)
    return df


def aggregate_by_family(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by party_family and year, producing _sum and _mean columns for:
      - environment_climate_issue1  (CAP_issue1 == 'environment and climate')
      - healthcare_issue1           (CAP_issue1 == 'healthcare')
      - climate_health              (nexus: env/climate + healthcare in any top-3 issue)
    Column names match what create_figures.ipynb expects.
    """
    clean = df.dropna(subset=["family_name", "year", "CAP_issue1"]).copy()
    clean["family_name"] = clean["family_name"].map(lambda x: FAMILY_NAME_MAP.get(x, x))
    clean = clean[clean["family_name"].notna()].copy()
    clean = clean.rename(columns={"family_name": "party_family"})
    clean = add_issue_columns(clean)

    grouped = clean.groupby(["party_family", "year"])
    sums  = grouped[METRIC_COLS].sum().add_suffix("_sum")
    means = grouped[METRIC_COLS].mean().add_suffix("_mean")

    result = pd.concat([sums, means], axis=1).reset_index()

    ordered = ["party_family", "year"]
    for col in METRIC_COLS:
        ordered += [f"{col}_sum", f"{col}_mean"]
    return result[ordered]


def aggregate_by_country(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by country and year, producing _sum and _mean columns for the same
    three metrics as aggregate_by_family.
    """
    clean = df.dropna(subset=["country", "year", "CAP_issue1"]).copy()
    clean = add_issue_columns(clean)

    grouped = clean.groupby(["country", "year"])
    sums  = grouped[METRIC_COLS].sum().add_suffix("_sum")
    means = grouped[METRIC_COLS].mean().add_suffix("_mean")

    result = pd.concat([sums, means], axis=1).reset_index()

    ordered = ["country", "year"]
    for col in METRIC_COLS:
        ordered += [f"{col}_sum", f"{col}_mean"]
    return result[ordered]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, default=2025,
                        help="Report year; data up to this year are included (default: 2025)")
    parser.add_argument("--input", type=str, default=None,
                        help="Path to press_releases_all_with_CAP_issues.parquet")
    parser.add_argument("--parlgov", type=str, default=None,
                        help="Path to parlgov_parties.xlsx")
    parser.add_argument("--out", type=str, default=None,
                        help="Output directory (default: <year>/data/ relative to this script)")
    args = parser.parse_args()

    year_max = args.year
    root = Path(__file__).resolve().parent

    parquet_path = Path(args.input) if args.input else root.parent / "press_releases_all_with_CAP_issues.parquet"
    parlgov_path = Path(args.parlgov) if args.parlgov else root.parent / "parlgov_parties.xlsx"
    out_dir = Path(args.out) if args.out else root / str(year_max) / "data"

    for path, label in [(parquet_path, "parquet"), (parlgov_path, "parlgov xlsx")]:
        if not path.exists():
            sys.exit(f"ERROR: {label} not found at {path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {parquet_path.name} ...")
    df = load_data(parquet_path, parlgov_path)
    print(f"  Rows after merge: {len(df):,}")

    df = add_year(df, year_max)
    df = df[(df["year"] >= 2010) & (df["year"] <= year_max)].copy()
    print(f"  Rows after year filter (2010–{year_max}): {len(df):,}")

    print("\nAggregating by party_family × year ...")
    df_family = aggregate_by_family(df)
    out_path = out_dir / "indicator_5_3_3.csv"
    df_family.to_csv(out_path, index=False)
    print(f"  {len(df_family):,} rows  →  {out_path}")

    print("\nAggregating by country × year ...")
    df_country = aggregate_by_country(df)
    out_path = out_dir / "indicator_5_3_3_country.csv"
    df_country.to_csv(out_path, index=False)
    print(f"  {len(df_country):,} rows  →  {out_path}")


if __name__ == "__main__":
    main()
