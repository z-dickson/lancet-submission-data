"""
climate_health_nexus.py

Assigns climate/health nexus variables and saves aggregated files for the
Lancet indicator submission.

Outputs (saved to lancet-submission-data/<year>/data/):
  - indicator_5_3_3.csv         (party family x year)
  - indicator_5_3_3_country.csv (country x year)

Usage:
    python climate_health_nexus.py <year>
"""

import os
import sys

import numpy as np
import pandas as pd
import polars as pl

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

PARQUET_PATH = os.path.join(ROOT, "press_releases_all_with_CAP_issues.parquet")
PARLGOV_PATH = os.path.join(ROOT, "party_press", "party-data", "parlgov_parties.xlsx")

# Party-level data quality thresholds (matching create_figures.ipynb)
MIN_PARTY_TOTAL = 200  # exclude parties with fewer total press releases overall
MIN_PARTY_YEAR  = 40   # flag individual years below this as low-reliability


def main(year: int) -> None:
    out_dir = os.path.join(ROOT, "lancet-submission-data", str(year), "data")

    # ── Load data ─────────────────────────────────────────────────────────────
    print(f"Loading {PARQUET_PATH} ...")
    df = pl.read_parquet(PARQUET_PATH).to_pandas()
    parlgov = pd.read_excel(PARLGOV_PATH)

    # ── Climate-health nexus flag ─────────────────────────────────────────────
    mask = (
        ((df["CAP_issue1"] == "healthcare") & (df["CAP_issue2"] == "environment and climate"))
        | ((df["CAP_issue1"] == "environment and climate") & (df["CAP_issue2"] == "healthcare"))
    )
    df["climate_health"] = np.where(mask, 1, 0)

    # ── Party family / ideology from parlgov ──────────────────────────────────
    parlgov_id_numeric = pd.to_numeric(df["parlgov_party_id"], errors="coerce")
    id_to = lambda col: dict(zip(parlgov["party_id"], parlgov[col]))  # noqa: E731

    df["party_family"] = parlgov_id_numeric.map(id_to("family_name"))
    df["left_right"] = parlgov_id_numeric.map(id_to("left_right"))
    df["family_id"] = parlgov_id_numeric.map(id_to("family_id"))

    # ── Date / year ───────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"], format="mixed", utc=True)
    df["year"] = df["date"].dt.year
    df.loc[df["year"] > year, "year"] = year

    # ── Issue dummies ─────────────────────────────────────────────────────────
    df["environment_climate_issue1"] = np.where(df["CAP_issue1"] == "environment and climate", 1, 0)
    df["environment_climate_issue2"] = np.where(df["CAP_issue2"] == "environment and climate", 1, 0)
    df["healthcare_issue1"] = np.where(df["CAP_issue1"] == "healthcare", 1, 0)
    df["healthcare_issue2"] = np.where(df["CAP_issue2"] == "healthcare", 1, 0)

    # ── Party family name cleanup ─────────────────────────────────────────────
    df["party_family"] = (
        df["party_family"]
        .str.title()
        .str.replace("Green/Ecologist", "Green", regex=False)
        .str.replace("Right-Wing", "Radical Right-Wing", regex=False)
    )

    # ── Country name cleanup ──────────────────────────────────────────────────
    df["country"] = df["country"].str.title()

    # ── Aggregate: party family x year ───────────────────────────────────────
    party_grouped = (
        df.groupby(["party_family", "year"])
        .agg(
            environment_climate_issue1_sum=("environment_climate_issue1", "sum"),
            environment_climate_issue1_mean=("environment_climate_issue1", "mean"),
            healthcare_issue1_sum=("healthcare_issue1", "sum"),
            healthcare_issue1_mean=("healthcare_issue1", "mean"),
            climate_health_sum=("climate_health", "sum"),
            climate_health_mean=("climate_health", "mean"),
        )
        .reset_index()
    )
    party_grouped = party_grouped[party_grouped["year"] >= 2010]

    # ── Aggregate: country x year ─────────────────────────────────────────────
    country_grouped = (
        df.groupby(["country", "year"])
        .agg(
            environment_climate_issue1_sum=("environment_climate_issue1", "sum"),
            environment_climate_issue1_mean=("environment_climate_issue1", "mean"),
            healthcare_issue1_sum=("healthcare_issue1", "sum"),
            healthcare_issue1_mean=("healthcare_issue1", "mean"),
            climate_health_sum=("climate_health", "sum"),
            climate_health_mean=("climate_health", "mean"),
        )
        .reset_index()
    )
    country_grouped = country_grouped[country_grouped["year"] >= 2010]
    country_grouped["country"] = country_grouped["country"].str.replace("Uk", "UK", regex=False)

    # ── Aggregate: individual party x year ───────────────────────────────────
    indiv_party_grouped = (
        df.groupby(["country", "party", "party_family", "year"])
        .agg(
            environment_climate_mean=("environment_climate_issue1", "mean"),
            healthcare_mean=("healthcare_issue1", "mean"),
            climate_health_mean=("climate_health", "mean"),
            n_total=("environment_climate_issue1", "count"),
        )
        .reset_index()
    )

    # Drop parties whose total press releases across all years is below threshold
    party_totals = indiv_party_grouped.groupby(["country", "party"])["n_total"].sum()
    valid_parties = party_totals[party_totals >= MIN_PARTY_TOTAL].reset_index()[["country", "party"]]
    indiv_party_grouped = indiv_party_grouped.merge(valid_parties, on=["country", "party"], how="inner")

    # Flag years where the party has too few press releases to interpret reliably
    indiv_party_grouped["low_data"] = indiv_party_grouped["n_total"] < MIN_PARTY_YEAR

    indiv_party_grouped = indiv_party_grouped[indiv_party_grouped["year"] >= 2010]

    # ── Save outputs ──────────────────────────────────────────────────────────
    os.makedirs(out_dir, exist_ok=True)

    party_out = os.path.join(out_dir, "indicator_5_3_3.csv")
    country_out = os.path.join(out_dir, "indicator_5_3_3_country.csv")
    indiv_party_out = os.path.join(out_dir, "indicator_5_3_3_party.csv")

    party_grouped.to_csv(party_out, index=False)
    country_grouped.to_csv(country_out, index=False)
    indiv_party_grouped.to_csv(indiv_party_out, index=False)

    print(f"Saved {party_out}")
    print(f"Saved {country_out}")
    print(f"Saved {indiv_party_out}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python climate_health_nexus.py <year>")
        sys.exit(1)
    main(int(sys.argv[1]))
