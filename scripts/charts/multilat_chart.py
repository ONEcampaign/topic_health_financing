"""Clean and analyse CRS data for multilateral (MDB) donors."""
import pandas as pd

from scripts.analysis.multilateral import (
    add_broad_sectors_column,
    add_income_levels,
    add_region_groups,
    add_sectors_column,
    filter_mdb_data,
    filter_multi_donors,
    read_raw_data,
    summarise_by_donor_recipient_year_flow_sector,
    to_constant_dac,
    rename_ambiguous_recipients,
)
from scripts.config import PATHS


def rename_regions(df: pd.DataFrame) -> pd.DataFrame:
    regions = {
        "Africa": "Africa",
        "South America": "South America",
        "Caribbean & Central America": "North & Central America",
        "America": "North & Central America",
        "Europe": "Europe",
        "Middle East": "Middle East",
        "South & Central Asia": "Asia",
        "Far East Asia": "Asia",
        "Asia": "Asia",
        "Oceania": "Oceania",
        "Regional and Unspecified": "Other, unspecified",
    }

    return df.assign(region_name=lambda d: d.region_name.map(regions))


def filter_health_broad(df: pd.DataFrame) -> pd.DataFrame:
    return df.query("broad_sector == 'Health'")


def summarise_health_disbursements(df: pd.DataFrame) -> pd.DataFrame:
    group_by = [
        "year",
        "broad_sector",
        "flow_name",
        "region_name",
        "recipient_name",
        "donor_name",
    ]

    return df.groupby(
        group_by,
        as_index=False,
        observed=True,
        dropna=False,
    )["usd_disbursement"].sum()


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.filter(
        [
            "year",
            "broad_sector",
            "flow_name",
            "region_name",
            "recipient_name",
            "donor_name",
            "usd_disbursement",
        ],
        axis=1,
    )


def create_health_total(df: pd.DataFrame) -> pd.DataFrame:
    group_by = ["year", "broad_sector", "flow_name", "region_name", "recipient_name"]
    return (
        df.groupby(group_by, as_index=False)["usd_disbursement"]
        .sum()
        .assign(donor_name="All")
    )


def pivot_chart_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(
        index=["year", "broad_sector", "flow_name", "region_name", "recipient_name"],
        columns="donor_name",
        values="usd_disbursement",
    ).reset_index()


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.set_index(
        ["year", "broad_sector", "flow_name", "region_name", "recipient_name", "Total"]
    ).reset_index()


def clean_numbers(df: pd.DataFrame) -> pd.DataFrame:
    return df.round(5).assign(Total=lambda d: round(d.Total, 4))


def filter_from_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return df.query(f"year >= {year}").reset_index(drop=True)


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=["broad_sector"]).rename(
        columns={
            "flow_name": "Type",
            "region_name": "Region",
            "recipient_name": "Recipient",
            "year": "Year",
        }
    )


def chart_4_1() -> None:
    """Pipeline for chart 4.1"""

    # read the raw crs data. Years are controlled from the config file
    data = read_raw_data()

    # Create a 'health' dataframe for multilaterals (MDBs)
    health = (
        data.pipe(filter_multi_donors)
        .pipe(filter_mdb_data)
        .pipe(add_sectors_column)
        .pipe(add_broad_sectors_column)
        .pipe(summarise_by_donor_recipient_year_flow_sector)
        .pipe(to_constant_dac)
        .pipe(rename_ambiguous_recipients)
        .pipe(add_income_levels)
        .pipe(add_region_groups)
        .pipe(rename_regions)
        .pipe(filter_health_broad)
        .pipe(summarise_health_disbursements)
        .pipe(filter_columns)
    )

    # Create a 'total' summary of the data. This produces a 'Total' for all mdbs
    health_total = health.pipe(create_health_total)

    # Combine the detailed and total data into a single dataframe
    chart_data = pd.concat([health, health_total], ignore_index=True)

    # Pivot the data for the chart
    chart_data = chart_data.pipe(pivot_chart_data)

    # Create the chart dataframe (reorder, clean, filter, sort)
    chart = (
        chart_data.pipe(reorder_columns)
        .pipe(clean_numbers)
        .pipe(filter_from_year, 2016)
        .sort_values(["year"], ascending=False)
        .pipe(clean_columns)
    )

    chart.to_csv(PATHS.output / "section_5_chart.csv", index=False)


if __name__ == "__main__":
    chart_4_1()