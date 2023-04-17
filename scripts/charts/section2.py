"""
Analyze health spending data across countries, focusing spending levels against various measures.
Use to process, filter and aggregate the data
"""

import json
from functools import partial

import pandas as pd

from scripts import config
from scripts.analysis.read_data_versions import read_spending_data_versions
from scripts.charts.common import (
    flag_africa,
    get_version,
    reorder_by_income,
)
from scripts.tools import value2gdp_share, value2gov_spending_share, value2pc

# Load a dictionary with dataframes for the different versions of "gov_spending" data
# These include: 'lcu', 'gdp_share','usd_current', 'usd_constant', 'usd_constant_pc'
GOV_SPENDING = read_spending_data_versions(dataset_name="gov_spending")

# Create a function to get a specific version. It returns a dataframe.
# Note that the `get_version` function, of which this is a partial implementation,
# handles some basic transformations of the data (like adding income levels or filtering
# out certain countries).
get_spending_version = partial(get_version, versions_dict=GOV_SPENDING)


def get_government_spending_shares(constant_usd_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate share of government spending"""
    # Calculate as a share of government spending for individual countries
    return value2gov_spending_share(constant_usd_data)


def get_gdp_spending_shares(constant_usd_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate share of gdp"""
    # Calculate as a share of government spending for income groups (total)
    return value2gdp_share(constant_usd_data)


def get_per_capita_spending(constant_usd_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate per capita figures"""
    # Calculate as a share of government spending for income groups (total)
    return value2pc(constant_usd_data)


def read_au_countries() -> list:
    """Read AU member countries"""
    with open(config.PATHS.raw_data / "AU_members.json", "r") as f:
        au_members = json.load(f)

    return au_members


def filter_au_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter a dataframe to keep only AU countries"""
    return df.query(f"iso_code in {read_au_countries()}")


def clean_data_for_chart(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and reorder the data for the Flourish chart"""

    return (
        df.filter(["country_name", "income_group", "value", "year"], axis=1)
        .pipe(reorder_by_income)
        .assign(
            year_note=lambda d: d["year"].dt.year,
            year=lambda d: d["year"].dt.strftime("%Y-%m-%d"),
        )
    )


def chart_2_2_1() -> None:
    """Data for chart 1 in section 2"""
    # Get spending in constant USD
    constant_data = get_spending_version(version="usd_constant")

    # Transform
    df_gdp = (
        get_gdp_spending_shares(constant_usd_data=constant_data)
        .pipe(clean_data_for_chart)
        .pipe(flag_africa)
        .drop(columns="year")
    )

    # Save
    df_gdp.to_csv(config.PATHS.output / "section2_chart2_1.csv", index=False)


def chart_2_2_2() -> None:
    """Data for chart 2 in section 2"""
    # Get spending in constant USD
    constant_data = get_spending_version(version="usd_constant")

    # Transform
    df_pc = (
        get_per_capita_spending(constant_usd_data=constant_data)
        .pipe(clean_data_for_chart)
        .pipe(flag_africa)
        .drop(columns="year")
    )
    # Save
    df_pc.to_csv(config.PATHS.output / "section2_chart2_2.csv", index=False)


def chart_2_3() -> None:
    """Data for chart 3 in section 2"""
    # Get spending in constant USD
    constant_data = get_spending_version(version="usd_constant")

    # Transform
    df = (
        get_government_spending_shares(constant_usd_data=constant_data)
        .pipe(filter_au_countries)
        .pipe(clean_data_for_chart)
        .drop(columns="year")
    )

    df.to_csv(config.PATHS.output / "section2_chart3.csv", index=False)


if __name__ == "__main__":
    chart_2_2_1()
    chart_2_2_2()
    chart_2_3()
