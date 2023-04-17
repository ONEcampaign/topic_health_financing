"""
Analyze health spending data across countries, focusing on per capita spending,
total spending, share of GDP, and share of government spending.

Use to process, filter and aggregate the data

"""
from functools import partial

import pandas as pd
from bblocks import convert_id

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    combine_income_countries,
    get_version,
    per_capita_africa,
    per_capita_by_income,
    total_africa,
    total_by_income,
)
from scripts.config import PATHS
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
    value2gov_spending_share,
    value2gov_spending_share_group,
)

# Load a dictionary with dataframes for the different versions of "health_spending" data
# These include: 'lcu', 'gdp_share','usd_current', 'usd_constant', 'usd_constant_pc'
SPENDING = read_spending_data_versions(dataset_name="health_spending")

# Create a function to get a specific version. It returns a dataframe.
# Note that the `get_version` function, of which this is a partial implementation,
# handles some basic transformations of the data (like adding income levels or filtering
# out certain countries).
get_spending_version = partial(get_version, versions_dict=SPENDING)


def per_capita_spending(usd_constant_data: pd.DataFrame) -> pd.DataFrame:
    """Function to calculate per capita spending for countries and groups"""

    # Get per capita spending in constant USD
    pc_spending_countries = get_spending_version(version="usd_constant_pc")

    # Calculate per capita spending for income groups (total)
    pc_spending_income = per_capita_by_income(usd_constant_data)

    # Calculate per capita spending for Africa (total)
    pc_spending_africa = per_capita_africa(usd_constant_data)

    # Combine the datasets
    combined_pc = combine_income_countries(
        income=pc_spending_income,
        country=pc_spending_countries,
        africa=pc_spending_africa,
    ).assign(indicator="Per capita spending ($US)")

    return combined_pc


def total_usd_spending(usd_constant_data: pd.DataFrame) -> pd.DataFrame:
    """Function to calculate total (in constant usd) spending for countries and groups."""

    # Calculate total spending for income groups (total)
    total_spending_income = total_by_income(usd_constant_data).assign(
        value=lambda d: round(d.value / 1e9, 3)
    )

    # Get total spending per country (in billion)
    total_spending_countries = usd_constant_data.assign(
        value=lambda d: round(d.value / 1e9, 3)
    )

    # Get total spending for Africa (in bilion)
    total_spending_africa = total_africa(usd_constant_data).assign(
        value=lambda d: round(d.value / 1e9, 3)
    )

    # Combine the datasets
    combined_total = combine_income_countries(
        income=total_spending_income,
        country=total_spending_countries,
        africa=total_spending_africa,
    ).assign(indicator="Total spending ($US billion)")

    return combined_total


def _data_as_share(
    usd_constant_data: pd.DataFrame,
    share_callable: callable,
    share_callable_group: callable,
    group_by: list,
    indicator_name: str,
) -> pd.DataFrame:
    """Helper function to calculate spending as a share of something else."""

    # Calculate % of GDP by income
    share_income = share_callable_group(data=usd_constant_data, group_by=group_by)

    # identify Africa data
    afr_data = usd_constant_data.assign(
        country_name=lambda d: convert_id(
            d.iso_code,
            from_type="ISO3",
            to_type="continent",
        )
    ).query("country_name == 'Africa'")

    # Calculate % of gdp for Africa
    share_africa = share_callable_group(
        data=afr_data, group_by=["country_name", "year"]
    )

    # Calculate % of gdp for individual countries
    share_countries = share_callable(usd_constant_data)

    # Combine the datasets
    combined = combine_income_countries(
        income=share_income,
        country=share_countries,
        africa=share_africa,
    ).assign(indicator=indicator_name)

    return combined


def spending_share_of_gdp(usd_constant_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate spending as a share of gdp for countries and groups"""

    return _data_as_share(
        usd_constant_data=usd_constant_data,
        share_callable=value2gdp_share,
        share_callable_group=value2gdp_share_group,
        group_by=["income_group", "year"],
        indicator_name="Share of GDP (%)",
    )


def clean_chart(data: pd.DataFrame) -> pd.DataFrame:
    """Sort and clean data for chart"""

    indicator_order = {
        "Total spending ($US billion)": 1,
        "Per capita spending ($US)": 2,
        "Share of GDP (%)": 3,
        "Share of government spending (%)": 4,
    }

    # Combine both views of the data
    return (
        data.assign(order=lambda d: d.indicator.map(indicator_order))
        .sort_values(["order", "year"], ascending=(True, True))
        .drop(columns=["order"])
        .set_index(["year", "indicator"])
        .reset_index()
    )


def spending_share_of_government(usd_constant_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate spending as a share of government spending for countries and groups"""

    return _data_as_share(
        usd_constant_data=usd_constant_data,
        share_callable=value2gov_spending_share,
        share_callable_group=value2gov_spending_share_group,
        group_by=["income_group", "year"],
        indicator_name="Share of government spending (%)",
    )


def chart1_1_pipeline() -> None:
    """This function processes various data sources related to health spending
    and generates a combined dataset that includes information on per capita spending,
    total spending, share of GDP, and share of government spending.
    The resulting dataset is saved in CSV format to a specified file location."""

    # Get total spending in constant USD (pd.DataFrame)
    total_spending_usd_constant = get_spending_version(version="usd_constant")

    # ---- Per capita spending ---------------------->

    # Get data in per capita terms (for countries and groups) (pd.DataFrame)
    combined_pc = per_capita_spending(usd_constant_data=total_spending_usd_constant)

    # ---- Total spending ---------------------->

    # Get data in total usd terms (for countries and groups) (pd.DataFrame)
    combined_total = total_usd_spending(usd_constant_data=total_spending_usd_constant)

    # ---- Share of GDP ---------------------->
    combined_gdp = spending_share_of_gdp(usd_constant_data=total_spending_usd_constant)

    # ---- Share of Government spending ---------------------->

    combined_govx = spending_share_of_government(
        usd_constant_data=total_spending_usd_constant
    )

    # ---- Combine all -------------------------->

    # Combine the different views of the data
    df = pd.concat(
        [combined_pc, combined_total, combined_gdp, combined_govx],
        ignore_index=True,
    )

    # Sort and clean the data
    df = clean_chart(data=df)

    # ---- Export ------------------------------->

    # Save
    df.to_csv(PATHS.output / "section1_chart1.csv", index=False)


if __name__ == "__main__":
    chart1_1_pipeline()
