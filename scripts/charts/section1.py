"""
Analyze health spending data across countries, focusing on per capita spending,
total spending, share of GDP, and share of government spending.

Use to process, filter and aggregate the data

"""
from functools import partial

import pandas as pd

from scripts.analysis.read_data_versions import read_spending_data_versions
from scripts.charts.common import (
    _data_as_share,
    get_version,
    per_capita_spending,
    spending_share_of_gdp,
    total_usd_spending,
)
from scripts.config import PATHS
from scripts.tools import (
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
get_spending_version: callable = partial(get_version, versions_dict=SPENDING)


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
        .reset_index(drop=False)
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
    combined_pc = per_capita_spending(
        spending_version_callable=get_spending_version,
        usd_constant_data=total_spending_usd_constant,
    )

    # ---- Total spending ---------------------->

    # Get data in total usd terms (for countries and groups) (pd.DataFrame)
    combined_total = total_usd_spending(usd_constant_data=total_spending_usd_constant)

    # ---- Share of GDP ---------------------->
    combined_gdp = spending_share_of_gdp(
        usd_constant_data=total_spending_usd_constant, group_by=["income_group", "year"]
    )

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
