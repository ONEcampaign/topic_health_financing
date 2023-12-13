"""
Analyse health spending data across countries, focusing on spending shares of total.
Use to process, filter and aggregate the data.
"""
from functools import partial

import pandas as pd
from bblocks import format_number

from scripts.analysis.read_data_versions import read_spending_data_versions
from scripts.charts.common import (
    get_version,
    per_capita_spending,
    spending_share_of_gdp,
    total_usd_spending,
)
from scripts.charts.section1 import spending_share_of_government
from scripts.config import PATHS

# Load a dictionary with dataframes for the different versions of "health_spending_by_source" data
# These include: 'lcu', 'gdp_share','usd_current', 'usd_constant', 'usd_constant_pc'
spending_full = read_spending_data_versions(dataset_name="health_spending_by_source")

# As above, but for out-of-pocket spending
spending_oop = read_spending_data_versions(dataset_name="health_spending_oop")

# Create a function to get a specific version. It returns a dataframe. This function
# will make sure to keep the "source" dimension contianed in the data
get_agg_spending_version = partial(
    get_version, versions_dict=spending_full, additional_cols="source"
)

# As above, but for out-of-pocket spending
get_oop_spending_version = partial(get_version, versions_dict=spending_oop)


def _rebuild_private_spending(
    by_source: pd.DataFrame, oop: pd.DataFrame
) -> pd.DataFrame:
    """
    Rebuilds a DataFrame containing spending data for domestic private sources.
    This is done by merging a DataFrame containing spending data by source with
    a DataFrame containing out-of-pocket spending data.

    Parameters:
    - by_source: A DataFrame containing healthcare spending data by source.
    - oop: A DataFrame containing out-of-pocket healthcare spending data

    Returns:
    - A DataFrame which splits private into out-of-pocket and other domestic private.
    """

    # Keep only domestic private and drop source column
    by_source = by_source.query("source == 'domestic private'").drop(columns=["source"])

    # Merge the two dataframes
    data = by_source.merge(
        oop,
        on=["iso_code", "country_name", "year", "income_group"],
        suffixes=("_agg", "_oop"),
    )

    # create the two totals
    data = data.assign(
        out_of_pocket_private=lambda d: d.value_oop,
        other_domestic_private=lambda d: d.value_agg - d.value_oop,
    )

    # Clean and return
    return data.drop(columns=["value_agg", "value_oop"]).melt(
        id_vars=["iso_code", "country_name", "year", "income_group"],
        value_vars=["out_of_pocket_private", "other_domestic_private"],
        var_name="source",
        value_name="value",
    )


def _incomes_first(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts a DataFrame so that countries are ordered by income level first, with lower
     income countries appearing first, and higher income countries appearing last.

    Parameters:
        - df: A DataFrame to resort

    Returns:
        - A DataFrame, where the countries are ordered by income level first,
        with lower income countries appearing first, and higher income countries
        appearing last.

    """

    # Define the order of income levels
    income_levels = {
        "Low income": 1,
        "Lower middle income": 2,
        "Upper middle income": 3,
        "High income": 4,
    }

    # filter the income level data and sort it
    top = (
        df.loc[lambda d: d.Country.isin(income_levels)]
        .assign(order=lambda d: d.Country.map(income_levels))
        .sort_values(["order", "year"], ascending=(True, True))
        .drop(columns="order")
    )

    # filter the dataframe to keep only non-income level data
    bottom = df.loc[lambda d: ~d.Country.isin(income_levels)]

    # combine the two dataframes
    return pd.concat([top, bottom], ignore_index=True)


def get_spending(version="usd_constant") -> pd.DataFrame:
    """
    Returns a DataFrame containing spending data by country and year, with
    the private spending data split into out-of-pocket and other domestic private
    categories.

    Parameters:
        - version (optional): A string representing the version of the spending data
        to retrieve. Default is 'usd_constant'.

    """

    # Get spending in constant USD
    spending_countries = (
        get_agg_spending_version(version=version)
        .query("source != 'domestic private'")
        .assign(
            source=lambda d: d.source.replace(
                {"domestic private excluding out-of-pocket": "other_domestic_private"},
                regex=False,
            )
        )
    )
    oop_spending_countries = get_oop_spending_version(version=version).assign(
        source="out_of_pocket_private"
    )
    # Combine the two dataframes
    return pd.concat([spending_countries, oop_spending_countries], ignore_index=True)


def clean_chart_3_1(df: pd.DataFrame, full_df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data for use in Flourish"""

    # Set the right indicator ordr
    indicator_order = {
        "Total spending ($US billion)": 1,
        "Per capita spending ($US)": 2,
        "Share of GDP (%)": 3,
    }

    # set the right country order
    country_order = list(full_df.country_name.unique()) + ["Africa"]

    # Set the income levels
    income_levels = [
        "High income",
        "Upper middle income",
        "Lower middle income",
        "Low income",
    ]

    # Specify the new column order
    column_order = ["year", "source", "indicator"] + income_levels + country_order

    # Set the source names
    source_names = {
        "domestic general government": "Domestic Government",
        "external": "External Aid",
        "other_domestic_private": "Other domestic private",
        "out_of_pocket_private": "Out-of-pocket",
    }

    return (
        df.assign(
            order=lambda d: d.indicator.map(indicator_order),
            year=lambda d: d.year.dt.year,
            source=lambda d: d.source.map(source_names),
        )
        .sort_values(["order", "year"], ascending=(True, True))
        .filter(column_order, axis=1)
    )


def reshape_chart_3_1(df: pd.DataFrame) -> pd.DataFrame:
    """Change the structure of the data. This first melts by year, source, indicator,
    and then pivots to that the source becomes columns"""

    # Reshape to vertical
    data = df.melt(
        id_vars=["year", "source", "indicator"],
        value_name="value",
        var_name="Country",
    )

    # Pivot to horizontal so sources become columns
    data = data.pivot(
        index=["year", "indicator", "Country"], columns="source", values="value"
    ).reset_index()

    # sort and return
    return data.sort_values(
        ["year", "indicator", "Country"], ascending=(False, True, True)
    )


def calculate_share_of_total_spending(df: pd.DataFrame) -> pd.DataFrame:
    """Share of source out of total spending"""
    return (
        df.melt(id_vars=["year", "source", "indicator"], var_name="Country")
        .assign(
            value=lambda d: d.groupby(["Country", "year"], group_keys=False)[
                "value"
            ].apply(lambda x: round(100 * x / x.sum(), 2)),
            indicator="Share of total spending (%)",
        )
        .pivot(index=["year", "indicator", "source"], columns="Country", values="value")
        .reset_index()
    )


def create_tooltip_3_1(df: pd.DataFrame) -> pd.DataFrame:
    """Create the text for the tooltips to be used on the chart"""

    # Get only total spending (as a DataFrame copy)
    df = df.query("indicator == 'Total spending ($US million)'").copy(deep=True)

    # For every column, replace any NaNs with a dash and format the numbers
    for column in [
        "Domestic Government",
        "External Aid",
        "Out-of-pocket",
        "Other domestic private",
    ]:
        df[column] = format_number(df[column], as_units=True, decimals=1).replace(
            "nan", "-"
        )

    # Create the tooltip text
    df["tooltip"] = (
        "<b>Domestic Government:</b> US$ "
        + df["Domestic Government"]
        + "m.<br>"
        + "<b>External Aid:</b> US$ "
        + df["External Aid"]
        + "m.<br>"
        + "<b>Out-of-pocket:</b> US$ "
        + df["Out-of-pocket"]
        + "m.<br>"
        + "<b>Other domestic private:</b> US$ "
        + df["Other domestic private"]
        + "m."
    )
    # Return a dataframe with country, year and tooltip
    return df.filter(["Country", "year", "tooltip"], axis=1)


def create_tooltip_1_2(df: pd.DataFrame) -> pd.DataFrame:
    """Create the text for the tooltips to be used on the chart"""

    # Get only total spending (as a DataFrame copy)
    df = (
        df.query(
            "source == 'domestic general government' and "
            "indicator != 'Share of government spending (%)'"
        )
        .copy(deep=True)
        .melt(id_vars=["year", "source", "indicator"], var_name="Country")
        .pivot(index=["year", "source", "entity"], columns="indicator", values="value")
        .reset_index()
    )

    # For every column, replace any NaNs with a dash and format the numbers
    for column in [
        "Per capita spending ($US)",
        "Share of GDP (%)",
    ]:
        df[column] = format_number(df[column], as_units=True, decimals=1).replace(
            "nan", "-"
        )

    # Create the tooltip text
    df["tooltip"] = (
        "<b>Per person:</b> US$ "
        + df["Per capita spending ($US)"]
        + "<br>"
        + "<b>As a share of GDP:</b> "
        + df["Share of GDP (%)"]
        + "%<br>"
    )
    # Return a dataframe with country, year and tooltip
    return df.filter(["Country", "year", "tooltip"], axis=1)


def chart_3_1():
    """Pipeline to create the chart"""

    # Get total spending in constant USD
    total_spending = get_spending(version="usd_constant")

    # ---- Per capita spending ---------------------->
    combined_pc = per_capita_spending(
        spending_version_callable=get_spending,
        usd_constant_data=total_spending,
        additional_grouper=["source"],
        threshold=0.5,
    )

    # ---- Total spending ---------------------->
    combined_total = total_usd_spending(
        usd_constant_data=total_spending,
        additional_grouper=["source"],
        threshold=0.5,
        factor=1e6,
        units="million",
    )

    # ---- Share of total spending ---------------------->
    total_with_share = calculate_share_of_total_spending(df=combined_total)

    # ---- Share of GDP ---------------------->
    combined_gdp = spending_share_of_gdp(
        usd_constant_data=total_spending,
        group_by=["income_group", "year"],
        additional_group_by=["source"],
        threshold=0.5,
    )

    # ---- Combine all -------------------------->
    # Combine all views of the data
    df = pd.concat(
        [combined_pc, combined_total, combined_gdp, total_with_share], ignore_index=True
    )

    # Export intermediate outputs
    df_long = df.melt(
        id_vars=["year", "source"], var_name="country", value_name="value"
    )
    for source in df_long.source.unique():
        df_long.query(f"source == '{source}'").to_csv(
            PATHS.output / f"{source}.csv", index=False
        )

    # Clean the data
    df = df.pipe(clean_chart_3_1, full_df=total_spending)

    # Reshape the data
    df = df.pipe(reshape_chart_3_1)

    # Get only share data
    share_df = df.query("indicator == 'Share of total spending (%)'")

    # Create tooltip
    tooltip_df = create_tooltip_3_1(df)

    # Merge tooltip
    share_df = share_df.merge(tooltip_df, on=["Country", "year"], how="left").drop(
        ["indicator"], axis=1
    )

    # incomes first
    share_df = _incomes_first(share_df)

    # Copy to clipboard
    share_df.to_csv(PATHS.output / "section3_chart1.csv", index=False)


def chart1_2_pipeline() -> None:
    # Get total spending in constant USD
    total_spending = get_spending(version="usd_constant")

    # # ---- Per capita spending ---------------------->
    # combined_pc = per_capita_spending(
    #     spending_version_callable=get_spending,
    #     usd_constant_data=total_spending,
    #     additional_grouper=["source"],
    #     threshold=0.5,
    # )
    #
    # # ---- Share of GDP ---------------------->
    # combined_gdp = spending_share_of_gdp(
    #     usd_constant_data=total_spending,
    #     group_by=["income_group", "year"],
    #     additional_group_by=["source"],
    #     threshold=0.5,
    # )
    # # ---- Share of Government spending ---------------------->

    combined_govx = spending_share_of_government(
        usd_constant_data=total_spending, additional_grouper=["source"]
    )

    data = pd.concat([combined_govx], ignore_index=True)

    data = data.query("source == 'domestic general government'").drop(
        ["source", "indicator"], axis=1
    )

    data.to_csv(PATHS.output / "section1_chart2.csv", index=False)


if __name__ == "__main__":
    chart_3_1()
    chart1_2_pipeline()
