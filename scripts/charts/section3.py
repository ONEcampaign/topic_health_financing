from functools import partial

import pandas as pd
from bblocks import format_number

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    combine_income_countries,
    get_version,
    per_capita_by_income,
    total_by_income,
)
from scripts.config import PATHS
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
    value2gov_spending_share,
    value2pc,
)

spending_full = read_spending_data_versions(dataset_name="health_spending_by_source")
spending_oop = read_spending_data_versions(dataset_name="health_spending_oop")

get_agg_spending_version = partial(
    get_version, versions_dict=spending_full, additional_cols="source"
)
get_oop_spending_version = partial(get_version, versions_dict=spending_oop)


def _rebuild_private_spending(
    by_source: pd.DataFrame, oop: pd.DataFrame
) -> pd.DataFrame:
    return (
        by_source.query("source == 'domestic private'")
        .drop(columns=["source"])
        .merge(
            oop,
            on=["iso_code", "country_name", "year", "income_group"],
            suffixes=("_agg", "_oop"),
        )
        .assign(
            out_of_pocket_private=lambda d: d.value_oop,
            other_domestic_private=lambda d: d.value_agg - d.value_oop,
        )
        .drop(columns=["value_agg", "value_oop"])
        .melt(
            id_vars=["iso_code", "country_name", "year", "income_group"],
            value_vars=["out_of_pocket_private", "other_domestic_private"],
            var_name="source",
            value_name="value",
        )
    )


def get_spending(version="usd_constant") -> pd.DataFrame:
    # Get spending in constant USD
    spending_countries = get_agg_spending_version(version=version)

    # Get OOP spending in constant USD
    oop_spending_countries = get_oop_spending_version(version=version)

    # rebuild private spending
    data_private = _rebuild_private_spending(
        by_source=spending_countries, oop=oop_spending_countries
    )

    data = spending_countries.query("source != 'domestic private'")

    return pd.concat([data, data_private], ignore_index=True)


def get_spending_share_of_total() -> pd.DataFrame:
    data = get_spending()

    return data.assign(
        value=lambda d: d.groupby(
            ["iso_code", "country_name", "year"], group_keys=False
        )["value"].apply(lambda x: round(100 * x / x.sum(), 2))
    )


def get_government_spending_shares() -> pd.DataFrame:

    # ---- Share of gov spending ---------------------->

    data = get_spending()

    # Calculate as a share of government spending for income groups (total)
    return value2gov_spending_share(data)


def get_gdp_spending_shares() -> pd.DataFrame:
    # ---- Share of gdp  ---------------------->

    data = get_spending()

    # Calculate as a share of gdp for income groups (total)
    return value2gdp_share(data)


def get_per_capita_spending() -> pd.DataFrame:
    # ---- per capita spending ---------------------->

    data = get_spending()

    return value2pc(data)


def clean_chart_3_1(df: pd.DataFrame, full_df: pd.DataFrame) -> pd.DataFrame:
    indicator_order = {
        "Per capita spending ($US)": 2,
        "Total spending ($US billion)": 1,
        "Share of GDP (%)": 3,
    }

    country_order = list(full_df.country_name.unique())
    income_levels = [
        "High income",
        "Upper-middle income",
        "Lower-middle income",
        "Low income",
    ]

    column_order = ["year", "source", "indicator"] + income_levels + country_order

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

    return (
        df.melt(
            id_vars=["year", "source", "indicator"],
            value_name="value",
            var_name="Country",
        )
        .pivot(index=["year", "indicator", "Country"], columns="source", values="value")
        .reset_index()
        .sort_values(["year", "indicator", "Country"], ascending=(False, True, True))
    )


def create_tooltip_3_1(df: pd.DataFrame) -> pd.DataFrame:

    df = df.query("indicator == 'Total spending ($US million)'").copy(deep=True)

    for column in [
        "Domestic Government",
        "External Aid",
        "Out-of-pocket",
        "Other domestic private",
    ]:
        df[column] = format_number(df[column], as_units=True, decimals=1).replace(
            "nan", "-"
        )

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
    return df.filter(["Country", "year", "tooltip"], axis=1)


def chart_3_1():
    # Get total spending in constant USD
    total_spending = get_spending(version="usd_constant")

    # ---- Per capita spending ---------------------->

    # Get per capita spending in constant USD
    pc_spending_countries = get_spending(version="usd_constant_pc")

    # Calcualte per capita spending for income groups (total)
    pc_spending_income = per_capita_by_income(
        total_spending, additional_grouper="source"
    )

    # Combine the datasets
    combined_pc = combine_income_countries(
        pc_spending_income, pc_spending_countries, additional_grouper="source"
    ).assign(indicator="Per capita spending ($US)")

    # ---- Total spending ---------------------->

    # Calculate total spending for income groups (total)
    total_spending_income = total_by_income(
        total_spending, additional_grouper="source"
    ).assign(value=lambda d: round(d.value / 1e6, 3))

    # Get total spending per country (in billion)
    total_spending_countries = total_spending.assign(
        value=lambda d: round(d.value / 1e6, 3)
    )
    # Combine the datasets
    combined_total = combine_income_countries(
        total_spending_income, total_spending_countries, additional_grouper="source"
    ).assign(indicator="Total spending ($US million)")

    # ---- Share of total spending ---------------------->
    total_with_share = (
        combined_total.melt(id_vars=["year", "source", "indicator"], var_name="Country")
        .assign(
            value=lambda d: d.groupby(["Country", "year"], group_keys=False)[
                "value"
            ].apply(lambda x: round(100 * x / x.sum(), 2)),
            indicator="Share of total spending (%)",
        )
        .pivot(index=["year", "indicator", "source"], columns="Country", values="value")
        .reset_index()
    )

    # ---- Share of GDP ---------------------->
    # Calculate % of GDP by income
    gdp_share_income = value2gdp_share_group(
        total_spending, group_by=["income_group", "year", "source"]
    )

    gdp_share_countries = value2gdp_share(total_spending)

    # Combine the datasets
    combined_gdp = combine_income_countries(
        gdp_share_income, gdp_share_countries, additional_grouper="source"
    ).assign(indicator="Share of GDP (%)")

    # ---- Combine all -------------------------->

    # Combine both views of the data
    df = pd.concat(
        [combined_pc, combined_total, combined_gdp, total_with_share], ignore_index=True
    ).pipe(clean_chart_3_1, full_df=total_spending)

    # Reshape the data
    df = reshape_chart_3_1(df)

    # Get only share data
    share_df = df.query("indicator == 'Share of total spending (%)'")

    # Create tooltip
    tooltip_df = create_tooltip_3_1(df)

    # Merge tooltip
    share_df = share_df.merge(tooltip_df, on=["Country", "year"], how="left").drop(
        ["indicator"], axis=1
    )

    # Copy to clipboard
    share_df.to_csv(PATHS.output / "section3_chart3.csv", index=False)


if __name__ == "__main__":

    chart_3_1()
