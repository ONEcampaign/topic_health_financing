import pandas as pd

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
    value2pc_group,
    value_total_group,
)


SPENDING = read_spending_data_versions(dataset_name="health_spending")


def get_spending_version(version: str) -> pd.DataFrame:
    columns = ["year", "iso_code", "income_group", "country_name", "value"]
    year_filter = "year.dt.year <= 2020"
    other_filters = "iso_code != 'VEN'"
    income_names = {
        "High": "High income",
        "Low": "Low income",
        "Lower-middle": "Lower-middle income",
        "Upper-middle": "Upper-middle income",
    }

    return (
        SPENDING[version]
        .query(year_filter)
        .query(other_filters)
        .replace({"income_group": income_names})
        .filter(columns, axis=1)
    )


def per_capita_by_income(spending: pd.DataFrame) -> pd.DataFrame:
    return (
        value2pc_group(
            data=spending,
            group_by=["year", "income_group"],
            value_column="value",
        )
        .sort_values(["year", "income_group"])
        .reset_index(drop=True)
    )


def total_by_income(spending: pd.DataFrame) -> pd.DataFrame:
    return (
        value_total_group(
            data=spending,
            group_by=["year", "income_group"],
            value_column="value",
        )
        .sort_values(["year", "income_group"])
        .reset_index(drop=True)
    )


def combine_income_countries(
    income: pd.DataFrame, country: pd.DataFrame
) -> pd.DataFrame:

    combined = pd.concat([income, country], ignore_index=True)

    # series
    combined["series"] = combined.country_name.fillna(combined.income_group)

    # order
    order = combined["series"].drop_duplicates().tolist()

    # reshape
    return (
        combined.pivot(index=["year"], columns="series", values="value")
        .filter(order, axis=1)
        .reset_index(drop=False)
    )


def pipeline() -> None:

    # Get total spending in constant USD
    total_spending = get_spending_version(version="usd_constant")

    # ---- Per capita spending ---------------------->

    # Get per capita spending in constant USD
    pc_spending_countries = get_spending_version(version="usd_constant_pc")

    # Calcualte per capita spending for income groups (total)
    pc_spending_income = per_capita_by_income(total_spending)

    # Combine the datasets
    combined_pc = combine_income_countries(
        pc_spending_income, pc_spending_countries
    ).assign(indicator="Per capita spending ($US)")

    # ---- Total spending ---------------------->

    # Calculate total spending for income groups (total)
    total_spending_income = total_by_income(total_spending).assign(
        value=lambda d: round(d.value / 1e9, 3)
    )

    # Get total spending per country (in billion)
    total_spending_countries = total_spending.assign(
        value=lambda d: round(d.value / 1e9, 3)
    )
    # Combine the datasets
    combined_total = combine_income_countries(
        total_spending_income, total_spending_countries
    ).assign(indicator="Total spending ($US billion)")

    # ---- Share of GDP ---------------------->
    # Calculate % of GDP by income
    gdp_share_income = value2gdp_share_group(
        total_spending, group_by=["income_group", "year"]
    )

    gdp_share_countries = value2gdp_share(total_spending)

    # Combine the datasets
    combined_gdp = combine_income_countries(
        gdp_share_income, gdp_share_countries
    ).assign(indicator="Share of GDP (%)")

    # ---- Combine all -------------------------->

    # Combine both views of the data
    df = pd.concat(
        [
            # combined_pc,
            # combined_total,
            combined_gdp,
        ],
        ignore_index=True,
    ).sort_values(["indicator", "year"])

    # Copy to clipboard
    df.to_clipboard(index=False)


if __name__ == "__main__":
    pipeline()
