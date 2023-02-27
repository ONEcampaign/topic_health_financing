import pandas as pd

from scripts.tools import value2pc_group, value_total_group


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


def get_version(versions_dict: dict, version: str) -> pd.DataFrame:
    columns = ["year", "iso_code", "income_group", "country_name", "value"]
    year_filter = "year.dt.year <= 2020"
    other_filters = "iso_code != 'VEN' and iso_code != 'LBR'"
    income_names = {
        "High": "High income",
        "Low": "Low income",
        "Lower-middle": "Lower-middle income",
        "Upper-middle": "Upper-middle income",
    }

    return (
        versions_dict[version]
        .query(year_filter)
        .query(other_filters)
        .replace({"income_group": income_names})
        .filter(columns, axis=1)
    )
