import pandas as pd
from bblocks import add_income_level_column, convert_id

from scripts.tools import value2pc_group, value_total_group


def per_capita_by_income(
    spending: pd.DataFrame, additional_grouper: str | list = None
) -> pd.DataFrame:
    if additional_grouper is None:
        additional_grouper = []

    if isinstance(additional_grouper, str):
        additional_grouper = [additional_grouper]

    return (
        value2pc_group(
            data=spending,
            group_by=["year", "income_group"] + additional_grouper,
            value_column="value",
        )
        .sort_values(["year", "income_group"])
        .reset_index(drop=True)
    )


def per_capita_africa(
    spending: pd.DataFrame, additional_grouper: str | list = None
) -> pd.DataFrame:
    if additional_grouper is None:
        additional_grouper = []

    if isinstance(additional_grouper, str):
        additional_grouper = [additional_grouper]

    spending = spending.assign(
        country_name=lambda d: convert_id(
            d.iso_code, from_type="ISO3", to_type="continent"
        )
    ).query("country_name == 'Africa'")

    return (
        value2pc_group(
            data=spending,
            group_by=["year", "country_name"] + additional_grouper,
            value_column="value",
        )
        .sort_values(["year", "country_name"])
        .reset_index(drop=True)
    )


def total_by_income(
    spending: pd.DataFrame, additional_grouper: str | list = None
) -> pd.DataFrame:
    if additional_grouper is None:
        additional_grouper = []

    if isinstance(additional_grouper, str):
        additional_grouper = [additional_grouper]

    return (
        value_total_group(
            data=spending,
            group_by=["year", "income_group"] + additional_grouper,
            value_column="value",
        )
        .sort_values(["year", "income_group"])
        .reset_index(drop=True)
    )


def total_africa(
    spending: pd.DataFrame, additional_grouper: str | list = None
) -> pd.DataFrame:
    if additional_grouper is None:
        additional_grouper = []

    if isinstance(additional_grouper, str):
        additional_grouper = [additional_grouper]

    spending = spending.assign(
        country_name=lambda d: convert_id(
            d.iso_code, from_type="ISO3", to_type="continent"
        )
    ).query("country_name == 'Africa'")

    return (
        value_total_group(
            data=spending,
            group_by=["year", "country_name"] + additional_grouper,
            value_column="value",
        )
        .sort_values(["year", "country_name"])
        .reset_index(drop=True)
    )


def combine_income_countries(
    income: pd.DataFrame,
    country: pd.DataFrame,
    africa: pd.DataFrame | None,
    additional_grouper: str | list = None,
) -> pd.DataFrame:
    if additional_grouper is None:
        additional_grouper = []

    if isinstance(additional_grouper, str):
        additional_grouper = [additional_grouper]

    if africa is None:
        africa = pd.DataFrame()

    combined = pd.concat(
        [income, africa, country],
        ignore_index=True,
    )

    # series
    combined["series"] = combined.country_name.fillna(combined.income_group)

    # order
    order = combined["series"].drop_duplicates().tolist()

    # reshape
    return (
        combined.pivot(
            index=["year"] + additional_grouper, columns="series", values="value"
        )
        .filter(order, axis=1)
        .reset_index(drop=False)
    )


def get_version(
    versions_dict: dict, version: str, additional_cols: str | list = None
) -> pd.DataFrame:
    if additional_cols is None:
        additional_cols = []

    if isinstance(additional_cols, str):
        additional_cols = [additional_cols]

    columns = [
        "year",
        "iso_code",
        "income_group",
        "country_name",
        "value",
    ] + additional_cols

    year_filter = "year.dt.year <= 2020"
    other_filters = "iso_code != 'VEN' and iso_code != 'LBR' and iso_code != 'ZWE'"

    return (
        versions_dict[version]
        .query(year_filter)
        .query(other_filters)
        .pipe(
            add_income_level_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="income_group",
        )
        .filter(columns, axis=1)
    )
