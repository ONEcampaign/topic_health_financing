"""Helper functions to conduct the analysis and transformations for the topic page"""

import pandas as pd
from bblocks import add_income_level_column, convert_id, set_bblocks_data_path

from scripts.config import PATHS
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
    value2pc_group,
    value_total_group,
)

set_bblocks_data_path(PATHS.raw_data)


def _validate_additional_grouper(grouper: list | str) -> list:
    """An optional additional grouper (beyond 'year' and 'income_group' can be provided
    This is used, for example, for another variable, like 'source'.

    Additionally, if provided as a string, it is converted to a list.
    """

    # If it is None, convert to empty list
    if grouper is None:
        return []

    # If a string is passed, convert to a list
    if isinstance(grouper, str):
        return [grouper]

    return grouper


def _filter_african_countries(data: pd.DataFrame) -> pd.DataFrame:
    """
    Helper function which assigns the name 'Africa' as 'country_name'
    to all African countries.
    """
    # Assign 'Africa' as country_name to African countries
    data = data.assign(
        country_name=lambda d: convert_id(
            d.iso_code, from_type="ISO3", to_type="continent"
        )
    )

    # Return keeping only Africa
    return data.query("country_name == 'Africa'").assign(
        country_name="Africa", income_group="Africa"
    )


def _filter_african_countries_excluding_hic(data: pd.DataFrame) -> pd.DataFrame:
    """
    Helper function which assigns the name 'Africa' as 'country_name'
    to all African countries.
    """
    # Assign 'Africa' as country_name to African countries
    data = data.assign(
        country_name=lambda d: convert_id(
            d.iso_code, from_type="ISO3", to_type="continent"
        )
    )

    # Return keeping only Africa
    return data.query(
        "country_name == 'Africa' and income_group != 'High income' "
        "and income_group != 'Upper middle income'"
    ).assign(
        country_name="Africa (Low and Lower middle income)",
        income_group="Africa (Low and Lower middle income)",
    )


def _callable_by(
    spending: pd.DataFrame,
    calculation_callable: callable,
    optional_group_callable: callable = None,
    main_group_by: list = None,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """
    Helper function to calculate per capita spending, based on a DataFrame of
    spending data. The resulting DataFrame is sorted by year and income group.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - calculation_callable: A function (like per capita or total) which is applied to the
        group.
    - optional_group_callable: A function which produces a 'group' based on the countries
        available in the data.
    - additional_grouper (optional): A string or list of strings containing additional
        columns to group the data by. Default is None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
        of values that must be non-null for a group to be included in the output DataFrame.
        Default is 0.95.

    Returns:
    - A pandas DataFrame containing the per capita spending
    """

    if main_group_by is None:
        main_group_by = ["year", "income_group"]

    # validate additional grouper
    additional_grouper = _validate_additional_grouper(grouper=additional_grouper)

    # if optional group callable isn't None, group the data
    if optional_group_callable is not None:
        spending = optional_group_callable(data=spending)

    # Calculate data
    data = calculation_callable(
        data=spending,
        group_by=main_group_by + additional_grouper,
        value_column="value",
        threshold=threshold,
    )

    # Sort and return
    return data.sort_values(main_group_by).reset_index(drop=True)


def per_capita_by_income(
    spending: pd.DataFrame,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """
    Calculates the per capita spending for each income group, based on a DataFrame of
    spending data. The resulting DataFrame is sorted by year and income group.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - additional_grouper (optional): A string or list of strings containing additional
        columns to group the data by. Default is None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
        of values that must be non-null for a group to be included in the output DataFrame.
        Default is 0.95.

    Returns:
    - A pandas DataFrame containing the per capita healthcare spending for each income
        group.
    """

    return _callable_by(
        spending=spending,
        calculation_callable=value2pc_group,
        additional_grouper=additional_grouper,
        threshold=threshold,
    )


def per_capita_africa(
    spending: pd.DataFrame,
    main_group_by: list = None,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """
    Calculates the per capita spending for 'Africa' as a whole, based on a DataFrame
    of spending data. The resulting DataFrame is sorted by year and country.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - additional_grouper (optional): A string or list of strings of additional columns
        to group the data by. Defaults to None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
        of non-null values for a group to be included in the output DataFrame. Defaults
        to 0.95.

    Returns:
    - A pandas DataFrame representing the per capita spending for Africa."""

    if main_group_by is None:
        main_group_by = ["year", "income_level"]

    return _callable_by(
        spending=spending,
        calculation_callable=value2pc_group,
        optional_group_callable=_filter_african_countries,
        additional_grouper=additional_grouper,
        threshold=threshold,
        main_group_by=main_group_by,
    )


def per_capita_africa_low_and_lower_middle(
    spending: pd.DataFrame,
    main_group_by: list = None,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """
    Calculates the per capita spending for 'Africa' as a whole, based on a DataFrame
    of spending data. The resulting DataFrame is sorted by year and country.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - additional_grouper (optional): A string or list of strings of additional columns
        to group the data by. Defaults to None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
        of non-null values for a group to be included in the output DataFrame. Defaults
        to 0.95.

    Returns:
    - A pandas DataFrame representing the per capita spending for Africa."""

    if main_group_by is None:
        main_group_by = ["year", "income_level"]

    return _callable_by(
        spending=spending,
        calculation_callable=value2pc_group,
        optional_group_callable=_filter_african_countries_excluding_hic,
        additional_grouper=additional_grouper,
        threshold=threshold,
        main_group_by=main_group_by,
    )


def total_by_income(
    spending: pd.DataFrame,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """
    Calculates the total spending for each income group, based on a DataFrame of spending data.
    The resulting DataFrame is sorted by year and income group.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - additional_grouper (optional): A string or list of strings of additional columns
    to group the data by. Defaults to None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
    of non-null values for a group to be included in the output DataFrame. Defaults
    to 0.95.

    Returns:
    - A pandas DataFrame with columns "year", "income_group", and "value", representing
    the total spending for each income group."""

    return _callable_by(
        spending=spending,
        calculation_callable=value_total_group,
        additional_grouper=additional_grouper,
        threshold=threshold,
    )


def total_africa(
    spending: pd.DataFrame,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
    main_group_by: list = None,
) -> pd.DataFrame:
    """
    Calculates the total spending for 'Africa' as a whole, based on a DataFrame
    of spending data. The resulting DataFrame is sorted by year and country.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - additional_grouper (optional): A string or list of strings of additional columns
        to group the data by. Defaults to None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
        of non-null values for a group to be included in the output DataFrame. Defaults
        to 0.95.

    Returns:
    - A pandas DataFrame representing the total spending for Africa.

    """
    if main_group_by is None:
        main_group_by = ["year", "country_name"]
    return _callable_by(
        spending=spending,
        calculation_callable=value_total_group,
        optional_group_callable=_filter_african_countries,
        additional_grouper=additional_grouper,
        threshold=threshold,
        main_group_by=main_group_by,
    )


def total_africa_excluding_high_income(
    spending: pd.DataFrame,
    additional_grouper: str | list = None,
    threshold: float = 0.95,
    main_group_by: list = None,
) -> pd.DataFrame:
    """
    Calculates the total spending for 'Africa' as a whole, based on a DataFrame
    of spending data. The resulting DataFrame is sorted by year and country.

    Parameters:
    - spending: A pandas DataFrame with (at least) columns "year", "iso_code", "value",
        and "income_group".
    - additional_grouper (optional): A string or list of strings of additional columns
        to group the data by. Defaults to None.
    - threshold (optional): A float between 0 and 1 representing the minimum proportion
        of non-null values for a group to be included in the output DataFrame. Defaults
        to 0.95.

    Returns:
    - A pandas DataFrame representing the total spending for Africa.

    """
    if main_group_by is None:
        main_group_by = ["year", "country_name"]
    return _callable_by(
        spending=spending,
        calculation_callable=value_total_group,
        optional_group_callable=_filter_african_countries_excluding_hic,
        additional_grouper=additional_grouper,
        threshold=threshold,
        main_group_by=main_group_by,
    )


def _clean_income_country_africa_combined_df(
    df: pd.DataFrame, additional_grouper: list
) -> pd.DataFrame:
    """Helper function to clean the combined dataframe and output in the right order"""

    # Create a columns 'series' which takes country names and adds income groups
    # if the country name is missing
    df["series"] = df.country_name.fillna(df.income_group)

    # Store the right 'order' for the data in a list
    order: list = df["series"].drop_duplicates().tolist()

    # reshape
    df = df.pivot(index=["year"] + additional_grouper, columns="series", values="value")

    # return a dataframe in the right order
    return df.filter(order, axis=1).reset_index(drop=False)


def combine_income_countries(
    income: pd.DataFrame,
    country: pd.DataFrame,
    africa: pd.DataFrame | None,
    additional_grouper: str | list = None,
    africa_excluding_hic: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Combines separate DataFrames for spending data for income groups, countries, and African
    countries (if available) into a single DataFrame.

    The resulting DataFrame is pivoted to have one column for each income group, country,
    and African country (if available), with columns for the year and any additional
    grouping columns specified in additional_grouper.

    Parameters:
    - income: A DataFrame representing spending for each income group.
    - country: A DataFrame representing spending for each country.
    - africa: An optional DataFrame representing spending for each African country.
    - additional_grouper: An optional string or list of strings of additional columns to group
        the data by. Defaults to None.

    Returns:
    - A pandas DataFrame with the combined data.
    """

    # validate additional grouper
    additional_grouper = _validate_additional_grouper(grouper=additional_grouper)

    # If africa data is not provided, create an empty dataframe
    if africa is None:
        africa = pd.DataFrame()

    # If africa excluding high income countries data is not provided, create an empty dataframe
    if africa_excluding_hic is None:
        africa_excluding_hic = pd.DataFrame()

    # Combine all data
    combined = pd.concat(
        [income, africa, africa_excluding_hic, country], ignore_index=True
    )

    # Reshape (pivot) and reorder data
    return _clean_income_country_africa_combined_df(
        df=combined, additional_grouper=additional_grouper
    )


def _exclude_countries_query() -> str:
    """Create a query string excluding certain countries"""
    countries = ["VEN", "LBR", "ZWE", "HRV"]
    return f"iso_code not in {countries}"


def get_version(
    versions_dict: dict,
    version: str,
    additional_cols: str | list = None,
    max_year: int = 2021,
    additional_filter: str | None = None,
) -> pd.DataFrame:
    """
    Retrieves a specific version of spending data from a dictionary of versions, and
    filters it by year, country code, and any additional columns specified in additional_cols.

    This function also applies transformations that are required for the entire pipeline,
    including filtering out certain countries, or harmonising income level information.

    The resulting DataFrame contains columns for the year, country code, income group,
    country name, spending value, and any additional columns specified in additional_cols.

    Parameters:
    - versions_dict: A dictionary of DataFrames containing spending data for different versions.
    - version: A string specifying the version of spending data to retrieve from the versions_dict.
    - additional_cols (optional): A string or list of strings containing additional columns
        to include in the output DataFrame. Defaults to None.

    Returns:
    - A pandas DataFrame containing with the version selected"""

    # Validate additional cols
    additional_cols = _validate_additional_grouper(additional_cols)

    # Define the columns that will be in the output
    columns: list = [
        "year",
        "iso_code",
        "income_group",
        "country_name",
        "value",
    ] + additional_cols

    # Create a year filter
    year_filter = f"year.dt.year <= {max_year}"

    # Create a countries (exclude) filter
    countries_filter = _exclude_countries_query()

    # get data
    data = versions_dict[version]

    # filter data
    data = data.query(year_filter).query(countries_filter)

    # Validate any additional filter passed
    if additional_filter is not None:
        data = data.query(additional_filter)

    # Add income levels
    data = data.pipe(
        add_income_level_column,
        id_column="iso_code",
        id_type="ISO3",
        target_column="income_group",
    )

    return data.filter(columns, axis=1).reset_index(drop=True)


def update_key_number(path: str, new_dict: dict) -> None:
    """
    Updates a JSON file containing key-value pairs, by adding or updating keys with values
    from a new dictionary.

    Parameters:
    - path: A string representing the file path of the JSON file to be updated.
    - new_dict: A dictionary containing the new key-value pairs to be added or updated in
        the JSON file.

    Returns:
    - None. The function updates the specified JSON file with the new key-value pairs.
    """
    import os
    import json

    # Check if the file exists, if not create
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump({}, f)

    with open(path, "r") as f:
        data = json.load(f)

    for k in new_dict.keys():
        data[k] = new_dict[k]

    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def reorder_by_income(df: pd.DataFrame, ascending=True) -> pd.DataFrame:
    """Reorder dataframe by income levels"""

    order = {
        "High income": 1,
        "Upper middle income": 2,
        "Lower middle income": 3,
        "Low income": 4,
    }

    return (
        df.assign(order=lambda d: d["income_group"].map(order))
        .sort_values(by=["order", "year", "value"], ascending=(True, False, False))
        .drop(columns="order")
        .reset_index(drop=True)
    )


def flag_africa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify african countries in a dataframe. Non-african countries get flagged
    as  'Other'.
    """

    # Add continents
    df = df.assign(
        Continent=lambda d: convert_id(
            d.country_name,
            from_type="regex",
            to_type="continent",
        )
    )

    # Identify African countries
    return df.assign(
        Continent=lambda d: d.Continent.apply(lambda x: x if x == "Africa" else "Other")
    )


def per_capita_spending(
    spending_version_callable: callable,
    usd_constant_data: pd.DataFrame,
    additional_grouper: None | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Function to calculate per capita spending for countries and groups"""

    # Get per capita spending in constant USD
    pc_spending_countries = spending_version_callable(version="usd_constant_pc")

    # Calculate per capita spending for income groups (total)
    pc_spending_income = per_capita_by_income(
        usd_constant_data, additional_grouper=additional_grouper, threshold=threshold
    )

    # Calculate per capita spending for Africa (total)
    pc_spending_africa = per_capita_africa(
        usd_constant_data,
        main_group_by=["year", "country_name"],
        additional_grouper=additional_grouper,
        threshold=threshold,
    )

    # Per capita Africa (low and lower middle income)
    pc_spending_africa_low_lower = per_capita_africa_low_and_lower_middle(
        usd_constant_data,
        main_group_by=["year", "country_name"],
        additional_grouper=additional_grouper,
        threshold=threshold,
    )

    # Combine the datasets
    combined_pc = combine_income_countries(
        income=pc_spending_income,
        country=pc_spending_countries,
        africa=pc_spending_africa,
        additional_grouper=additional_grouper,
        africa_excluding_hic=pc_spending_africa_low_lower,
    ).assign(indicator="Per capita spending ($US)")

    return combined_pc


def total_usd_spending(
    usd_constant_data: pd.DataFrame,
    factor: int | float = 1e9,
    units: str = "billion",
    additional_grouper: None | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Function to calculate total (in constant usd) spending for countries and groups."""

    # Calculate total spending for income groups (total)
    total_spending_income = (
        total_by_income(
            usd_constant_data,
            additional_grouper=additional_grouper,
            threshold=threshold,
        )
        .assign(value=lambda d: round(d.value / factor, 3))
        .dropna(subset=["income_group"])
    )

    # Get total spending per country
    total_spending_countries = usd_constant_data.assign(
        value=lambda d: round(d.value / factor, 3)
    )

    # Get total spending for Africa
    total_spending_africa = total_africa(
        spending=usd_constant_data,
        additional_grouper=additional_grouper,
        threshold=threshold,
        main_group_by=["year", "country_name"],
    ).assign(value=lambda d: round(d.value / factor, 3))

    # Get total spending for Africa excluding high income countries
    total_spending_africa_excluding_hic = total_africa_excluding_high_income(
        spending=usd_constant_data,
        additional_grouper=additional_grouper,
        threshold=threshold,
        main_group_by=["year", "country_name"],
    ).assign(value=lambda d: round(d.value / factor, 3))

    # Combine the datasets
    combined_total = combine_income_countries(
        income=total_spending_income,
        country=total_spending_countries,
        africa=total_spending_africa,
        additional_grouper=additional_grouper,
        africa_excluding_hic=total_spending_africa_excluding_hic,
    ).assign(indicator=f"Total spending ($US {units})")

    return combined_total


def _data_as_share(
    usd_constant_data: pd.DataFrame,
    share_callable: callable,
    share_callable_group: callable,
    group_by: list,
    indicator_name: str,
    additional_group_by: None | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Helper function to calculate spending as a share of something else."""
    if additional_group_by is None:
        additional_group_by = []

    # Calculate % of GDP by income
    share_income = share_callable_group(
        data=usd_constant_data,
        group_by=group_by + additional_group_by,
        threshold=threshold,
    )

    # identify Africa data
    afr_data = usd_constant_data.assign(
        country_name=lambda d: convert_id(
            d.iso_code,
            from_type="ISO3",
            to_type="continent",
        )
    ).query("country_name == 'Africa'")

    # identify Africa (excluding high income) data
    afr_data_exc_hic = (
        usd_constant_data.assign(
            country_name=lambda d: convert_id(
                d.iso_code,
                from_type="ISO3",
                to_type="continent",
            )
        )
        .query(
            "country_name == 'Africa' and income_group != 'High income'"
            " and income_group != 'Upper middle income'"
        )
        .assign(country_name="Africa (Low and Lower middle income)")
    )

    # Calculate % of gdp for Africa
    share_africa = share_callable_group(
        data=afr_data,
        group_by=["country_name", "year"] + additional_group_by,
        threshold=threshold,
    )

    # Calculate % of gdp for Africa (excluding high income)
    share_africa_exc_hic = share_callable_group(
        data=afr_data_exc_hic,
        group_by=["country_name", "year"] + additional_group_by,
        threshold=threshold,
    )

    # Calculate % of gdp for individual countries
    share_countries = share_callable(usd_constant_data)

    # Combine the datasets
    combined = combine_income_countries(
        income=share_income,
        country=share_countries,
        africa=share_africa,
        additional_grouper=additional_group_by,
        africa_excluding_hic=share_africa_exc_hic,
    ).assign(indicator=indicator_name)

    return combined


def spending_share_of_gdp(
    usd_constant_data: pd.DataFrame,
    group_by: str | list,
    additional_group_by: None | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Calculate spending as a share of gdp for countries and groups"""

    return _data_as_share(
        usd_constant_data=usd_constant_data,
        share_callable=value2gdp_share,
        share_callable_group=value2gdp_share_group,
        group_by=group_by,
        additional_group_by=additional_group_by,
        indicator_name="Share of GDP (%)",
        threshold=threshold,
    )
