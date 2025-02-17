"""Common helper function"""
from functools import lru_cache

import pandas as pd
import bblocks_data_importers as bbdata

from scripts.config import PATHS

def format_large_numbers(series: pd.Series, tn_dec: int = 2, bn_dec: int = 2, mn_dec: int = 2, other_dec: int = 2) -> pd.Series:
    """
    Format a pandas Series of numbers into human-readable strings representing millions, billions, or trillions.

    Args:
        series (pd.Series): A pandas Series containing numeric values.
        tn_dec (int): Number of decimal places for trillions.
        bn_dec (int): Number of decimal places for billions.
        mn_dec (int): Number of decimal places for millions.
        other_dec (int): Number of decimal places for numbers less than a million.

    Returns:
        pd.Series: A pandas Series with formatted strings representing large numbers (e.g., '6.00 million', '2.30 billion').

    Notes:
        - Numbers less than 1 million are formatted to `other_dec` decimal places.
        - Numbers in the millions, billions, and trillions are suffixed accordingly and formatted to their respective decimal places.
        - NaN values are preserved as None.
    """
    def format_number(num):
        if pd.isna(num):
            return None
        elif abs(num) >= 1_000_000_000_000:
            return f"{num / 1_000_000_000_000:.{tn_dec}f} trillion"
        elif abs(num) >= 1_000_000_000:
            return f"{num / 1_000_000_000:.{bn_dec}f} billion"
        elif abs(num) >= 1_000_000:
            return f"{num / 1_000_000:.{mn_dec}f} million"
        else:
            return f"{num:,.{other_dec}f}"

    return series.apply(format_number)


def custom_sort(df: pd.DataFrame, col: str, priority_list: list) -> pd.DataFrame:
    """
    Sorts a dataframe such that values in `priority_list` appear first in `col`,
    in the order they appear in `priority_list`, followed by all other values alphabetically.

    :param df: Input dataframe
    :param col: Column to sort by
    :param priority_list: List of values to prioritize in sorting
    :return: Sorted dataframe
    """
    df = df.copy(deep=True)
    df[col] = df[col].astype(str)  # Ensure the column is of string type for sorting

    df["order"] = df[col].apply(lambda x: priority_list.index(x) if x in priority_list else len(priority_list))
    df = df.sort_values(by=["order", col]).drop(columns=["order"]).reset_index(drop=True)

    return df


def keep_relevant_groups(df):
    """Keep only Africa, and income groups"""

    return (df
            .loc[lambda d: (d.entity_name.isin(['Africa', "Africa (Low and lower middle income)", 'Low income', 'Lower middle income', 'Upper middle income', 'High income'])) |
                           (d.iso3_code.notna())
    ]
        .reset_index(drop=True)
            )

@lru_cache
def get_ghed_data():
    """ """

    return pd.read_csv(PATHS.raw_data / "ghed.csv")


def _add_indicator(df, indicator_code, indicator_col):
    """ """

    ind_df = (get_ghed_data()
              .loc[lambda d: d.indicator_code == indicator_code, ['iso3_code', 'year', 'value']]
              .reset_index(drop=True)
              .rename(columns={'value': indicator_col})
              )

    return df.merge(ind_df, on=['iso3_code', 'year'], how='left')



def add_pop(df,*, pop_col = "population"):
    """Add population data"""

    return _add_indicator(df, "pop", pop_col)

def add_gge_usd_const_2022(df, *, gge_col ="gge_usd2022"):
    """Add general government expenditure data"""

    return _add_indicator(df, "gge_usd2022", gge_col)

def add_gdp_usd_curr(df,*, gdp_col = "gdp_usd_curr"):
    """Add gdp usd current data"""

    return _add_indicator(df, "gdp_usd", gdp_col)

def add_gdp_usd_const_2022(df,*, gdp_col = "gdp_usd_const_2022"):
    """Add gdp usd constant 2022 data"""

    return _add_indicator(df, "gdp_usd2022", gdp_col)

def add_che_usd2022(df,*, che_col = "che_usd2022"):
    """Add current health expenditure data"""

    return _add_indicator(df, "che_usd2022", che_col)

