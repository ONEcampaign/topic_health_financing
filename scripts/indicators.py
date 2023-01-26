from functools import partial

import pandas as pd

from scripts.read_data import get_indicator, CollectionCursor, COLLECTION_NAME

# Create a partial function that will always use the same collection cursor.
_get_indicator = partial(get_indicator, CollectionCursor(COLLECTION_NAME))


def get_current_health_exp() -> pd.DataFrame:
    """Get current health expenditure data"""
    return _get_indicator("ghed_current_health_expenditure")


def get_health_exp_by_source() -> pd.DataFrame:
    """Get health expenditure by source data"""
    return _get_indicator("ghed_current_health_expenditure_by_source")


def get_health_exp_by_function() -> pd.DataFrame:
    """Get health expenditure by function data"""
    return _get_indicator("ghed_current_health_expenditure_by_health_care_function")


def get_health_exp_by_disease() -> pd.DataFrame:
    """Get health expenditure by disease data"""
    return _get_indicator("ghed_current_health_expenditure_by_disease_and_conditions")


def get_health_exp_by_financing_scheme() -> pd.DataFrame:
    """Get health expenditure by financing scheme data"""
    return _get_indicator("ghed_current_health_expenditure_by_financing_schemes")


if __name__ == "__main__":
    exp = get_current_health_exp()

    exp_source = get_health_exp_by_source()

    exp_function = get_health_exp_by_function()

    exp_disease = get_health_exp_by_disease()

    exp_scheme = get_health_exp_by_financing_scheme()
