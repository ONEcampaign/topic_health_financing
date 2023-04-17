from functools import partial

import pandas as pd

from scripts.logger import logger
from scripts.read_data import get_indicator, CollectionCursor, COLLECTION_NAME

# Create a partial function that will always use the same collection cursor.
_get_indicator = partial(get_indicator, CollectionCursor(COLLECTION_NAME))


def get_current_health_exp(*, additional_filter: dict = None) -> pd.DataFrame:
    """Get current health expenditure data"""
    indicator = "ghed_current_health_expenditure"
    logger.info(f"Getting {indicator} data")
    return _get_indicator(indicator, additional_filter=additional_filter)


def get_health_exp_by_source(*, additional_filter: dict = None) -> pd.DataFrame:
    """Get health expenditure by source data"""
    indicator = "ghed_current_health_expenditure_by_source"
    logger.info(f"Getting {indicator} data")
    return _get_indicator(indicator, additional_filter=additional_filter)


def get_health_exp_by_function(*, additional_filter: dict = None) -> pd.DataFrame:
    """Get health expenditure by function data"""
    indicator = "ghed_current_health_expenditure_by_health_care_function"
    logger.info(f"Getting {indicator} data")
    return _get_indicator(indicator, additional_filter=additional_filter)


def get_health_exp_by_disease(*, additional_filter: dict = None) -> pd.DataFrame:
    """Get health expenditure by disease data"""
    indicator = "ghed_current_health_expenditure_by_disease_and_conditions"
    logger.info(f"Getting {indicator} data")
    return _get_indicator(indicator, additional_filter=additional_filter)


def get_health_exp_by_financing_scheme(
    *, additional_filter: dict = None
) -> pd.DataFrame:
    """Get health expenditure by financing scheme data"""
    indicator = "ghed_current_health_expenditure_by_financing_schemes"
    logger.info(f"Getting {indicator} data")
    return _get_indicator(indicator, additional_filter=additional_filter)


def current_lcu_data(
    spending_func: callable, *, additional_filter: dict = None
) -> callable:
    """Get current lcu data for a given dataset"""
    if additional_filter is None:
        additional_filter = {}
    full_filter = additional_filter | {"units": "national currency unit, millions"}
    return spending_func(additional_filter=full_filter)
