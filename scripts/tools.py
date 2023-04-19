from functools import partial

import pandas as pd
from bblocks import WorldEconomicOutlook, set_bblocks_data_path
from pydeflate import deflate, exchange, set_pydeflate_path

from scripts import config
from scripts.logger import logger
from scripts.population_data.un_population import un_population_data

set_bblocks_data_path(config.PATHS.raw_data)
set_pydeflate_path(config.PATHS.pydeflate_data)


def get_weo_indicator(indicator: str, keep_metadata: bool = False) -> pd.DataFrame:
    """Get a single indicator from the World Economic Outlook data."""
    # Create object
    weo = WorldEconomicOutlook()

    # Load indicator
    weo.load_data(indicator)

    # Log
    logger.debug(f"Loaded indicator {indicator} from {config.PATHS.raw_data}")

    # Return data
    return weo.get_data(keep_metadata=keep_metadata)


def bn2units(data: pd.DataFrame, value_column: str = "value") -> pd.DataFrame:
    """Convert billions to units"""
    return data.assign(value=lambda d: d[value_column] * 1e9)


def mn2units(data: pd.DataFrame, value_column: str = "value") -> pd.DataFrame:
    """Convert millions to units"""
    return data.assign(value=lambda d: d[value_column] * 1e6)


def year2date(data: pd.DataFrame, year_column: str = "year") -> pd.DataFrame:
    """Convert year to date"""
    return data.assign(year=lambda d: pd.to_datetime(d[year_column], format="%Y"))


lcu2usd_current = partial(
    exchange,
    source_currency="LCU",
    target_currency="USA",
    rates_source="world_bank",
    target_column="value",
    date_column="year",
)

lcu2usd_constant = partial(
    deflate,
    base_year=config.CONSTANT_YEAR,
    deflator_source="imf",
    deflator_method="gdp",
    exchange_source="imf",
    exchange_method="implied",
    source_currency="LCU",
    target_currency="USA",
    date_column="year",
)

usd2usd_constant = partial(
    deflate,
    base_year=config.CONSTANT_YEAR,
    deflator_source="imf",
    deflator_method="gdp",
    exchange_source="imf",
    exchange_method="implied",
    source_currency="USA",
    target_currency="USA",
    date_column="year",
)


def lcu2gdp(data: pd.DataFrame, value_column: str = "value") -> pd.DataFrame:
    """Convert numbers in LCU to share of GDP"""

    if value_column not in data.columns:
        data = data.rename(columns={value_column: "value"})

    # Store columns
    cols = data.columns

    # Load GDP data
    gdp = get_weo_indicator("NGDP").pipe(bn2units)

    # Merge
    data = pd.merge(
        data, gdp, on=["iso_code", "year"], how="left", suffixes=("", "_gdp")
    )

    # Convert to units
    data["value"] = round(data.value * 100 / data.value_gdp, 6)

    # state units
    data["units"] = "share of GDP"

    # Drop columns
    return data.filter(cols, axis=1).rename(columns={"value": value_column})


def _value2(
    data: pd.DataFrame,
    other_data: pd.DataFrame,
    units_str: str,
    factor: int,
    value_column: str = "value",
) -> pd.DataFrame:
    """Helper function"""

    if value_column not in data.columns:
        data = data.rename(columns={value_column: "value"})

    # Store columns
    cols = data.columns

    # Merge
    data = pd.merge(
        data, other_data, on=["iso_code", "year"], how="left", suffixes=("", "_other")
    ).dropna(subset=["value"], how="any")

    data["value"] = round(factor * data.value / data.value_other, 3)

    # state units
    try:
        data["units"] = data["units"] + f" {units_str}"
    except KeyError:
        data["units"] = units_str

    # Drop columns
    return data.filter(cols, axis=1).rename(columns={"value": value_column})


def _value2_group(
    data: pd.DataFrame,
    other_data: pd.DataFrame,
    group_by: str | list = None,
    value_column: str = "value",
    units_str: str = "",
    factor: int = 1,
    threshold: float = 0.95,
):
    if group_by is None:
        group_by = ["iso_code", "year"]
    elif isinstance(group_by, str):
        group_by = [group_by]

    if value_column not in data.columns:
        data = data.rename(columns={value_column: "value"})

    # Store columns
    cols = data.columns

    # Merge
    data = pd.merge(
        data,
        other_data,
        on=["iso_code", "year"],
        how="left",
        suffixes=("", "_other"),
    )

    # fill gaps
    data = fill_gaps_by_group(data, value_columns=["value", "value_other"])

    # Get total counts for each group
    new_group = [c for c in group_by if c != "year"]

    # Add total counts
    data = add_total_counts_by_group(df=data, group=new_group)

    # Filter by threshold
    data = filter_by_threshold(
        df=data,
        threshold=threshold,
        value_columns=["value", "value_other"],
        group=group_by,
    )

    data["value"] = round(factor * data.value / data.value_other, 3)

    # state units
    try:
        data["units"] = data["units"] + f" {units_str}"
    except KeyError:
        data["units"] = units_str

    # Drop columns
    return data.filter(cols, axis=1).rename(columns={"value": value_column})


def value2pc(data: pd.DataFrame, value_column: str = "value") -> pd.DataFrame:
    """Convert units to per capita figures"""

    # Load expenditure data
    population = un_population_data().pipe(year2date)

    return _value2(
        data=data,
        other_data=population,
        units_str="per capita",
        factor=1,
        value_column=value_column,
    )


def value2gov_spending_share(
    data: pd.DataFrame, value_column: str = "value", constant: bool = True
) -> pd.DataFrame:
    """Convert units to per capita figures"""

    # Load expenditure data
    ggx = get_weo_indicator("GGX").pipe(bn2units).pipe(year2date)

    if constant:
        ggx = lcu2usd_constant(ggx)

    return _value2(
        data=data,
        other_data=ggx,
        units_str="% of government spending",
        factor=100,
        value_column=value_column,
    )


def value2gdp_share(
    data: pd.DataFrame,
    value_column: str = "value",
    constant: bool = True,
) -> pd.DataFrame:
    """Convert units to per capita figures"""
    # Load expenditure data
    gdp = get_weo_indicator("NGDPD").pipe(bn2units).pipe(year2date)

    if constant:
        gdp = usd2usd_constant(gdp)

    return _value2(
        data=data,
        other_data=gdp,
        units_str="% of GDP",
        factor=100,
        value_column=value_column,
    )


def value2pc_group(
    data: pd.DataFrame,
    value_column: str = "value",
    group_by: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Convert units to per capita figures"""

    # Load expenditure data
    population = un_population_data().pipe(year2date)

    return _value2_group(
        data=data,
        other_data=population,
        units_str="per capita",
        factor=1,
        value_column=value_column,
        group_by=group_by,
        threshold=threshold,
    )


def value2gdp_share_group(
    data: pd.DataFrame,
    value_column: str = "value",
    group_by: str | list = None,
    constant: bool = True,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Convert units to per capita figures"""

    # Load expenditure data
    gdp = get_weo_indicator("NGDPD").pipe(bn2units).pipe(year2date)

    if constant:
        gdp = usd2usd_constant(gdp)

    return _value2_group(
        data=data,
        other_data=gdp,
        units_str="% of GDP",
        factor=100,
        value_column=value_column,
        group_by=group_by,
        threshold=threshold,
    )


def value2gov_spending_share_group(
    data: pd.DataFrame,
    value_column: str = "value",
    group_by: str | list = None,
    constant: bool = True,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Convert units to per capita figures"""

    # Load expenditure data
    ggx = get_weo_indicator("GGX").pipe(bn2units).pipe(year2date)

    if constant:
        ggx = lcu2usd_constant(ggx)

    return _value2_group(
        data=data,
        other_data=ggx,
        units_str="% of government spending",
        factor=100,
        value_column=value_column,
        group_by=group_by,
        threshold=threshold,
    )


def fill_gaps_by_group(
    df: pd.DataFrame, value_columns: str | list = "value"
) -> pd.DataFrame:
    if isinstance(value_columns, str):
        value_columns = [value_columns]

    return (
        df.sort_values(["year", "iso_code"])
        .groupby(
            [c for c in df.columns if c not in [*value_columns, "year"]],
            observed=True,
            dropna=False,
            group_keys=False,
        )
        .apply(lambda group: group.ffill(limit=2))
        .dropna(subset=["value"])  # drop rows with that still have NaNs
    )


def add_total_counts_by_group(df: pd.DataFrame, group: list) -> pd.DataFrame:
    counts = (
        df.groupby(group, observed=True, dropna=False, as_index=False)
        .agg({"iso_code": "nunique"})
        .rename(columns={"iso_code": "total_counts"})
    )

    return df.merge(counts, on=group, how="left")


def filter_by_threshold(
    df: pd.DataFrame,
    threshold: float,
    group: list,
    value_columns: str | list = "value",
    counts_column: str = "total_counts",
) -> pd.DataFrame:
    if isinstance(value_columns, str):
        value_columns = [value_columns]

    value_columns = {k: "sum" for k in value_columns}

    return (
        df.groupby(group, observed=True, dropna=False, as_index=False)
        .agg(value_columns | {"iso_code": "count", counts_column: "max"})
        .query(f"iso_code >= {threshold} * {counts_column}")
        .reset_index(drop=True)
    )


def value_total_group(
    data: pd.DataFrame,
    value_column: str = "value",
    group_by: str | list = None,
    threshold: float = 0.95,
) -> pd.DataFrame:
    """Convert units to per capita figures"""

    if group_by is None:
        group_by = ["iso_code", "year"]
    elif isinstance(group_by, str):
        group_by = [group_by]

    if value_column not in data.columns:
        data = data.rename(columns={value_column: "value"})

    # Store columns
    cols = data.columns

    # fill gaps
    data = fill_gaps_by_group(data)

    # Get total counts for each group
    new_group = [c for c in group_by if c != "year"]

    # Add total counts
    data = add_total_counts_by_group(df=data, group=new_group)

    # Filter by threshold
    data = filter_by_threshold(df=data, threshold=threshold, group=group_by)

    return data.filter(cols, axis=1).rename(columns={"value": value_column})
