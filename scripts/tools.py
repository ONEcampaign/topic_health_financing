from functools import partial

import pandas as pd
from bblocks.import_tools.imf import WorldEconomicOutlook

from scripts import config
from scripts.logger import logger

from pydeflate import deflate, set_pydeflate_path, exchange

from scripts.population_data.un_population import un_population_data
from scripts.wb_codes.codes import wb_countries_df

set_pydeflate_path(config.PATHS.pydeflate_data)


def get_weo_indicator(indicator: str, keep_metadata: bool = False) -> pd.DataFrame:
    """Get a single indicator from the World Economic Outlook data."""
    # Create object
    weo = WorldEconomicOutlook(data_path=f"{config.PATHS.raw_data}")

    # Load indicator
    weo.load_indicator(indicator)

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
    target_column="value",
    date_column="year",
)

lcu2usd_constant = partial(
    deflate,
    base_year=config.CONSTANT_YEAR,
    source="imf",
    method="gdp",
    source_currency="LCU",
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


def value2pc(data: pd.DataFrame, value_column: str = "value") -> pd.DataFrame:
    """Convert units to per capita figures"""

    if value_column not in data.columns:
        data = data.rename(columns={value_column: "value"})

    # Store columns
    cols = data.columns

    # Load expenditure data
    population = un_population_data().pipe(year2date)

    # Merge
    data = pd.merge(
        data, population, on=["iso_code", "year"], how="left", suffixes=("", "_pop")
    )

    # Convert to LCU
    data["value"] = round(data.value / data.value_pop, 3)

    # state units
    try:
        data["units"] = data["units"] + " per capita"
    except KeyError:
        data["units"] = "per capita"

    # Drop columns
    return data.filter(cols, axis=1).rename(columns={"value": value_column})


def african_countries() -> dict[str, str]:
    """Return a dictionary of African ISO codes and their names."""
    import country_converter as coco

    return (
        coco.CountryConverter()
        .data[["name_short", "ISO3", "continent"]]
        .query("continent == 'Africa'")
        .set_index("ISO3")["name_short"]
        .to_dict()
    )


def income_levels() -> dict[str, str]:
    """Return a dictionary of iso_codes and their corresponding income level."""
    return wb_countries_df().set_index("iso_code")["income"].to_dict()


def interpolate_missing_values(
    df: pd.DataFrame, date_range: pd.DatetimeIndex, grouper: list[str]
) -> pd.DataFrame:
    """Interpolate missing values in a dataframe"""

    def __interpolate(d_: pd.DataFrame) -> pd.DataFrame:
        idx = [c for c in d_.columns if c not in ["value", "year"]]

        return (
            d_.sort_values(by=["year"])
            .set_index("year")
            .reindex(date_range)
            .set_index(idx, append=True)
            .interpolate(
                method="linear",
                axis=0,
                limit=3,
                limit_direction="both",
                limit_area="inside",
            )
            .reset_index()
            .rename(columns={"level_0": "year"})
        )

    return df.groupby(grouper).apply(__interpolate).reset_index(drop=True)


def _report_missing(
    data: pd.DataFrame, idx: list[str], value_column: str, report_completeness: str
) -> None:

    """Export a summary of the missing data in a dataframe"""

    # Check missing data
    _size = data.groupby(idx, as_index=False).agg(
        {value_column: lambda d: d.isna().sum()}
    )
    _missing = data.groupby(idx, as_index=False).agg(
        {value_column: lambda x: x.isna().mean()}
    )

    (
        pd.merge(_size, _missing, on=idx, suffixes=("_missing", "_missing_share"))
        .assign(
            group_size=lambda d: d.value_missing / d.value_missing_share,
            value_missing_share=lambda d: round(d.value_missing_share * 100, 2),
        )
        .to_csv(
            config.PATHS.raw_data / f"missing_data_{report_completeness}.csv",
            index=False,
        )
    )


def _create_group_total(
    data: pd.DataFrame,
    method: str,
    value_column: str,
    grouper: list[str] = None,
    interpolate: bool = False,
    report_completeness: str = None,
) -> pd.DataFrame:
    """Create a group total for a given dataframe."""

    valid_methods: list = ["median", "sum"]

    if method not in valid_methods:
        raise ValueError(f"method must be one of {valid_methods}")

    if value_column not in data.columns:
        raise ValueError(f"{value_column} not a valid column in data")

    if grouper is None:
        grouper = ["iso_code", "country_name", "indicator_code", "units"]

    for col in grouper:
        if col not in data.columns:
            raise ValueError(f"{col} not a valid column in data")

    if interpolate:
        years = pd.date_range(start=data.year.min(), end=data.year.max(), freq="AS")
        data = interpolate_missing_values(data, grouper=grouper, date_range=years)

    idx = [
        c
        for c in data.columns
        if c not in [value_column, "iso_code", "country_name", "region", "income_group"]
    ]

    if report_completeness is not None:
        _report_missing(data, idx, value_column, report_completeness)

    return data.groupby(idx, as_index=False).agg({value_column: method})


def create_africa_agg(
    data: pd.DataFrame,
    method: str = "median",
    value_column: str = "value",
    interpolate: bool = True,
) -> pd.DataFrame:
    """Create a total for Africa"""

    order = data.columns

    return (
        data.query(f"iso_code in {list(african_countries())}")
        .pipe(
            _create_group_total,
            method=method,
            value_column=value_column,
            interpolate=interpolate,
        )
        .assign(
            iso_code="AFR",
            country_name=f"Africa ({method})",
        )
        .filter(order, axis=1)
    )


def create_income_agg(
    data: pd.DataFrame,
    method: str = "median",
    value_column: str = "value",
    interpolate: bool = True,
    report_completeness: str = None,
) -> pd.DataFrame:
    """Create a total for each income group"""

    incomes = {
        "High income": "HIC",
        "Upper middle income": "UMC",
        "Lower middle income": "LMC",
        "Low income": "LIC",
    }

    order = data.columns

    return (
        data.assign(income_level=lambda d: d.iso_code.map(income_levels()))
        .pipe(
            _create_group_total,
            method=method,
            value_column=value_column,
            interpolate=interpolate,
            report_completeness=report_completeness,
        )
        .rename(columns={"income_level": "country_name"})
        .assign(iso_code=lambda d: d.country_name.map(incomes))
        .filter(order, axis=1)
    )
