from functools import partial

import pandas as pd
from oda_data import read_crs, set_data_path, download_crs
from pydeflate import deflate, set_pydeflate_path

from scripts import config
from scripts.config import PATHS
from bblocks import add_income_level_column, set_bblocks_data_path

MULTI_CONSTANT_YEAR: int = 2022
MULTI_START_YEAR: int = 2006
MULTI_END_YEAR: int = 2022

# Set path to raw data folders
set_data_path(PATHS.raw_data)
set_pydeflate_path(PATHS.pydeflate_data)
set_bblocks_data_path(PATHS.raw_data)

# ----------------------------- Sector Groups ------------------------------------------
health = [120]
health_general = list(range(121 * 100, 122 * 100)) + [121]
health_basic = list(range(122 * 100, 123 * 100)) + [122]
health_NCDs = list(range(123 * 100, 124 * 100)) + [123]
pop_RH = list(range(130 * 100, 131 * 100)) + [130]

# All health purpose codes
all_health = health + health_general + health_basic + health_NCDs + pop_RH

# Create broader health sub-groups
health_group = {
    "Health": health,
    "Health, General": health_general,
    "Basic Health": health_basic,
    "Non-communicable diseases (NCDs)": health_NCDs,
    "Population Policies/Programmes & Reproductive Health": pop_RH,
}

# Create a 'Health' sector with a mapping for all relevant purpose codes.
health_broad_group = {
    "Health": "Health",
    "Health, General": "Health",
    "Basic Health": "Health",
    "Non-communicable diseases (NCDs)": "Health",
    "Population Policies/Programmes & Reproductive Health": "Health",
}

# ----------------------------- Multilaterals ------------------------------------------

# Define all the multilaterals used for the analysis.
MULTILATERALS: dict = {
    901: ("International Bank for Reconstruction and Development", [1]),
    905: ("International Development Association", [1]),
    906: ("Caribbean Development Bank", [x for x in range(1, 100)]),
    909: ("Inter-American Development Bank", [x for x in range(1, 100)]),
    910: ("Central American Bank for Economic Integration", [1, 2]),
    913: ("African Development Bank", [x for x in range(1, 100)]),
    914: ("African Development Fund", [x for x in range(1, 100)]),
    915: ("Asian Development Bank", [x for x in range(1, 100)]),
    918: ("European Investment Bank", [3]),
    921: ("Arab Fund", [x for x in range(1, 100)]),
    953: ("Arab Bank for Economic Development in Africa", [x for x in range(1, 100)]),
    976: ("Islamic Development Bank", [1]),
    990: ("European Bank for Reconstruction and Development", [1]),
    1013: ("Council of Europe Development Bank", [x for x in range(1, 100)]),
    1015: ("Development Bank of Latin America", [x for x in range(1, 100)]),
    1019: ("IDB Invest", [x for x in range(1, 100)]),
    1024: ("Asian Infrastructure Investment Bank", [1]),
    1037: ("International Investment Bank", [x for x in range(1, 100)]),
    1044: ("New Development Bank", [1, 2]),
}


# -------------------------------------------------------------------------------------

# helper function to convert from current to constant prices, using DAC data
to_constant_dac = partial(
    deflate,
    base_year=MULTI_CONSTANT_YEAR,
    deflator_source="oecd_dac",
    deflator_method="dac_deflator",
    exchange_source="oecd_dac",
    exchange_method="implied",
    source_currency="USA",
    target_currency="USA",
    id_column="donor_code",
    id_type="DAC",
    source_column="usd_disbursement",
    target_column="usd_disbursement",
    date_column="year",
)


def read_raw_data() -> pd.DataFrame:
    """Read the CRS for the years under study"""
    return read_crs(years=range(MULTI_START_YEAR, MULTI_END_YEAR + 1)).filter(
        [
            "year",
            "donor_code",
            "agency_code",
            "donor_name",
            "recipient_code",
            "recipient_name",
            "region_code",
            "region_name",
            "purpose_code",
            "sector_code",
            "purpose_name",
            "sector_name",
            "flow_name",
            "usd_disbursement",
        ],
        axis=1,
    )


def filter_multi_donors(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the CRS data to only include multilateral donors"""
    return df.loc[lambda d: d.donor_code.isin(MULTILATERALS)]


def add_sectors_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a column with the sector name for health, and 'other' for everything else."""

    df = df.copy(deep=True)

    for sector, codes in health_group.items():
        df.loc[lambda d: d.purpose_code.isin(codes), "sector"] = sector

    return df.fillna({"sector": "Other"})


def add_broad_sectors_column(df: pd.DataFrame) -> pd.DataFrame:
    """Broad sector name for health, and 'other' for everything else"""

    df = df.copy(deep=True)

    for sector, name in health_broad_group.items():
        df.loc[df.sector == sector, "broad_sector"] = name

    return df.fillna({"broad_sector": "Other"})


def _multi_donor_query(donors_dict: dict[str, tuple[str, list[int]]]) -> str:
    """Return a query string to filter a DataFrame for multilateral donors."""
    query = ""
    for bank, (name, codes) in donors_dict.items():
        query += f"(donor_code == {bank} & agency_code.isin({codes})) | "
    return query[:-3]


def filter_mdb_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame of MDB data."""

    query = _multi_donor_query(MULTILATERALS)

    return data.query(query).assign(
        donor_name=lambda d: d.donor_code.map(MULTILATERALS).str[0]
    )


def summarise_by_donor_recipient_year_flow_sector(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data by  donor, recipient, year and sector"""
    group_by = [
        "donor_code",
        "donor_name",
        "recipient_code",
        "recipient_name",
        # "region_code",
        "flow_name",
        "year",
        "sector",
        "broad_sector",
    ]
    return df.groupby(group_by, observed=True, dropna=False, as_index=False)[
        "usd_disbursement"
    ].sum(numeric_only=True)


def rename_ambiguous_recipients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename recipients with ambiguous names to make them unique.
    """

    mapping = {
        "Southern Africa, regional": "Regional Africa Southern, regional",
        "TÃ¼rkiye": "Turkey",
        r"Türkiye": "Turkey",
        r"CÃ´te d'Ivoire": "Cote d'Ivoire",
    }

    df["recipient_name"] = (
        df["recipient_name"].map(mapping).fillna(df["recipient_name"])
    )

    return df


def add_income_levels(df: pd.DataFrame) -> pd.DataFrame:
    """Add an income level column to the data"""

    return add_income_level_column(
        df, id_column="recipient_name", id_type="regex"
    ).fillna({"income_level": "Not classified by income"})


def add_region_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Group sub-regions together into full regions"""
    regions = {
        10003: "Africa",
        10006: "South America",
        15006: "Regional and Unspecified",
        10009: "South & Central Asia",
        10010: "Europe",
        10012: "Oceania",
        10007: "Asia",
        10011: "Middle East",
        10008: "Far East Asia",
        10005: "Caribbean & Central America",
        10002: "Africa",
        10004: "America",
        10001: "Africa",
        298: "Africa",
        798: "Asia",
    }

    return df.assign(region_name=lambda d: d.region_code.map(regions))


if __name__ == "__main__":
    # to update underlying data
    download_crs(years=range(MULTI_START_YEAR, MULTI_END_YEAR + 1))
