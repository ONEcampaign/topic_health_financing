from functools import partial

import pandas as pd
from oda_data import read_crs, set_data_path
from pydeflate import deflate, set_pydeflate_path

from scripts import config
from scripts.config import PATHS

set_data_path(PATHS.raw_data)
set_pydeflate_path(PATHS.pydeflate_data)

START_YEAR: int = 2006
END_YEAR: int = 2021

health = [120]
health_general = list(range(121 * 100, 122 * 100)) + [121]
health_basic = list(range(122 * 100, 123 * 100)) + [122]
health_NCDs = list(range(123 * 100, 124 * 100)) + [123]
pop_RH = list(range(130 * 100, 131 * 100)) + [130]

all_health = health + health_general + health_basic + health_NCDs + pop_RH

health_group = {
    "Health": health,
    "Health, General": health_general,
    "Basic Health": health_basic,
    "Non-communicable diseases (NCDs)": health_NCDs,
    "Population Policies/Programmes & Reproductive Health": pop_RH,
}

health_broad_group = {
    "Health": "Health",
    "Health, General": "Health",
    "Basic Health": "Health",
    "Non-communicable diseases (NCDs)": "Health",
    "Population Policies/Programmes & Reproductive Health": "Health",
}

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
to_constant_dac = partial(
    deflate,
    base_year=config.CONSTANT_YEAR,
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
    return read_crs(years=range(START_YEAR, END_YEAR + 1)).filter(
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

    return df.groupby(
        [
            "donor_code",
            "donor_name",
            "recipient_code",
            "recipient_name",
            "region_code",
            "flow_name",
            "year",
            "sector",
            "broad_sector",
        ],
        observed=True,
        dropna=False,
        as_index=False,
    )["usd_disbursement"].sum(numeric_only=True)


def summarise_all_flows(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but flows"""

    cols = [c for c in df.columns if c not in ["flow_name", "usd_disbursement"]]

    return (
        df.groupby(cols, observed=True, dropna=False, as_index=False)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(flow_name="Total (ODA + OOF)")
    )


def summarise_all_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but sectors"""

    cols = [c for c in df.columns if c not in ["sector", "usd_disbursement"]]

    return (
        df.groupby(cols, observed=True, dropna=False, as_index=False)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(sector="Total")
    )


def summarise_all_broad_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but broad sectors"""

    cols = [c for c in df.columns if c not in ["broad_sector", "usd_disbursement"]]

    return (
        df.groupby(cols, observed=True, dropna=False, as_index=False)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(broad_sector="Total")
    )


def get_africa_total(df: pd.DataFrame) -> pd.DataFrame:
    regions = [r for r in df.region_name.unique() if ("frica" in r or "ahara" in r)]
    columns = [
        c
        for c in df.columns
        if c
        not in ["usd_disbursement", "region_name", "recipient_name", "recipient_code"]
    ]

    return (
        df.query(f"region_name in {regions}")
        .groupby(columns, as_index=False, dropna=False, observed=True)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(recipient_name="Africa")
    )


def get_income_totals(df: pd.DataFrame) -> pd.DataFrame:
    from bblocks import add_income_level_column

    df = add_income_level_column(
        df, id_column="recipient_name", id_type="regex"
    ).fillna({"income_level": "Not classified by income"})

    columns = [
        c
        for c in df.columns
        if c
        not in ["usd_disbursement", "recipient_name", "recipient_code", "region_name"]
    ]

    return (
        df.groupby(columns, as_index=False, dropna=False, observed=True)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(recipient_name=lambda d: d.income_level)
        .drop(columns=["income_level"])
    )


def add_income_levels(df: pd.DataFrame) -> pd.DataFrame:
    from bblocks import add_income_level_column

    return add_income_level_column(
        df, id_column="recipient_name", id_type="regex"
    ).fillna({"income_level": "Not classified by income"})


def add_region_groups(df: pd.DataFrame) -> pd.DataFrame:
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
