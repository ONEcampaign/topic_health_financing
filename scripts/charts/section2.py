import json
from functools import partial

import pandas as pd
from bblocks import convert_id

from scripts import config
from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    get_version,
)
from scripts.tools import value2gdp_share, value2gov_spending_share, value2pc

GOV_SPENDING = read_spending_data_versions(dataset_name="gov_spending")

get_spending_version = partial(get_version, versions_dict=GOV_SPENDING)


def get_government_spending_shares() -> pd.DataFrame:

    # ---- Share of gov spending ---------------------->

    # Get spending in constant USD
    spending_countries = get_spending_version(version="usd_constant")

    # Calculate as a share of government spending for income groups (total)
    gov_spending_share_countries = value2gov_spending_share(spending_countries)

    df = gov_spending_share_countries.query("country_name != 'Liberia'")

    return df


def get_gdp_spending_shares() -> pd.DataFrame:

    # ---- Share of gov spending ---------------------->

    # Get spending in constant USD
    spending_countries = get_spending_version(version="usd_constant")

    # Calculate as a share of government spending for income groups (total)
    gdp_share_countries = value2gdp_share(spending_countries)

    df = gdp_share_countries.query("country_name != 'Liberia'")

    return df


def get_per_capita_spending() -> pd.DataFrame:

    # ---- Share of gov spending ---------------------->

    # Get spending in constant USD
    spending_countries = get_spending_version(version="usd_constant")

    # Calculate as a share of government spending for income groups (total)
    per_capita = value2pc(spending_countries)

    df = per_capita.query("country_name != 'Liberia'")

    return df


def read_au_countries() -> list:

    # read json file
    with open(config.PATHS.raw_data / "AU_members.json", "r") as f:
        au_members = json.load(f)

    return au_members


def filter_au_countries(df: pd.DataFrame) -> pd.DataFrame:

    return df.query(f"iso_code in {read_au_countries()}")


def clean_data_for_chart(df: pd.DataFrame) -> pd.DataFrame:
    order = {
        "High income": 1,
        "Upper-middle income": 2,
        "Lower-middle income": 3,
        "Low income": 4,
    }

    return (
        df.filter(["country_name", "income_group", "value", "year"], axis=1)
        .assign(
            order=lambda d: d["income_group"].map(order),
            year_note=lambda d: d["year"].dt.year,
            year=lambda d: d["year"].dt.strftime("%Y-%m-%d"),
        )
        .sort_values(by=["order", "year", "value"], ascending=(True, False, False))
        .drop(columns="order")
    )


def flag_africa(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Continent=lambda d: convert_id(
            d.country_name, from_type="regex", to_type="continent"
        ),
    ).assign(
        Continent=lambda d: d.Continent.apply(lambda x: x if x == "Africa" else "Other")
    )


def chart_2_1() -> None:
    df = (
        get_government_spending_shares()
        .pipe(filter_au_countries)
        .pipe(clean_data_for_chart)
    )

    df.to_csv(config.PATHS.output / "section2_chart1.csv", index=False)


def chart_2_2_1() -> None:
    df_gdp = get_gdp_spending_shares().pipe(clean_data_for_chart).pipe(flag_africa)
    df_gdp.to_csv(config.PATHS.output / "section2_chart2_1.csv", index=False)


def chart_2_2_2() -> None:
    df_pc = get_per_capita_spending().pipe(clean_data_for_chart).pipe(flag_africa)
    df_pc.to_csv(config.PATHS.output / "section2_chart2_2.csv", index=False)


if __name__ == "__main__":
    ...
    # chart_2_1()
