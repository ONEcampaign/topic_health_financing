import pandas as pd
from bblocks import format_number

from scripts.analysis.multilateral import (
    add_broad_sectors_column,
    add_income_levels,
    add_region_groups,
    add_sectors_column,
    filter_mdb_data,
    filter_multi_donors,
    read_raw_data,
    summarise_by_donor_recipient_year_flow_sector,
    to_constant_dac,
)
from scripts.config import PATHS


def summarise_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but regions"""

    cols = [
        c
        for c in df.columns
        if c
        not in ["recipient_name", "recipient_code", "region_code", "usd_disbursement"]
    ]

    return df.groupby(cols, observed=True, dropna=False, as_index=False)[
        "usd_disbursement"
    ].sum(numeric_only=True)


def summarise_income_levels(df: pd.DataFrame) -> pd.DataFrame:
    """"""
    cols = [c for c in df.columns if c not in ["income_level", "usd_disbursement"]]

    return df.groupby(cols, observed=True, dropna=False, as_index=False)[
        "usd_disbursement"
    ].sum(numeric_only=True)


def summarise_health(df: pd.DataFrame) -> pd.DataFrame:
    columns = [c for c in df.columns if c not in ["sector", "usd_disbursement"]]

    not_health = df.query("sector == 'Other'")
    health = df.query("sector != 'Other'")

    health = (
        health.groupby(columns, as_index=False, dropna=False, observed=True)
        .sum(numeric_only=True)
        .assign(sector="Health")
    )

    return pd.concat([health, not_health], ignore_index=True)


def summarise_all_donors(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but donors"""

    cols = [
        c
        for c in df.columns
        if c not in ["donor_code", "donor_name", "usd_disbursement"]
    ]

    return (
        df.groupby(cols, observed=True, dropna=False, as_index=False)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(donor_name="MDBs, Total")
    )


def summarise_all_recipients(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but recipient"""

    cols = [
        c
        for c in df.columns
        if c not in ["recipient_code", "recipient_name", "usd_disbursement"]
    ]

    return (
        df.groupby(cols, observed=True, dropna=False, as_index=False)[
            "usd_disbursement"
        ]
        .sum(numeric_only=True)
        .assign(recipient_name="All Developing Countries")
    )


def summarise_flow_type(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data all columns but flow type"""

    cols = [c for c in df.columns if c not in ["flow_name", "usd_disbursement"]]

    return df.groupby(cols, observed=True, dropna=False, as_index=False)[
        "usd_disbursement"
    ].sum(numeric_only=True)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {
        "year": "Year",
        "donor_name": "Donor",
        "recipient_name": "Recipient",
        "region_name": "Region",
        "flow_name": "Type",
        "sector": "Sector",
        "broad_sector": "Broad Sector",
        "usd_disbursement": "Disbursement",
    }

    return df.filter(cols, axis=1).rename(columns=cols)


def rename_regions(df: pd.DataFrame) -> pd.DataFrame:
    regions = {
        "Africa": "Africa",
        "South America": "South America",
        "Caribbean & Central America": "North & Central America",
        "America": "North & Central America",
        "Europe": "Europe",
        "Middle East": "Middle East",
        "South & Central Asia": "Asia",
        "Far East Asia": "Asia",
        "Asia": "Asia",
        "Oceania": "Oceania",
        "Regional and Unspecified": "Other, unspecified",
    }

    return df.assign(region_name=lambda d: d.region_name.map(regions))


def sort_sankey(df: pd.DataFrame) -> pd.DataFrame:
    flow_order = {
        "Total (ODA + OOF)": 0,
        "ODA Grants": 1,
        "ODA Loans": 2,
        "Equity Investment": 3,
        "Other Official Flows (non Export Credit)": 4,
    }

    return (
        df.assign(
            flow_order=lambda d: d.From.map(flow_order),
        )
        .sort_values(["Year", "flow_order", "To"], ascending=(False, True, True))
        .drop(columns=["flow_order"])
        .reset_index(drop=True)
    )


def chart_4_1() -> None:
    data = read_raw_data()

    df = (
        data.pipe(filter_multi_donors)
        .pipe(filter_mdb_data)
        .pipe(add_sectors_column)
        .pipe(add_broad_sectors_column)
        .pipe(summarise_by_donor_recipient_year_flow_sector)
        .pipe(to_constant_dac)
        .pipe(add_income_levels)
        .pipe(add_region_groups)
        .pipe(rename_regions)
    )

    health = df.query("broad_sector == 'Health'")

    health = (
        health.pipe(summarise_all_donors)
        .pipe(summarise_health)
        .pipe(summarise_regions)
        .pipe(summarise_income_levels)
    )

    chart = health.pipe(rename_columns)

    part1 = (
        chart.groupby(
            ["Year", "Donor", "Type"], as_index=False, dropna=False, observed=True
        )["Disbursement"]
        .sum(numeric_only=True)
        .rename(columns={"Type": "To", "Donor": "From"})
    )

    part2 = (
        chart.groupby(
            ["Year", "Region", "Type"], as_index=False, dropna=False, observed=True
        )["Disbursement"]
        .sum(numeric_only=True)
        .rename(columns={"Type": "From", "Region": "To"})
    )

    sankey = (
        pd.concat([part1, part2], ignore_index=True)
        .filter(["Year", "From", "To", "Disbursement"], axis=1)
        .pipe(sort_sankey)
        .assign(Disbursement=lambda d: round(d.Disbursement, 1))
    )

    sankey.to_csv(PATHS.output / "chart_4_1.csv", index=False)


def pivot_line_graph(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(
        index=["Year", "Donor", "Region"], columns="Sector", values="Disbursement"
    ).reset_index()


def create_tooltip_4_2(df: pd.DataFrame) -> pd.DataFrame:
    data = df.set_index(["Year", "Donor", "Region"])

    df = data.assign(total=lambda d: d.sum(axis=1))

    for column in [
        "Basic Health",
        "Health, General",
        "Population & Reproductive Health",
        "Non-communicable diseases (NCDs)",
    ]:
        df[column] = format_number(
            df[column] / df["total"], as_percentage=True, decimals=1
        ).replace("nan", "-")

    df["tooltip"] = (
        "<b>Basic Health:</b> "
        + df["Basic Health"]
        + "<br>"
        + "<b>Health, General:</b> "
        + df["Health, General"]
        + "<br>"
        + "<b>Population & Reproductive Health:</b> "
        + df["Population & Reproductive Health"]
        + "<br>"
        + "<b>Non-communicable diseases (NCDs):</b> "
        + df["Non-communicable diseases (NCDs)"]
        + ""
    )
    return data.assign(tooltip=df["tooltip"]).reset_index()


def chart_4_2() -> None:
    data = read_raw_data()

    df = (
        data.pipe(filter_multi_donors)
        .pipe(filter_mdb_data)
        .pipe(add_sectors_column)
        .pipe(add_broad_sectors_column)
        .pipe(summarise_by_donor_recipient_year_flow_sector)
        .pipe(to_constant_dac)
        .pipe(add_income_levels)
        .pipe(add_region_groups)
        .pipe(rename_regions)
    )

    health = df.query("broad_sector == 'Health'")

    health = (
        health.pipe(summarise_all_donors)
        .pipe(summarise_flow_type)
        .pipe(summarise_regions)
        .pipe(summarise_income_levels)
    )

    chart = (
        health.pipe(rename_columns)
        .filter(["Year", "Donor", "Region", "Sector", "Disbursement"], axis=1)
        .pipe(pivot_line_graph)
        .rename(
            columns={
                "Population Policies/Programmes &"
                " Reproductive Health": "Population & Reproductive Health"
            }
        )
        .filter(
            [
                "Year",
                "Donor",
                "Region",
                "Basic Health",
                "Health, General",
                "Population & Reproductive Health",
                "Non-communicable diseases (NCDs)",
            ],
            axis=1,
        )
    )

    chart = chart.pipe(create_tooltip_4_2)

    chart.to_csv(PATHS.output / "chart_4_2.csv", index=False)
