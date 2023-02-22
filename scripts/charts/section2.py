from functools import partial

import pandas as pd

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    combine_income_countries,
    get_version,
    per_capita_by_income,
)
from scripts.tools import value2gov_spending_share

GOV_SPENDING = read_spending_data_versions(dataset_name="gov_spending")

get_spending_version = partial(get_version, versions_dict=GOV_SPENDING)

# Get total spending in constant USD
total_spending = get_spending_version(version="usd_constant")

# ---- Share of gov spending ---------------------->

# Get per capita spending in constant USD
spending_countries = get_spending_version(version="usd_constant")

# Calculate per capita spending for income groups (total)
gov_spending_share_countries = value2gov_spending_share(spending_countries)

d = gov_spending_share_countries.query(
    "year.dt.year == 2018 and country_name != 'Liberia'"
)
d = d.filter(["country_name", "income_group", "value"])
d = d.sort_values(by=["value"], ascending=False)
order = d.country_name.to_list()
d = d.assign(country=lambda d: d.country_name)
d = (
    d.pivot(index=["country", "income_group"], columns="country_name", values="value")
    .filter(order)
    .reset_index()
)


def bin_values(df, bin_ranges):
    """
    Bins the values in a pandas dataframe column into groups based on specified ranges.

    Parameters:
        df (pandas.DataFrame): The dataframe containing the data to bin.
        bin_ranges (list): A list of tuples containing the lower and upper bounds of each bin.

    Returns:
        pandas.DataFrame: A new dataframe with an additional column containing the number of values
        that fall into each bin.
    """
    conditions = [
        (
            bin_range[0],
            f"{bin_range[0]}%-{bin_range[1]}%",
            f"{bin_range[0]} <= value < {bin_range[1]}",
        )
        for bin_range in bin_ranges
    ]

    frames = []

    for idx, label, condition in conditions:
        frames.append(df.query(condition).assign(spending=f"{idx}", range=label))

    data = pd.concat(frames, ignore_index=True)

    order = {
        "High income": 1,
        "Upper-middle income": 2,
        "Lower-middle income": 3,
        "Low income": 4,
    }

    data = data.groupby(
        ["income_group", "spending", "range", "year"], group_keys=True
    ).apply(
        lambda d: d.assign(
            countries=lambda x: x.iso_code.count(),
            note=lambda x: x.country_name.str.cat(sep=", "),
        )
        .drop_duplicates(subset="note")
        .filter(["countries", "note"])
    )

    data = (
        data.reset_index()
        .assign(
            order=lambda d: d.income_group.map(order), year=lambda d: d.year.dt.year
        )
        .sort_values(["year", "order", "range"])
        .drop(columns=["order", "level_4"])
    )

    return data


bins = [(x / 10, y / 10) for x, y in zip(range(0, 200, 5), range(5, 205, 5))]


d = bin_values(gov_spending_share_countries.query("year.dt.year == 2019"), bins)


def histogram_chart(df) -> None:
    """Create a curved histogram of Gender Inequality Index by continent"""

    bins = [
        0,
        0.0001,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
    ]
    labels = {
        "0": 0,
        "0.001-0.1": 0.05,
        "0.1-1": 0.55,
        "2-3": 2.5,
        "3-4": 3.5,
        "4-5": 4.5,
        "5-6": 5.5,
        "6-7": 6.5,
        "7-8": 7.5,
        "8-9": 8.5,
        "9-10": 9.5,
        "10-11": 10.5,
        "11-12": 11.5,
        "12-13": 12.5,
        "13-14": 13.5,
        "14-15": 14.5,
        "15-16": 15.5,
        "16-17": 16.5,
        "17-18": 17.5,
        "18-19": 18.5,
        "19-20": 19.5,
    }
    order = {
        "High income": 1,
        "Upper-middle income": 2,
        "Lower-middle income": 3,
        "Low income": 4,
    }

    return (
        df.assign(
            binned=lambda d: pd.cut(
                d.value, bins=bins, labels=labels.keys(), include_lowest=True
            ),
        )
        .groupby(["binned", "income_group"])
        .size()
        .reset_index(name="counts")
        .assign(x_values=lambda d: d.binned.map(labels))
        .assign(order=lambda d: d.income_group.map(order))
        .sort_values(["order", "x_values"])
        .drop(columns=["order"])
    )


d2 = histogram_chart(gov_spending_share_countries.query("year.dt.year == 2019"))
