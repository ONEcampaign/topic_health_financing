from functools import partial

import pandas as pd

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    combine_income_countries,
    get_version,
    per_capita_by_income,
    total_by_income,
)
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
)

SPENDING = read_spending_data_versions(dataset_name="health_spending")


get_spending_version = partial(get_version, versions_dict=SPENDING)


def pipeline() -> None:

    # Get total spending in constant USD
    total_spending = get_spending_version(version="usd_constant")

    # ---- Per capita spending ---------------------->

    # Get per capita spending in constant USD
    pc_spending_countries = get_spending_version(version="usd_constant_pc")

    # Calcualte per capita spending for income groups (total)
    pc_spending_income = per_capita_by_income(total_spending)

    # Combine the datasets
    combined_pc = combine_income_countries(
        pc_spending_income, pc_spending_countries
    ).assign(indicator="Per capita spending ($US)")

    # ---- Total spending ---------------------->

    # Calculate total spending for income groups (total)
    total_spending_income = total_by_income(total_spending).assign(
        value=lambda d: round(d.value / 1e9, 3)
    )

    # Get total spending per country (in billion)
    total_spending_countries = total_spending.assign(
        value=lambda d: round(d.value / 1e9, 3)
    )
    # Combine the datasets
    combined_total = combine_income_countries(
        total_spending_income, total_spending_countries
    ).assign(indicator="Total spending ($US billion)")

    # ---- Share of GDP ---------------------->
    # Calculate % of GDP by income
    gdp_share_income = value2gdp_share_group(
        total_spending, group_by=["income_group", "year"]
    )

    gdp_share_countries = value2gdp_share(total_spending)

    # Combine the datasets
    combined_gdp = combine_income_countries(
        gdp_share_income, gdp_share_countries
    ).assign(indicator="Share of GDP (%)")

    # ---- Combine all -------------------------->

    indicator_order = {
        "Per capita spending ($US)": 2,
        "Total spending ($US billion)": 1,
        "Share of GDP (%)": 3,
    }

    # Combine both views of the data
    df = (
        pd.concat(
            [
                combined_pc,
                combined_total,
                combined_gdp,
            ],
            ignore_index=True,
        )
        .assign(order=lambda d: d.indicator.map(indicator_order))
        .sort_values(["order", "year"], ascending=(True, True))
        .drop(columns=["order"])
    )

    # Copy to clipboard
    df.to_clipboard(index=False)


if __name__ == "__main__":
    pipeline()
