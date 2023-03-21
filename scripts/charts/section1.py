from functools import partial

import pandas as pd
from bblocks import convert_id

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    combine_income_countries,
    get_version,
    per_capita_africa,
    per_capita_by_income,
    total_africa,
    total_by_income,
)
from scripts.config import PATHS
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
    value2gov_spending_share,
    value2gov_spending_share_group,
)

SPENDING = read_spending_data_versions(dataset_name="health_spending")


get_spending_version = partial(get_version, versions_dict=SPENDING)


def chart1_1_pipeline() -> None:
    # Get total spending in constant USD
    total_spending = get_spending_version(version="usd_constant")

    # ---- Per capita spending ---------------------->

    # Get per capita spending in constant USD
    pc_spending_countries = get_spending_version(version="usd_constant_pc")

    # Calcualte per capita spending for income groups (total)
    pc_spending_income = per_capita_by_income(total_spending)

    # Calculate per capita spending for Africa (total)
    pc_spending_africa = per_capita_africa(total_spending)

    # Combine the datasets
    combined_pc = combine_income_countries(
        income=pc_spending_income,
        country=pc_spending_countries,
        africa=pc_spending_africa,
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

    # Get total spending for Africa (in bilion)
    total_spending_africa = total_africa(total_spending).assign(
        value=lambda d: round(d.value / 1e9, 3)
    )

    # Combine the datasets
    combined_total = combine_income_countries(
        income=total_spending_income,
        country=total_spending_countries,
        africa=total_spending_africa,
    ).assign(indicator="Total spending ($US billion)")

    # ---- Share of GDP ---------------------->
    # Calculate % of GDP by income
    gdp_share_income = value2gdp_share_group(
        total_spending, group_by=["income_group", "year"]
    )

    gpd_share_africa = value2gdp_share_group(
        total_spending.assign(
            country_name=lambda d: convert_id(
                d.iso_code, from_type="ISO3", to_type="continent"
            )
        ).query("country_name == 'Africa'"),
        group_by=["country_name", "year"],
    )

    gdp_share_countries = value2gdp_share(total_spending)

    # Combine the datasets
    combined_gdp = combine_income_countries(
        income=gdp_share_income,
        country=gdp_share_countries,
        africa=gpd_share_africa,
    ).assign(indicator="Share of GDP (%)")

    # ---- Share of Government spending ---------------------->
    # Calculate % of government spending
    govx_share_income = value2gov_spending_share_group(
        total_spending, group_by=["income_group", "year"]
    )

    # Africa
    govx_share_africa = value2gov_spending_share_group(
        total_spending.assign(
            country_name=lambda d: convert_id(
                d.iso_code, from_type="ISO3", to_type="continent"
            )
        ).query("country_name == 'Africa'"),
        group_by=["country_name", "year"],
    )

    govx_share_countries = value2gov_spending_share(total_spending)

    # Combine the datasets
    combined_govx = combine_income_countries(
        income=govx_share_income,
        country=govx_share_countries,
        africa=govx_share_africa,
    ).assign(indicator="Share of government spending (%)")

    # ---- Combine all -------------------------->

    indicator_order = {
        "Total spending ($US billion)": 1,
        "Per capita spending ($US)": 2,
        "Share of GDP (%)": 3,
        "Share of government spending (%)": 4,
    }

    # Combine both views of the data
    df = (
        pd.concat(
            [
                combined_pc,
                combined_total,
                combined_gdp,
                combined_govx,
            ],
            ignore_index=True,
        )
        .assign(order=lambda d: d.indicator.map(indicator_order))
        .sort_values(["order", "year"], ascending=(True, True))
        .drop(columns=["order"])
        .set_index(["year", "indicator"])
        .reset_index()
    )

    # Copy to clipboard
    df.to_csv(PATHS.output / "section1_chart1.csv", index=False)


if __name__ == "__main__":
    ...
    chart1_1_pipeline()
