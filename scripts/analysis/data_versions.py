from functools import partial

import pandas as pd

from scripts import config
from scripts.analysis.indicators import (
    get_current_health_exp,
    current_lcu_data,
    get_health_exp_by_financing_scheme,
    get_health_exp_by_function,
    get_health_exp_by_source,
)
from scripts.tools import (
    lcu2gdp,
    mn2units,
    year2date,
    lcu2usd_current,
    lcu2usd_constant,
    value2pc,
)

# Function to get the overall spending data in local currency units
health_exp_current_lcu = partial(current_lcu_data, get_current_health_exp)

# Function to get government total spending data in local currency units
gov_total_exp_current_lcu = partial(
    current_lcu_data,
    get_health_exp_by_source,
    additional_filter={"source": "domestic general government"},
)

# Function to extract COVID-19 spending from the spending by function data, in LCU
covid_exp_current_lcu = partial(
    current_lcu_data,
    get_health_exp_by_function,
    additional_filter={"type": "COVID-19 spending"},
)

# function to get other out-of pocket payments
health_exp_oop = partial(
    current_lcu_data,
    get_health_exp_by_financing_scheme,
    additional_filter={"type": "Household out-of-pocket payment"},
)

# Function to get spending by source
health_exp_by_source = partial(current_lcu_data, get_health_exp_by_source)


def save_data_versions(spending_function: callable, dataset_name: str) -> None:
    """Save versions of the LCU data"""
    # Local currency units
    data = {"lcu": spending_function().pipe(year2date).pipe(mn2units)}

    # Share of GDP
    data["gdp_share"] = data["lcu"].pipe(lcu2gdp)

    # USD current
    data["usd_current"] = data["lcu"].pipe(lcu2usd_current).assign(units="USD")

    # USD constant
    data["usd_constant"] = (
        data["lcu"]
        .pipe(lcu2usd_constant)
        .assign(units=f"{config.CONSTANT_YEAR} constant USD")
    )

    # USD constant per capita
    data["usd_constant_pc"] = data["usd_constant"].pipe(value2pc).reset_index(drop=True)

    # Save
    for key, value in data.items():
        value.to_feather(config.PATHS.output / f"{dataset_name}_{key}.feather")


def read_spending_data_versions(dataset_name: str) -> dict[str, pd.DataFrame]:
    """Read versions of the LCU data"""
    return {
        key: pd.read_feather(config.PATHS.output / f"{dataset_name}_{key}.feather")
        for key in [
            "lcu",
            "gdp_share",
            "usd_current",
            "usd_constant",
            "usd_constant_pc",
        ]
    }


def pipeline() -> None:
    # Overall health spending
    save_data_versions(
        spending_function=health_exp_current_lcu, dataset_name="health_spending"
    )

    # COVID-19 spending
    save_data_versions(
        spending_function=covid_exp_current_lcu, dataset_name="covid_spending"
    )

    # Government spending
    save_data_versions(
        spending_function=gov_total_exp_current_lcu, dataset_name="gov_spending"
    )

    # Spending by source
    save_data_versions(
        spending_function=health_exp_by_source, dataset_name="health_spending_by_source"
    )

    # Out-of-pocket spending
    save_data_versions(
        spending_function=health_exp_oop, dataset_name="health_spending_oop"
    )


if __name__ == "__main__":
    # Run the pipeline
    pipeline()

    overall_spending = read_spending_data_versions(dataset_name="health_spending")
    covid_spending = read_spending_data_versions(dataset_name="covid_spending")
    gov_spending = read_spending_data_versions(dataset_name="gov_spending")
    scheme_spending = read_spending_data_versions(
        dataset_name="health_spending_by_source"
    )
    oop_spending = read_spending_data_versions(dataset_name="health_spending_oop")
