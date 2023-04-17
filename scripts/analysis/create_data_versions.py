from functools import partial

from scripts import config
from scripts.analysis.download_indicators import (
    current_lcu_data,
    get_current_health_exp,
    get_health_exp_by_disease,
    get_health_exp_by_financing_scheme,
    get_health_exp_by_function,
    get_health_exp_by_source,
)
from scripts.tools import (
    lcu2gdp,
    lcu2usd_constant,
    lcu2usd_current,
    mn2units,
    value2pc,
    year2date,
)

# Function to get the overall spending data in local currency units.
health_exp_current_lcu = partial(current_lcu_data, get_current_health_exp)

# Function to get government total spending data in local currency units
gov_total_exp_current_lcu = partial(
    current_lcu_data,
    get_health_exp_by_source,
    additional_filter={"source": "domestic general government"},
)

# Function to extract COVID-19 spending from the spending by function data, in LCU.
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
health_exp_by_source_current_lcu = partial(current_lcu_data, get_health_exp_by_source)

# Function to get spending by disease
health_exp_by_disease_current_lcu = partial(
    current_lcu_data,
    get_health_exp_by_disease,
)


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
        value.to_feather(config.PATHS.raw_data / f"{dataset_name}_{key}.feather")


def save_data_pipeline() -> None:
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
        spending_function=health_exp_by_source_current_lcu,
        dataset_name="health_spending_by_source",
    )

    # Out-of-pocket spending
    save_data_versions(
        spending_function=health_exp_oop, dataset_name="health_spending_oop"
    )

    # Spending by disease
    save_data_versions(
        spending_function=health_exp_by_disease_current_lcu,
        dataset_name="health_spending_by_disease",
    )
