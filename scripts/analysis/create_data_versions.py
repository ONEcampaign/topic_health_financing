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
from scripts.tools import value2pc, year2date

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


def save_data_versions(
    spending_function: callable, dataset_name: str, additional_filter: dict = None
) -> None:
    """Save versions of the LCU data"""
    # Local currency units
    if additional_filter is None:
        kwargs = {}
    else:
        kwargs = {"additional_filter": additional_filter}

    full_data = spending_function(**kwargs).pipe(year2date)

    data = {
        "lcu": full_data.loc[lambda d: d.units == "current LCU"].assign(
            units="Current LCU"
        ),
        "usd_current": full_data.loc[lambda d: d.units == "current USD"].assign(
            units="USD"
        ),
        "usd_constant": full_data.loc[lambda d: d.units == "constant USD"].assign(
            units=f"{config.CONSTANT_YEAR} constant USD"
        ),
        "gdp_share": full_data.loc[lambda d: d.units == "percent of GDP"].assign(
            units="share of GDP"
        ),
        "usd_constant_pc": full_data.loc[lambda d: d.units == "constant USD"].pipe(
            value2pc
        ),
    }

    # Save
    for key, value in data.items():
        value.to_feather(config.PATHS.raw_data / f"{dataset_name}_{key}.feather")


def save_data_pipeline() -> None:
    # Overall health spending
    save_data_versions(
        spending_function=get_current_health_exp, dataset_name="health_spending"
    )

    # COVID-19 spending
    save_data_versions(
        spending_function=get_health_exp_by_function,
        dataset_name="covid_spending",
        additional_filter={
            "dimension_1": "COVID-19 spending",
            "source": None,
            "dimension_2": None,
            "dimension_3": None,
        },
    )

    # Government spending
    save_data_versions(
        spending_function=get_health_exp_by_source,
        additional_filter={"source": "domestic general government"},
        dataset_name="gov_spending",
    )

    # Spending by source
    save_data_versions(
        spending_function=get_health_exp_by_source,
        dataset_name="health_spending_by_source",
    )

    # Out-of-pocket spending
    save_data_versions(
        spending_function=get_health_exp_by_financing_scheme,
        additional_filter={"dimension_1": "Household out-of-pocket payment"},
        dataset_name="health_spending_oop",
    )

    # Spending by disease
    save_data_versions(
        spending_function=get_health_exp_by_disease,
        additional_filter={"source": None, "dimension_2": None},
        dataset_name="health_spending_by_disease",
    )


if __name__ == "__main__":
    save_data_pipeline()
