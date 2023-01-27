from functools import partial

import pandas as pd

from scripts import config
from scripts.indicators import get_current_health_exp
from scripts.tools import (
    lcu2gdp,
    mn2units,
    year2date,
    lcu2usd_current,
    lcu2usd_constant,
    value2pc,
)

health_exp_current_lcu = partial(
    get_current_health_exp, additional_filter={"units": "national currency unit"}
)


def save_spending_data_versions() -> None:
    """Save versions of the LCU data"""
    # Local currency units
    data = {"lcu": health_exp_current_lcu().pipe(year2date).pipe(mn2units)}

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
    data["usd_constant_pc"] = data["usd_constant"].pipe(value2pc)

    # Save
    for key, value in data.items():
        value.to_feather(config.PATHS.output / f"health_spending_{key}.feather")


def read_spending_data_versions() -> dict[str, pd.DataFrame]:
    """Read versions of the LCU data"""
    return {
        key: pd.read_feather(config.PATHS.output / f"health_spending_{key}.feather")
        for key in [
            "lcu",
            "gdp_share",
            "usd_current",
            "usd_constant",
            "usd_constant_pc",
        ]
    }


if __name__ == "__main__":
    # save_spending_data_versions()

    spending = read_spending_data_versions()
