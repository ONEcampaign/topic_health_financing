import pandas as pd
from bblocks import filter_african_countries

from scripts.charts.common import spending_share_of_gdp, total_usd_spending
from scripts.charts.section2 import get_spending_version
from scripts.config import PATHS


def read_data() -> pd.DataFrame:
    constant = get_spending_version(version="usd_constant")
    # Get data in total usd terms (for countries and groups) (pd.DataFrame)
    combined_total = total_usd_spending(usd_constant_data=constant).assign(
        indicator="Total spending ($US billion)"
    )

    # ---- Share of GDP ---------------------->
    combined_gdp = spending_share_of_gdp(
        usd_constant_data=constant, group_by=["income_group", "year"]
    ).assign(indicator="Share of GDP (%)")

    df = pd.concat(
        [
            combined_total,
            combined_gdp,
        ],
        ignore_index=True,
    ).melt(id_vars=["year", "indicator"], var_name="country", value_name="value")

    return df


def _filter_africa(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(filter_african_countries, id_column="country")


def _add_derived_gdp(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.pivot(index=["year", "country"], columns="indicator", values="value")
        .reset_index()
        .assign(
            **{
                "GDP ($US billion)": lambda d: round(
                    d["Total spending ($US billion)"] / (d["Share of GDP (%)"] / 100),
                    4,
                )
            }
        )
    )

    return df


def _add_5p_target(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(**{"5p target": lambda d: round(d["GDP ($US billion)"] * 0.05, 3)})
    return df


def _add_shortfall(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        **{
            "Shortfall": lambda d: round(
                d["5p target"] - d["Total spending ($US billion)"], 3
            )
        }
    )
    return df


def _melt_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.melt(
            id_vars=["year", "country"],
            var_name="indicator",
            value_name="value",
        )
        .set_index(["year", "country", "indicator", "value"])
        .reset_index()
    )


if __name__ == "__main__":
    data = (
        read_data()
        .pipe(_filter_africa)
        .pipe(_add_derived_gdp)
        .pipe(_add_5p_target)
        .pipe(_add_shortfall)
    )
    data.to_csv(PATHS.output / "africa_health_spending_gdp_usd.csv", index=False)
