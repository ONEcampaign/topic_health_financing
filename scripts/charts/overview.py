import pandas as pd
from bblocks import convert_id

from scripts.charts.common import update_key_number
from scripts.config import PATHS


def _read_spending() -> pd.DataFrame:
    return pd.read_csv(
        PATHS.output / "section1_chart1.csv", parse_dates=["year"]
    ).assign(year=lambda d: d.year.dt.year)


def _read_gov_spending() -> pd.DataFrame:
    return pd.read_csv(
        PATHS.output / "section2_chart1.csv", parse_dates=["year"]
    ).assign(year=lambda d: d.year.dt.year)


def _filter_latest2y(df: pd.DataFrame) -> pd.DataFrame:
    return df.query("year in [year.max(),year.max()-1]")


def _filter_indicator(df: pd.DataFrame, indicator: str) -> pd.DataFrame:
    return df.query(f"indicator == '{indicator}'")


def _filter_income_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.filter(
        [
            "year",
            "High income",
            "Low income",
            "Lower middle income",
            "Upper middle income",
        ],
        axis=1,
    )


def _filter_africa_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.filter(["year", "Africa"], axis=1)


def _reshape_vertical(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(id_vars=["year"], var_name="entity", value_name="value")


def _summarise_by_year(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["year"], as_index=False).sum(numeric_only=True)


def _calculate_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(change=lambda d: d.value.pct_change())


def abuja_count() -> int:
    return (
        _read_gov_spending()
        .loc[lambda d: d.year == d.year.max()]
        .loc[lambda d: d.value >= 15]
        .country_name.nunique()
    )


def as_smt_dict_total(df: pd.DataFrame, prefix: str) -> dict:
    result = {
        f"{prefix}_latest_year": f"{df.year.max()}",
        f"{prefix}_previous_year": f"{df.year.max() - 1}",
        f"{prefix}_latest_value": df.query("year == year.max()").value.values[0],
        f"{prefix}_change": df.query("year == year.max()").change.values[0],
    }

    return result


def as_smt_dict_shares(df: pd.DataFrame) -> dict:
    result = {
        "income_year": f"{df.Year.max()}",
        "share_high_income": str(df.query("entity == 'High income'").Share.values[0]),
        "share_low_income": str(df.query("entity == 'Low income'").Share.values[0]),
        "share_lower_middle_income": str(
            df.query("entity == 'Lower middle income'").Share.values[0]
        ),
        "share_upper_middle_income": str(
            df.query("entity == 'Upper middle income'").Share.values[0]
        ),
    }
    return result


def as_smt_df(
    title: str,
    top: int | float,
    bottom_text: str,
    bottom: int | float,
    centre: float,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "title": [title],
            "top": [top],
            "bottom_text": [bottom_text],
            "bottom": [bottom],
            "centre": [centre],
        }
    )


def total_spending_sm() -> None:
    """Total health spending latest year compared to previous year"""

    data = (
        _read_spending()
        .pipe(_filter_latest2y)
        .pipe(_filter_indicator, indicator="Total spending ($US billion)")
        .pipe(_filter_income_data)
        .pipe(_reshape_vertical)
        .pipe(_summarise_by_year)
        .pipe(_calculate_pct_change)
        .assign(
            value=lambda d: round(d.value * 1e6, 1),
            change=lambda d: round(d.change * 100, 1),
        )
        .pipe(as_smt_dict_total, prefix="total_health")
    )

    chart = as_smt_df(
        title=f"As of {data['total_health_latest_year']}",
        top=data["total_health_latest_value"],
        bottom_text=f"Compared to {data['total_health_previous_year']}",
        bottom=data["total_health_change"],
        centre=data["total_health_change"] / 10,
    )

    kn_dict = {
        "total_health_spending": f'{data["total_health_latest_value"] / 1e9:.1f} trillion',
        "total_health_spending_year": f'{data["total_health_latest_year"]}',
        "total_health_spending_change": f"{data['total_health_change']}%",
        "total_health_spending_previous_year": f'{data["total_health_previous_year"]}',
    }

    update_key_number(PATHS.output / "overview.json", new_dict=kn_dict)

    # Save chart
    chart.to_csv(PATHS.output / "overview_c1.csv", index=False)


def total_spending_by_income_bar() -> None:
    """Total health spending latest year compared to previous year"""

    data = (
        _read_spending()
        .pipe(_filter_latest2y)
        .pipe(_filter_indicator, indicator="Total spending ($US billion)")
        .pipe(_filter_income_data)
        .pipe(_reshape_vertical)
        .loc[lambda d: d.year == d.year.max()]
        .assign(
            Share=lambda d: round(100 * d.value / d.value.sum(), 1),
            value=lambda d: round(d.value, 1),
        )
        .rename(columns={"value": "$US billion", "year": "Year"})
    )

    kn_dict = data.pipe(as_smt_dict_shares)

    update_key_number(PATHS.output / "overview.json", new_dict=kn_dict)

    # Save chart
    data.to_csv(PATHS.output / "overview_c2.csv", index=False)


def africa_spending_trend_line() -> None:
    """Time series of total health spending in Africa"""
    data_us = (
        _read_spending()
        .pipe(_filter_indicator, indicator="Total spending ($US billion)")
        .pipe(_filter_africa_data)
        .pipe(_reshape_vertical)
    )

    data_gdp = (
        _read_spending()
        .pipe(_filter_indicator, indicator="Share of GDP (%)")
        .pipe(_filter_africa_data)
        .pipe(_reshape_vertical)
    )

    data = data_us.merge(
        data_gdp, on=["year", "entity"], suffixes=("_spending", "_gdp")
    ).rename(columns={"value_spending": "Africa", "value_gdp": "Share of GDP (%)"})

    key_numbers = (
        data_us.pipe(_calculate_pct_change)
        .assign(
            value=lambda d: round(d.value, 1),
            change=lambda d: round(d.change * 100, 1),
        )
        .pipe(as_smt_dict_total, prefix="africa_health")
    )

    key_numbers["met_abuja"] = f"{abuja_count()}"

    update_key_number(PATHS.output / "overview.json", new_dict=key_numbers)

    # Save chart
    data.to_csv(PATHS.output / "overview_c3.csv", index=False)


if __name__ == "__main__":
    total_spending_sm()
    total_spending_by_income_bar()
    africa_spending_trend_line()
