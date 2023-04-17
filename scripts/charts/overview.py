import pandas as pd
from bblocks import add_income_level_column, convert_id, set_bblocks_data_path

from scripts.charts.common import update_key_number
from scripts.config import PATHS

set_bblocks_data_path(PATHS.raw_data)


def _read_spending() -> pd.DataFrame:
    return pd.read_csv(
        PATHS.output / "section1_chart1.csv", parse_dates=["year"]
    ).assign(year=lambda d: d.year.dt.year)


def _read_gov_spending() -> pd.DataFrame:
    return pd.read_csv(
        PATHS.output / "section2_chart3.csv",
    ).rename(columns={"year_note": "year"})


def _read_external() -> pd.DataFrame:
    return pd.read_csv(
        PATHS.output / "section3_chart1.csv",
    )


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


def _filter_income_data_rows(df: pd.DataFrame, column: str = "Country") -> pd.DataFrame:
    incomes = [
        "High income",
        "Low income",
        "Lower middle income",
        "Upper middle income",
    ]

    return df.query(f"{column} in {incomes}").reset_index(drop=True)


def _filter_africa_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.filter(["year", "Africa"], axis=1)


def _reshape_vertical(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(id_vars=["year"], var_name="entity", value_name="value")


def _summarise_by_year(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["year"], as_index=False).sum(numeric_only=True)


def _calculate_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(change=lambda d: d.value.pct_change())


def _spending_income(data: pd.DataFrame, income: str) -> dict:
    numbers = {
        f"{income}_spending": str(
            round(
                data.loc[
                    (data.entity == income) & (data.year == data.year.max()),
                    "value",
                ].sum(),
                1,
            )
        ),
        f"{income}_spending_change": str(
            round(
                data.loc[
                    (data.entity == income) & (data.year == data.year.max()),
                    "change",
                ].sum()
                * 100,
                1,
            )
        ),
    }

    return numbers


def _change_direction(
    data: pd.DataFrame, variable: str, entity: str
) -> tuple[float, str]:
    result = (
        data.query(f"variable == '{variable}' and Country == '{entity}'")
        .difference.sum()
        .round(1)
    )

    result_direction = "increase" if result > 0 else "decrease"

    return result, result_direction


def abuja_count() -> str:
    countries = (
        _read_gov_spending()
        .loc[lambda d: d.year == d.year.max()]
        .loc[lambda d: d.value >= 15]
        .country_name.unique()
    )

    # save as string separated by comma
    return ", ".join(countries)


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

    low_lower = {
        "low_lower_share": str(
            round(
                float(kn_dict["share_low_income"])
                + float(kn_dict["share_lower_middle_income"]),
                1,
            )
        )
    }

    kn_dict = kn_dict | low_lower

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


def section1_dynamic_text() -> None:
    """
    In 2020, low-income countries spent US$20.7 billion on health, a 4% increase
    from 2019. Lower-middle income countries spent US$325 billion on health,
    a 1% decrease from 2019. Health’s share of gross domestic product (GDP)
    increased across all income levels between 2019 and 2020, likely caused
    by the onset of the COVID-19 pandemic. More data is needed to know if this
     upward trend will continue beyond 2020.
    """
    numbers = {}
    data = (
        _read_spending()
        .pipe(_filter_indicator, indicator="Total spending ($US billion)")
        .pipe(_filter_income_data)
        .pipe(_reshape_vertical)
        .assign(change=lambda d: d.groupby(["entity"])["value"].pct_change())
    )

    data_total = data.pipe(_summarise_by_year)

    numbers["total_health_spending_2000"] = str(
        round(data_total.loc[data_total.year == 2000, "value"].sum() / 1e3, 1)
    )

    low_income = _spending_income(data, "Low income")
    lower_middle = _spending_income(data, "Lower middle income")

    numbers = numbers | low_income | lower_middle

    update_key_number(PATHS.output / "overview.json", new_dict=numbers)


def section2_dynamic_text() -> None:
    numbers = {}

    gdp = (
        _read_spending()
        .pipe(_filter_indicator, indicator="Share of GDP (%)")
        .drop(columns=["indicator"])
        .pipe(_reshape_vertical)
        .pipe(add_income_level_column, id_column="entity", id_type="regex")
        .assign(
            continent=lambda d: convert_id(
                d.entity, from_type="regex", to_type="continent"
            )
        )
        .loc[lambda d: d.continent == "Africa"]
        .loc[lambda d: d.value >= 5]
        .dropna(subset="income_level")
        .loc[lambda d: d.year == d.year.max()]
    )

    per_capita = (
        _read_spending()
        .pipe(_filter_indicator, indicator="Per capita spending ($US)")
        .drop(columns=["indicator"])
        .pipe(_reshape_vertical)
        .pipe(add_income_level_column, id_column="entity", id_type="regex")
        .assign(
            continent=lambda d: convert_id(
                d.entity, from_type="regex", to_type="continent"
            )
        )
        .loc[lambda d: d.continent == "Africa"]
        .loc[lambda d: d.value >= 86]
        .dropna(subset="income_level")
        .loc[lambda d: d.year == d.year.max()]
    )

    gdp_entities = set(gdp.entity.unique())
    per_capita_entities = set(per_capita.entity.unique())
    in_both = list(gdp_entities.intersection(per_capita_entities))

    data = pd.concat([gdp, per_capita], ignore_index=True)

    both = (
        data.loc[data.entity.isin(in_both)]
        .drop_duplicates(subset=["entity"])
        .groupby("income_level", as_index=False)
        .count()
    )

    low = both.query("income_level == 'Low income'")

    if len(low) == 0:
        numbers["low_income_target"] = 0

    lower_middle = both.query("income_level == 'Lower middle income'")

    numbers["lower_middle_target"] = str(lower_middle.entity.sum())

    update_key_number(PATHS.output / "overview.json", new_dict=numbers)


def section3_dynamic_text() -> None:
    spending = (
        _read_external()
        .filter(["year", "Country", "External Aid", "Domestic Government"], axis=1)
        .melt(id_vars=["year", "Country"])
        .loc[lambda d: d.year.isin([2000, d.year.max()])]
        .sort_values(by=["Country", "variable", "year"])
        .pipe(_filter_income_data_rows)
        .assign(
            difference=lambda d: d.groupby(
                ["Country", "variable"], as_index=False, group_keys=False
            ).value.diff()
        )
        .loc[lambda d: d.year == d.year.max()]
    )

    low_external, low_external_change = _change_direction(
        spending, "External Aid", "Low income"
    )

    low_domestic, low_domestic_change = _change_direction(
        spending, "Domestic Government", "Low income"
    )

    lmic_external, lmic_external_change = _change_direction(
        spending, "External Aid", "Lower middle income"
    )

    lmic_domestic, lmic_domestic_change = _change_direction(
        spending, "Domestic Government", "Lower middle income"
    )

    numbers = {
        "low_income_external_change": f"{abs(low_external)}",
        "low_income_external_direction": f"{low_external_change}",
        "lmic_external_change": f"{abs(lmic_external)}",
        "lmic_income_external_direction": f"{lmic_external_change}",
        "low_income_domestic_change": f"{abs(low_domestic)}",
        "low_income_domestic_direction": f"{low_domestic_change}",
        "lmic_domestic_change": f"{abs(lmic_domestic)}",
        "lmic_income_domestic_direction": f"{lmic_domestic_change}",
    }

    update_key_number(PATHS.output / "overview.json", new_dict=numbers)


if __name__ == "__main__":
    total_spending_sm()
    total_spending_by_income_bar()
    africa_spending_trend_line()
    section1_dynamic_text()
    section2_dynamic_text()
    section3_dynamic_text()
