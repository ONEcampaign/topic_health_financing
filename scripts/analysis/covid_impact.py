from scripts.analysis.data_versions import read_spending_data_versions
import pandas as pd

from bblocks.analysis_tools.get import period_avg

from scripts.logger import logger
from scripts.tools import create_africa_agg, create_income_agg


def change_in_spending(
    data_type: str = "usd_constant_pc", aggregation: str = "median"
) -> pd.DataFrame:
    """Compare the spending levels, in real terms, with the average of the previous 2-3 years"""

    data = read_spending_data_versions("health_spending")

    df_constant = data[data_type]

    # 2017-2019 average
    df_avg = period_avg(
        df_constant,
        start_date="2017-01-01",
        end_date="2019-01-01",
        date_column="year",
        value_columns="value",
        group_by=["country_name", "iso_code"],
    )

    df_2020 = df_constant.query("year.dt.year == 2020")

    df = df_2020.merge(
        df_avg, on=["country_name", "iso_code"], suffixes=("_2020", "_avg_17_19")
    )

    africa = df.pipe(
        create_africa_agg,
        value_column=["value_2020", "value_avg_17_19"],
        method=aggregation,
    )
    income = df.pipe(
        create_income_agg,
        value_column=["value_2020", "value_avg_17_19"],
        method=aggregation,
    )

    df = pd.concat([africa, income], ignore_index=True).filter(
        ["country_name", "indicator_code", "value_2020", "value_avg_17_19", "units"],
        axis=1,
    )

    return df


def predict_2020(d_: pd.DataFrame) -> pd.DataFrame:
    from sklearn.linear_model import LinearRegression

    model = LinearRegression()
    x = d_.year.values.reshape(-1, 1)
    y = d_.value.values.reshape(-1, 1)

    try:
        model.fit(x, y)

        # predict 2020
        x_predict = pd.DataFrame({"year": [2020]})
        y_predict = model.predict(x_predict.year.values.reshape(-1, 1))
    except ValueError:
        logger.info(f"Not enough data for {d_.iso_code.unique()} to predict 2020")
        return d_

    # add 2020 predicted data to the original data
    data = d_.loc[lambda d: d.year == 2019].assign(value=y_predict[0][0], year=2020)

    return pd.concat([d_, data], ignore_index=True)


def spending_business_as_usual():
    """for every country, project what spending would have been for 2020 if the trends from
    2017-2019 had continued"""

    # get the spending time series
    data = read_spending_data_versions("health_spending")
    df = (
        data["usd_constant"].assign(year=lambda x: x.year.dt.year).query("year <= 2020")
    )

    # Create a liner projection of spending for 2020. Start by using 5 years of data
    df_5yr = df.query("year >= 2015 and year < 2020")

    # create a projection for each country
    df_5yrp = (
        df_5yr.groupby(["country_name", "iso_code"], as_index=False)
        .apply(predict_2020)
        .reset_index(drop=True)
        .rename(columns={"value": "value_predicted"})
        .query("year >= 2019")
    )

    df_area = df.rename(columns={"value": "value_area"}).query("year >2019")

    df = (
        df.merge(df_5yrp, on=[c for c in df.columns if c != "value"], how="left")
        .merge(df_area, on=[c for c in df.columns if c != "value"], how="left")
        .query("year >=2015")
    )

    df.to_clipboard(index=False)
