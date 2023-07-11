"""
Disease specific financing charts
"""

from functools import partial
import pandas as pd

from scripts import config
from scripts.logger import logger
from scripts.analysis.read_data_versions import read_spending_data_versions
from scripts.charts.common import get_version, total_usd_spending

# Load a dictionary with dataframes for the different versions of "health_spending_by_disease" data
# These include: 'lcu', 'gdp_share','usd_current', 'usd_constant', 'usd_constant_pc'
DISEASE = read_spending_data_versions(dataset_name="health_spending_by_disease")

# Create a function to get a specific version. It returns a dataframe.
# Note that the `get_version` function, of which this is a partial implementation,
# handles some basic transformations of the data (like adding income levels or filtering
# out certain countries).
get_spending_version: callable = partial(get_version, versions_dict=DISEASE)

# define a global dict of indicators by type and subtype
INDICATORS: dict[str, dict] = {
    "type": {
        " Nutritional Deficiencies": "Nutritional deficiencies",
        "Noncommunicable Diseases (NCDs)": "Noncommunicable diseases",
        "Injuries": "Injuries",
        "Other and unspecified Diseases and Conditions": "Other diseases and conditions",
    },
    "subtype": {
        "HIV/AIDS and Sexually Transmitted Diseases (STDs)": "HIV/AIDS and other STDs",
        "Tuberculosis (TB)": "Tuberculosis",
        "Malaria": "Malaria",
        "Maternal Conditions": "Maternal conditions",
        "Contraceptive Management (Family Planning)": "Family planning",
    },
}


def get_disease_spending():
    """Get disease specific spending data in USD constant values for specific diseases."""
    return (
        get_spending_version(
            version="usd_constant", additional_cols=["source", "type", "subtype"]
        )
        # keep only the relevant diseases and conditions
        .loc[
            lambda d: (
                (d.subtype.isin(INDICATORS["subtype"]))
                | ((d.type.isin(INDICATORS["type"])) & (d.subtype.isna()))
            )
        ]
        .assign(
            disease=lambda d: d.subtype.fillna(d["type"]).replace(
                {**INDICATORS["type"], **INDICATORS["subtype"]}
            )
        )
        .filter(
            [
                "iso_code",
                "year",
                "disease",
                "value",
                "income_group",
                "country_name",
                "source",
            ],
        )
    )


def chart_5_1(spending: pd.DataFrame) -> None:
    """Create line chart for disease specific spending and save to output folder

    Args:
        spending (pd.DataFrame): spending data
    """

    df = (
        spending.loc[spending.source == "total"] # keep only total source
        .drop(columns=["source"])
        .dropna(subset="value")
        .pivot(index=["year", "country_name"], columns="disease", values="value")
        .reset_index()
    )

    df.to_csv(config.PATHS.output / "section5_chart1.csv", index=False)
    logger.info("Saved section5_chart1.csv to output folder")


def chart_5_2(spending: pd.DataFrame) -> None:
    """Create Sankey chart for disease specific spending and save to output folder

    Args:
        spending (pd.DataFrame): spending data
    """

    df = (
        spending.loc[spending.source != "total"]  # exclude total source
        #.melt(id_vars=["year", "disease", "source"])
        .dropna(subset="value")  # drop rows with missing values
        #.rename(columns={"series": "country"})
        # keep only latest values
        .assign(year=lambda d: d.year.dt.year)
        .loc[lambda d: d.groupby(["disease", "country_name", "source"]).year.idxmax()]
        # rename sources
        .assign(
            source=lambda d: d.source.replace(
                {
                    "external": "External aid",
                    "domestic private": "Out-of-pocket & other private *",
                    "domestic general government": "Domestic government",
                }
            )
        )
        # custom sort values based on disease
        .assign(
            disease=lambda d: pd.Categorical(
                d.disease,
                categories=[
                    "HIV/AIDS and other STDs",
                    "Tuberculosis",
                    "Malaria",
                    "Maternal conditions",
                    "Family planning",
                    "Noncommunicable diseases",
                    "Injuries",
                    "Nutritional deficiencies",
                    "Other diseases and conditions",
                ],
            )
        )
        .sort_values(["disease", "country_name", "source"])
        .reset_index(drop=True)
    )

    df.to_csv(config.PATHS.output / "section5_chart2.csv", index=False)
    logger.info("Saved section5_chart2.csv to output folder")


if __name__ == "__main__":
    spending_data = get_disease_spending()  # get disease specific spending
    chart_5_1(spending_data)  # create line chart
    chart_5_2(spending_data)  # create sankey chart
    logger.debug("Successfully created charts for section 5")
