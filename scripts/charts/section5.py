"""
Analyze disease specific financing
"""

from functools import partial
import pandas as pd

from scripts import config
from scripts.logger import logger
from scripts.analysis.read_data_versions import read_spending_data_versions
from scripts.charts.common import (
    get_version,
    total_usd_spending
)

DISEASE = read_spending_data_versions(dataset_name="health_spending_by_disease")

get_spending_version: callable = partial(get_version, versions_dict=DISEASE)

indicators = {'type': {' Nutritional Deficiencies': 'Nutritional deficiencies',
                       'Noncommunicable Diseases (NCDs)': 'Noncommunicable diseases',
                       'Injuries': 'Injuries',
                       'Other and unspecified Diseases and Conditions': 'Other diseases and conditions'},
              'subtype': {
                  'HIV/AIDS and Sexually Transmitted Diseases (STDs)': 'HIV/AIDS and other STDs',
                  'Tuberculosis (TB)': 'Tuberculosis',
                  'Malaria': 'Malaria',
                  'Maternal Conditions': 'Maternal conditions',
                  'Contraceptive Management (Family Planning)': 'Family planning'}
              }


def get_disease_spending():
    """Get disease specific spending data in USD constant values for specific diseases"""
    return (get_spending_version(version="usd_constant",
                                 additional_cols=['source', 'type', 'subtype'])
    # keep only the relevant diseases and contitions
            .loc[lambda d: ((d.subtype.isin(indicators['subtype'].keys()))
                            | ((d.type.isin(indicators['type'])) & (d.subtype.isna()))
                            )]
            .assign(disease=lambda d: d.subtype.fillna(d['type'])
                    .replace({**indicators['type'], **indicators['subtype']}))
            .loc[:,
            ['iso_code', 'year', 'disease', 'value', 'income_group', 'country_name', 'source']]
    # calculate aggregates
            .pipe(total_usd_spending, additional_grouper=['disease', 'source'], factor=1e6)
            )


def chart_5_1(spending: pd.DataFrame) -> None:
    """Create line chart for disease specific spending and save to output folder

    Args:
        spending (pd.DataFrame): spending data
    """

    df = (spending
            .loc[spending.source == 'total']
            .drop(columns=['source', 'indicator'])
            .melt(id_vars=['year', 'disease'])
            .dropna(subset='value')
            .pivot(index=['year', 'series'], columns='disease', values='value')
            .reset_index()
            )

    df.to_csv(config.PATHS.output / "section5_chart1.csv", index=False)
    logger.info("Saved section5_chart1.csv to output folder")


def chart_5_2(spending: pd.DataFrame) -> None:
    """Create Sankey chart for disease specific spending and save to output folder

    Args:
        spending (pd.DataFrame): spending data
    """

    df = (spending
            .loc[spending.source != 'total'] # exclude total spending
            .drop(columns=['indicator']) # drop indicator column
            .melt(id_vars=['year', 'disease', 'source'])
            .dropna(subset='value') # drop rows with missing values
            .rename(columns={'series': 'country'})
           # keep only latest values
            .assign(year=lambda d: d.year.dt.year)
            .loc[lambda d: d.groupby(['disease', 'country', 'source']).year.idxmax()]
            .assign(source=lambda d: d.source.replace({'external': 'External aid',
                                                       'domestic private': 'Out-of-pocket & other private *',
                                                       'domestic general government': 'Domestic government'}))
            # custom sort values based on disease
            .assign(disease=lambda d: pd.Categorical(d.disease,
                                                     categories=['HIV/AIDS and other STDs',
                                                                 'Tuberculosis','Malaria',
                                                                 'Maternal conditions', 'Family planning', 'Noncommunicable diseases', 'Injuries', 'Nutritional deficiencies',
                                                                            'Other diseases and conditions'])
                    )
            .sort_values(['disease', 'country', 'source'])
            .reset_index(drop=True)
            )

    df.to_csv(config.PATHS.output / "section5_chart2.csv", index=False)
    logger.info("Saved section5_chart2.csv to output folder")


if __name__ == "__main__":
    spending = get_disease_spending() # get disease specific spending
    chart_5_1(spending) # create line chart
    chart_5_2(spending) # create sankey chart
    logger.debug("Successfully created charts for section 5")

