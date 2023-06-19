"""
Analyze disease specific financing
"""

import json
from functools import partial

import pandas as pd

from scripts import config
from scripts.analysis.read_data_versions import read_spending_data_versions
from scripts.charts.common import (
    flag_africa,
    get_version,
    reorder_by_income,
    total_usd_spending
)

DISEASE = read_spending_data_versions(dataset_name="health_spending_by_disease")

get_spending_version: callable = partial(get_version, versions_dict=DISEASE)

indicators = {'type': {' Nutritional Deficiencies': 'nutritional deficiencies',
                       'Noncommunicable Diseases (NCDs)': 'noncommunicable diseases',
                       'Injuries': 'injuries',
                       'Other and unspecified Diseases and Conditions': 'other diseases and conditions'},
              'subtype': {
                  'HIV/AIDS and Sexually Transmitted Diseases (STDs)': 'HIV/AIDS and other STDs',
                  'Tuberculosis (TB)': 'Tuberculosis',
                  'Malaria': 'Malaria',
                  'Maternal Conditions': 'maternal conditions',
                  'Contraceptive Management (Family Planning)': 'family planning'}
              }


def get_disease_spending():
    """ """
    return (get_spending_version(version="usd_constant",
                                 additional_cols=['source', 'type', 'subtype'])
            .loc[lambda d: ((d.subtype.isin(indicators['subtype'].keys()))
                            | ((d.type.isin(indicators['type'])) & (d.subtype.isna()))
                            )]
            .assign(disease=lambda d: d.subtype.fillna(d['type'])
                    .replace({**indicators['type'], **indicators['subtype']}))
            .loc[:,
            ['iso_code', 'year', 'disease', 'value', 'income_group', 'country_name', 'source']]
            .pipe(total_usd_spending, additional_grouper=['disease', 'source'], factor=1e6)
            )


def chart_5_1(spending: pd.DataFrame):
    """ """

    return (spending
            .loc[spending.source == 'total']
            .drop(columns=['source', 'indicator'])
            .melt(id_vars=['year', 'disease'])
            .dropna(subset='value')
            .pivot(index=['year', 'series'], columns='disease', values='value')
            .reset_index()
            )


def chart_5_2(spending: pd.DataFrame):
    """ """

    return (spending
            .loc[spending.source != 'total']
            .drop(columns=['indicator'])
            .melt(id_vars=['year', 'disease', 'source'])
            .dropna(subset='value')
            .rename(columns={'series': 'country'})
            .assign(year=lambda d: d.year.dt.year)
            .loc[lambda d: d.groupby(['disease', 'country', 'source']).year.idxmax()]
            )
