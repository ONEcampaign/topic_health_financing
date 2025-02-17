""" """

import pandas as pd
import numpy as np
import country_converter as coco
from bblocks.dataframe_tools.add import add_income_level_column

from scripts.analysis.common import add_pop, add_gge_usd_const_2022, add_gdp_usd_curr, add_gdp_usd_const_2022, add_che_usd2022



def expand_df(df):
    """ """

    return (df
            .set_index(["iso3_code", "year"])
            .pipe(lambda d: d.reindex(pd.MultiIndex.from_product([d.index.get_level_values("iso3_code").unique(), d.index.get_level_values("year").unique()],names=["iso3_code", "year"])))
            .reset_index()
            )

def ffill_df(df):
    """ """

    return(df
           .sort_values(["iso3_code", "year"])
           .pipe(lambda d: d.assign(**d.groupby("iso3_code").ffill(limit=2)))
           )

def add_africa_low_middle_income(df, col_name = "group"):
    """Add a group for Africa low and lower middle income countries"""
    afr_df = (df
              .pipe(add_income_level_column, "iso3_code", 'ISO3')
              .assign(continent = lambda d: coco.convert(d.iso3_code, src="ISO3", to="continent"))
              .loc[lambda d: (d.continent == "Africa") & (d.income_level.isin(["Low income", "Lower middle income"]))]
              .drop(columns = ['continent', 'income_level'])
              .assign(**{col_name: 'Africa (Low and lower middle income)'})
              )

    return pd.concat([df, afr_df], ignore_index=True)

def add_group(df, group):
    """ """

    if group == "continent":
        return (df
                .assign(group = lambda d: coco.convert(d.iso3_code, src="ISO3", to='continent'))
                .pipe(add_africa_low_middle_income, col_name = "group")
                )

    elif group == "income_level":
        return (df
                .pipe(add_income_level_column, "iso3_code", 'ISO3', target_column= 'group')
                )

    else:
        raise ValueError(f"Invalid group: {group}")

def filter_threshold(df, threshold=0.95):
    """

    df: expanded dataframe

    """

    #special countries that did not exist in some years - South Sudan, Timor-leste
    # dictionary of the iso3 code and the year it was created

    spcial_iso = {"SSD": 2011, "TSL": 2002} # these iso codes should only be included from the year they were created

    total_count= (df
                  [['iso3_code', "group", "year"]]
                          # remove the special iso codes before the year they were created
                          .loc[lambda d: ~(((d.iso3_code=="SSD") & (d.year < 2011))
                                           | ((d.iso3_code=="TSL") & (d.year < 2002)))
                          ]

                  .groupby(["group", 'year'])
                  .agg("count")['iso3_code']
                  .reset_index()
                  .rename(columns={"iso3_code": "total_count"})
                  )

    return (df
            .dropna(subset="value")
            .groupby(["group", 'year'])
            .agg({"iso3_code": "count"})
            .reset_index()
            .merge(total_count, on=["group", 'year'], how='left')
            # .assign(total_count = lambda d: d["group"].map(total_count_mapper))
            .assign(completion = lambda d: d.iso3_code/d.total_count)
            .loc[lambda d: d.completion>= threshold, ["group", 'year']]
            .merge(df, on=["group", 'year'], how='left')
            .reset_index(drop=True)
            )

def agg(df):
    """ """

    return (df
            .groupby(["group", 'year'])
            .agg({"value": 'sum'})
            .reset_index()
            )


def agg_proportion(df, denominator_col):
    """ """

    return (df
            .groupby(["group", 'year'])
            .agg({"value": 'sum', denominator_col: 'sum'})
            .assign(value = lambda d: d.value/d[denominator_col])
            .reset_index()
            )


def aggregate(df, continent=True, income_level=True):
    """ """

    # if both continent and income_level are False, raise and error
    if not continent and not income_level:
        raise ValueError("At least one of continent or income_level must be True")

    if continent:
        cont_df = (df
                .pipe(expand_df)
                .pipe(ffill_df)
                .pipe(add_group, "continent")
                .pipe(filter_threshold)
                .pipe(agg)
                )
    else:
        cont_df = pd.DataFrame()

    if income_level:
        income_df = (df
                     .pipe(expand_df)
                     .pipe(ffill_df)
                     .pipe(add_group, "income_level")
                     .pipe(filter_threshold)
                     .pipe(agg)
                     )
    else:
        income_df = pd.DataFrame()

    return pd.concat([cont_df, income_df], ignore_index=True).loc[lambda d: d.year <= 2022]


def aggregate_proportion(df, proportion_funct, denominator_col, *, continent=True, income_level=True):
    """ """

    # if both continent and income_level are False, raise and error
    if not continent and not income_level:
        raise ValueError("At least one of continent or income_level must be True")

    if continent:
        cont_df = (df
                .pipe(expand_df)
                .pipe(ffill_df)
                .pipe(add_group, "continent")
                .pipe(filter_threshold)
                   .pipe(proportion_funct) # add the denominator column
                   .pipe(agg_proportion, denominator_col=denominator_col)
                   .drop(columns=[denominator_col])

                )
    else:
        cont_df = pd.DataFrame()

    if income_level:
        income_df = (df
                     .pipe(expand_df)
                     .pipe(ffill_df)
                     .pipe(add_group, "income_level")
                     .pipe(filter_threshold)
                     .pipe(proportion_funct) # add the denominator column
                     .pipe(agg_proportion, denominator_col=denominator_col)
                     .drop(columns=[denominator_col])
                     )
    else:
        income_df = pd.DataFrame()

    return pd.concat([cont_df, income_df], ignore_index=True).loc[lambda d: d.year <= 2022]


def aggregate_per_capita(df, *, continent=True, income_level=True):
    """ """

    return (df
            .pipe(aggregate_proportion, add_pop, "population")
            )

def aggregate_pct_gge_usd_const_2022(df, *, continent=True, income_level=True):
    """ """

    return (df
            .pipe(aggregate_proportion, add_gge_usd_const_2022, "gge_usd2022")
            .assign(value = lambda d: d.value*100)
            )

def aggregate_pct_che_usd2022(df, *, continent=True, income_level=True):
    """ """

    return (df
            .pipe(aggregate_proportion, add_che_usd2022, "che_usd2022")
            .assign(value = lambda d: d.value*100)
            )

def aggregate_pct_gdp_usd_curr(df, *, continent=True, income_level=True):
    """ """

    return (df
            .pipe(aggregate_proportion, add_gdp_usd_curr, "gdp_usd_curr")
            .assign(value = lambda d: d.value*100)
            )

def aggregate_pct_gdp_usd_const_2022(df, *, continent=True, income_level=True):
    """ """

    return (df
            .pipe(aggregate_proportion, add_gdp_usd_const_2022, "gdp_usd_const_2022")
            .assign(value = lambda d: d.value*100)
            )
