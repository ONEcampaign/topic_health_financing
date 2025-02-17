""" """

import pandas as pd
import numpy as np
import country_converter as coco

from scripts.analysis.common import get_ghed_data, keep_relevant_groups
from scripts.analysis.aggregates import aggregate_per_capita, aggregate_pct_gdp_usd_const_2022, aggregate, aggregate_pct_gge_usd_const_2022, aggregate_pct_che_usd2022
from scripts.config import PATHS


def create_total_health_expenditure():
    """ """

    ghed = get_ghed_data()

    # Total health expenditure in constant 2022 USD
    tt = (ghed.loc[lambda d: d.indicator_code == 'che_usd2022', ['iso3_code', 'year', 'value']].reset_index(drop=True))
    ag_tt = aggregate(tt)
    tt_full = pd.concat([tt, ag_tt.rename(columns={'group':'iso3_code'})]).assign(unit = "USD constant (2022)")

    # Total health expenditure per capita in constant 2022 USD
    tt_pc = (ghed.loc[lambda d: d.indicator_code == 'che_usd2022_pc', ['iso3_code', 'year', 'value']].reset_index(drop=True))
    ag_tt_pc = aggregate_per_capita(ghed.loc[lambda d: d.indicator_code == 'che_usd2022', ['iso3_code', 'year', 'value']])
    tt_pc_full = pd.concat([tt_pc, ag_tt_pc.rename(columns={'group':'iso3_code'})]).assign(unit = "per capita, USD constant (2022)")

    # Total health expenditure as a percentage of GDP
    tt_gdp = ghed.loc[lambda d: d.indicator_code == 'che_gdp', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    ag_tt_gdp = aggregate_pct_gdp_usd_const_2022(ghed.loc[lambda d: d.indicator_code == 'che_usd2022', ['iso3_code', 'year', 'value']])
    tt_gdp_full = pd.concat([tt_gdp, ag_tt_gdp.rename(columns={'group':'iso3_code'})]).assign(unit = "percent of GDP")

    df = (pd.concat([tt_full,tt_pc_full,tt_gdp_full])
            .assign(entity_name=lambda d: coco.convert(d.iso3_code, to='name_short', not_found=None))
            .assign(iso3_code = lambda d: coco.convert(d.iso3_code, src='ISO3', to='ISO3', not_found=np.nan))
            )

    return df
    # df.to_csv(PATHS.output / "total_health_expenditure", index=False)


def create_gov_expenditure():
    """Data with aggregates as percent of general government expenditure, include constant USD values and total government expenditure values"""

    ghed = get_ghed_data()

    # gov expenditure as a percent of total government expenditure
    gov = ghed.loc[lambda d: d.indicator_code == 'gghed_gge', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    gov_agg = aggregate_pct_gge_usd_const_2022(ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']])
    gov_full = pd.concat([gov, gov_agg.rename(columns={'group':'iso3_code'})]).assign(unit = "percent of general government expenditure")

    # gov expenditure in constant 2022 USD
    gov_usd = ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    gov_usd_agg = aggregate(ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']])
    gov_usd_full = pd.concat([gov_usd, gov_usd_agg.rename(columns={'group':'iso3_code'})]).assign(unit = "USD constant (2022)")

    #gov expenditure as a percent of GDP
    gov_gdp = ghed.loc[lambda d: d.indicator_code == 'gghed_gdp', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    gov_gdp_agg = aggregate_pct_gdp_usd_const_2022(ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']])
    gov_gdp_full = pd.concat([gov_gdp, gov_gdp_agg.rename(columns={'group':'iso3_code'})]).assign(unit = "percent of GDP")

    # gov expenditure per capita
    gov_pc = ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022_pc', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    gov_pc_agg = aggregate_per_capita(ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']])
    gov_pc_full = pd.concat([gov_pc, gov_pc_agg.rename(columns={'group':'iso3_code'})]).assign(unit = "per capita, USD constant (2022)")

    return (pd.concat([gov_full, gov_usd_full, gov_gdp_full, gov_pc_full])
            .assign(entity_name=lambda d: coco.convert(d.iso3_code, to='name_short', not_found=None))
            .assign(iso3_code = lambda d: coco.convert(d.iso3_code, src='ISO3', to='ISO3', not_found=np.nan))
            )


def calculate_pvt_excl_oop_usd2022(ghed: pd.DataFrame):
    """ """

    return (ghed.loc[lambda d: d.indicator_code.isin(["fs4_usd2022", "fs5_usd2022", "fs6_usd2022", "fsnec_usd2022", "fs61_usd2022"])]
     .pivot(index=["iso3_code", "year"], columns = 'indicator_code', values='value')
     .dropna(how="all")
     .fillna(0)
     .assign(value = lambda d: d.fs4_usd2022 + d.fs5_usd2022 + d.fs6_usd2022 + d.fsnec_usd2022 - d.fs61_usd2022,
             )
     .loc[:, ["value"]]
     .reset_index()
     )


def calculate_pvt_excl_oop_percent_che(ghed: pd.DataFrame):
    """ """

    che = (ghed
           .loc[lambda d: d.indicator_code.isin(["hf1", "hf2", "hf3", "hf4", "hfnec"])]
           .pivot(index=["iso3_code", "year"], columns = 'indicator_code', values='value')
           .dropna(how="all")
           .fillna(0)
           .assign(che = lambda d: d.hf1 + d.hf2 + d.hf3 + d.hf4 + d.hfnec)
           .loc[:, ['che']]
           .reset_index()
           )

    pvtd_excl_oop = (ghed.loc[lambda d: d.indicator_code.isin(["fs4", "fs5", "fs6", "fsnec", "fs61"])]
                     .pivot(index=["iso3_code", "year"], columns = 'indicator_code', values='value')
                     .dropna(how="all")
                     .fillna(0)
                     .assign(value = lambda d: d.fs4 + d.fs5 + d.fs6 + d.fsnec - d.fs61,
                             )
                     .loc[:, ["value"]]
                     .reset_index())

    return (pd.merge(pvtd_excl_oop, che, on = ['iso3_code', 'year'], how='left')
                         .assign(value = lambda d: (d.value/d.che)*100)
                         .drop(columns = ['che'])
                         )


def create_expenditure_by_source():
    """External, domestic gov, OOP, private excl OOP"""

    ghed = get_ghed_data()

    sources = {"gov": "Domestic government",
               "ext": "External",
               "pvt": "Other private",
                "oop": "Out-of-pocket"
               }

    # 1. Sources as shares of total health expenditure

    # gov expenditure as a percent of total government expenditure
    gov = ghed.loc[lambda d: d.indicator_code == 'gghed_che', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    gov_agg = aggregate_pct_che_usd2022(ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']])
    gov_full = (pd.concat([gov, gov_agg.rename(columns={'group':'iso3_code'})])
                .assign(unit = "percent of health expenditure",
                        source = sources['gov']
                        )
                )

    # external expenditure as a percent of total government expenditure
    ext = ghed.loc[lambda d: d.indicator_code == 'ext_che', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    ext_agg = aggregate_pct_che_usd2022(ghed.loc[lambda d: d.indicator_code == 'ext_usd2022', ['iso3_code', 'year', 'value']])
    ext_full = (pd.concat([ext, ext_agg.rename(columns={'group':'iso3_code'})])
                .assign(unit = "percent of health expenditure",
                        source = sources['ext']
                        )
                )

    # other private - need to calculate private excluding oop first
    pvt = calculate_pvt_excl_oop_percent_che(ghed)
    pvt_agg = aggregate_pct_che_usd2022(calculate_pvt_excl_oop_usd2022(ghed))
    pvt_full = (pd.concat([pvt, pvt_agg.rename(columns={'group':'iso3_code'})])
                .assign(unit = "percent of health expenditure",
                        source = sources['pvt']
                        )
                )

    # out-of-pocket expenditure as a percent of total government expenditure, using indicator hf3
    oop = ghed.loc[lambda d: d.indicator_code == 'hf3_che', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    oop_agg = aggregate_pct_che_usd2022(ghed.loc[lambda d: d.indicator_code == 'hf3_usd2022', ['iso3_code', 'year', 'value']])
    oop_full = (pd.concat([oop, oop_agg.rename(columns={'group':'iso3_code'})])
                .assign(unit = "percent of health expenditure",
                        source = sources['oop']
                        )
                )

    # 2. Sources in constant USD

    # gov expenditure in constant 2022 USD
    gov_usd = ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    gov_agg_usd = aggregate(ghed.loc[lambda d: d.indicator_code == 'gghed_usd2022', ['iso3_code', 'year', 'value']])
    gov_full_usd = (pd.concat([gov_usd, gov_agg_usd.rename(columns={'group':'iso3_code'})])
                .assign(unit = "constant USD (2022)",
                        source = sources['gov']
                        )
                )

    # external expenditure as a percent of total government expenditure
    ext_usd = ghed.loc[lambda d: d.indicator_code == 'ext_usd2022', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    ext_agg_usd = aggregate(ghed.loc[lambda d: d.indicator_code == 'ext_usd2022', ['iso3_code', 'year', 'value']])
    ext_full_usd = (pd.concat([ext_usd, ext_agg_usd.rename(columns={'group':'iso3_code'})])
                .assign(unit = "constant USD (2022)",
                        source = sources['ext']
                        )
                )

    # other private - need to calculate private excluding oop first
    pvt_usd = calculate_pvt_excl_oop_usd2022(ghed)
    pvt_agg_usd = aggregate(calculate_pvt_excl_oop_usd2022(ghed))
    pvt_full_usd = (pd.concat([pvt_usd, pvt_agg_usd.rename(columns={'group':'iso3_code'})])
                .assign(unit = "constant USD (2022)",
                        source = sources['pvt']
                        )
                )

    # out-of-pocket expenditure as a percent of total government expenditure, using indicator hf3
    oop_usd = ghed.loc[lambda d: d.indicator_code == 'hf3_usd2022', ['iso3_code', 'year', 'value']].reset_index(drop=True)
    oop_agg_usd = aggregate(ghed.loc[lambda d: d.indicator_code == 'hf3_usd2022', ['iso3_code', 'year', 'value']])
    oop_full_usd = (pd.concat([oop_usd, oop_agg_usd.rename(columns={'group':'iso3_code'})])
                .assign(unit = "constant USD (2022)",
                        source = sources['oop']
                        )
                )

    return (pd.concat([gov_full, ext_full, pvt_full, oop_full,
                       gov_full_usd, ext_full_usd, pvt_full_usd, oop_full_usd])
            .assign(entity_name=lambda d: coco.convert(d.iso3_code, to='name_short', not_found=None))
            .assign(iso3_code = lambda d: coco.convert(d.iso3_code, src='ISO3', to='ISO3', not_found=np.nan))
            )



def create_expenditure_by_condition():
    """ """

    ghed = get_ghed_data()

    dis_indicators = {"dis11": "HIV/AIDS and other STDs",
                      "dis12": "Tuberculosis",
                      "dis13": "Malaria",
                      # "dis16": "Neglected Tropical Diseases",
                      # "dis192": "COVID-19",
                      "dis21": "Maternal health",
                      "dis23": "Family planning",
                      "dis3": "Nutritional deficiencies",
                      "dis4": "Noncommunicable diseases",
                      "dis5": "Injuries",
                      # "disnec": "Other diseases and conditions"
                      }

    sources = {"": "total",
               "ext_": "External",
               "gghed_": "Domestic government",
               "pvtd_": "Private and out-of-pocket"}

    df = pd.DataFrame()

    for source_code, source_name in sources.items():

        for k,v in dis_indicators.items():
            dis = (ghed
                   .loc[lambda d: d.indicator_code == f"{k}_{source_code}usd2022", ['iso3_code', 'year', 'value']]
                   .reset_index(drop=True)
                   .assign(condition = v,
                           source=source_name)
                   )

            # aggregates may not be generated because of extensive missing data for these breakdowns
            # dis_agg = aggregate(ghed.loc[lambda d: d.indicator_code == f"{k}_{source_code}usd2022", ['iso3_code', 'year', 'value']])
            # dis_full = pd.concat([dis, dis_agg.rename(columns={"group": "iso3_code"})], ignore_index=True).assign(condition = f"{v}", source=source_name)

            df = pd.concat([df, dis], ignore_index=True)

    return (df
            .assign(entity_name=lambda d: coco.convert(d.iso3_code, to='name_short', not_found=None))
            .assign(iso3_code = lambda d: coco.convert(d.iso3_code, src='ISO3', to='ISO3', not_found=np.nan))
            )



if __name__ == "__main__":

    (create_total_health_expenditure()
     .pipe(keep_relevant_groups)
     .to_csv(PATHS.output / "total_health_expenditure.csv", index=False))
    create_gov_expenditure().pipe(keep_relevant_groups).to_csv(PATHS.output / "gov_expenditure.csv", index=False)
    create_expenditure_by_source().pipe(keep_relevant_groups).to_csv(PATHS.output / "expenditure_by_source.csv", index=False)
    create_expenditure_by_condition().pipe(keep_relevant_groups).to_csv(PATHS.output / "expenditure_by_condition.csv", index=False)




