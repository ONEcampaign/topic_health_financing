""" """

import pandas as pd
import numpy as np
import country_converter as coco
from bblocks.dataframe_tools.add import add_income_level_column

from scripts.analysis.common import custom_sort, format_large_numbers
from scripts.config import PATHS


# Section 1

def chart_1_1():
    """Total health expenditure - total usd const, per capita, percent of GDP

    """

    df = pd.read_csv(PATHS.output / "total_health_expenditure.csv")


    # save data for download
    df.to_csv(PATHS.output / "section_1_1_download.csv", index=False)

    # format data for chart
    (df
     .assign(value = lambda d: np.where(d.unit == "USD constant (2022)", d.value/1e9, d.value))
     .assign(unit = lambda d: d.unit.map({"USD constant (2022)": "Total (USD billions)",
                                          "per capita, USD constant (2022)": "Per capita (US$)",
                                          "percent of GDP": "Percent of GDP"}),
             unit_annotate = lambda d: d.unit.map({"Total (USD billions)": "billion US$",
                                                   "Per capita (USD)": "US$ per capita",
                                                   "Percent of GDP": "% of GDP"}),

             )
     .pivot(index=['year', 'unit', "unit_annotate"], columns='entity_name', values="value")
     .reset_index()
     .pipe(custom_sort, 'unit', ["Total (US$ billions)", "Per capita (US$)", "Percent of GDP"])
     .to_csv(PATHS.output / "section_1_1_chart.csv", index=False)
     )


def chart_1_2():
    """Government health spending as a percentage of total government expenditure"""

    df = (pd.read_csv(PATHS.output / "gov_expenditure.csv")
    .dropna(subset='value')
    .loc[lambda d: d.unit.isin(['USD constant (2022)', 'percent of general government expenditure'])]
          )

    # save data
    df.to_csv(PATHS.output / "section_1_2_download.csv", index=False)

    #create chart
    (df
     .pivot(index = ['year', 'entity_name'], columns='unit', values='value')
     .rename(columns = {'USD constant (2022)': "total",
                        'percent of general government expenditure': "share_of_gov"})
     .reset_index()
     .assign(total = lambda d: format_large_numbers(d.total))
     .pivot(index=['year', 'total'], columns='entity_name', values='share_of_gov')
     .reset_index()
     .to_csv(PATHS.output / "section_1_2_chart.csv", index=False)
     )


def chart_2_1():
    """ """

    df =  (pd.read_csv(PATHS.output / "gov_expenditure.csv")
           .loc[lambda d: (d.iso3_code.notna())&(d.unit == "percent of GDP")]
           .assign(continent = lambda d: coco.convert(d.iso3_code, src="ISO3", to="continent"))
           .pipe(add_income_level_column, "iso3_code", "ISO3")
           )

    # save data
    df.to_csv(PATHS.output / "section_2_1_download.csv", index=False)

    (df
     .assign(continent = lambda d: np.where(d.continent == 'Africa', "Africa", "Other"))
     .loc[lambda d: ~d.income_level.isin(['Not classified', np.nan])]
     .loc[lambda d: d.year <= 2022]
     .sort_values(by="year", ascending=False)
     .pipe(custom_sort, "income_level", ["Low income", "Lower middle income", "Upper middle income", "High income"])

     .reset_index(drop=True)
     .assign(colour = lambda d: np.where(d.continent == "Africa", "Africa", None))

     .to_csv(PATHS.output / "section_2_1_chart.csv", index=False)
     )

def chart_2_2():
    """ """

    df =  (pd.read_csv(PATHS.output / "gov_expenditure.csv")
           .loc[lambda d: (d.iso3_code.notna())&(d.unit == "per capita, USD constant (2022)")]
           .assign(continent = lambda d: coco.convert(d.iso3_code, src="ISO3", to="continent"))
           .pipe(add_income_level_column, "iso3_code", "ISO3")
           )

    # save data
    df.to_csv(PATHS.output / "section_2_2_download.csv", index=False)


    (df
     .assign(continent = lambda d: np.where(d.continent == 'Africa', "Africa", "Other"))
     .loc[lambda d: ~d.income_level.isin(['Not classified', np.nan])]
     .loc[lambda d: d.year <= 2022]
     .sort_values(by="year", ascending=False)
     .pipe(custom_sort, "income_level", ["Low income", "Lower middle income", "Upper middle income", "High income"])

     .reset_index(drop=True)
     .assign(colour = lambda d: np.where(d.continent == "Africa", "Africa", None))

     .to_csv(PATHS.output / "section_2_2_chart.csv", index=False)

     )


def chart_2_3():
    """Abuja"""

    df = (pd.read_csv(PATHS.output / "gov_expenditure.csv")
          .loc[lambda d: (d.iso3_code.notna())&(d.unit == "percent of general government expenditure")]
          .assign(continent = lambda d: coco.convert(d.iso3_code, src="ISO3", to="continent"))
          .loc[lambda d: d.continent == "Africa"]
          .pipe(add_income_level_column, "iso3_code", "ISO3")
          )

    # save data
    df.to_csv(PATHS.output / "section_2_3_download.csv", index=False)

    # create chart
    (df.loc[lambda d: d.year <= 2022]
     .sort_values(by="year", ascending=False)
     .pipe(custom_sort, "income_level", ["Low income", "Lower middle income", "Upper middle income", "High income"])
     .reset_index(drop=True)
     .to_csv(PATHS.output / "section_2_3_chart.csv", index=False)
     )

def chart_3_1():
    """ """

    df = (pd.read_csv(PATHS.output / "expenditure_by_source.csv"))

    # save data
    df.to_csv(PATHS.output / "section_3_1_download.csv", index=False)

    # create chart
    (df
     .dropna(subset='value')
     .assign(value = lambda d: format_large_numbers(d.value))
     .pivot(index=['year', 'entity_name', "source"], columns="unit", values='value')
     .reset_index()
     .pivot(index=['year', 'entity_name', "constant USD (2022)"], columns='source', values="percent of health expenditure")
     .reset_index()

     .pipe(custom_sort, "entity_name", ["Africa (Low and lower middle income)", "Africa", "Low income", "Lower middle income", "Upper middle income", "High income"])
     .reset_index(drop=True)
     .to_csv(PATHS.output / "section_3_1_chart.csv", index=False)
     )


def chart_4_1():
    """ """

    df = (pd.read_csv(PATHS.output / "expenditure_by_condition.csv")
          .loc[lambda d: d.source=="total"]
          .dropna(subset='value')
          )

    # save data
    df.to_csv(PATHS.output / "section_4_1_download.csv", index=False)

    # create chart
    (df
     .assign(value_annotation = lambda d: format_large_numbers(d.value))
     .pivot(index=['year', 'entity_name', 'value_annotation'], columns="condition", values='value')
     .reset_index()
     .sort_values(by='entity_name')
     .to_csv(PATHS.output / "section_4_1_chart.csv", index=False)
     )



def chart_4_2():
    """ """

    df = (pd.read_csv(PATHS.output / "expenditure_by_condition.csv")
    .dropna(subset="value")
    .loc[lambda d: d.source!="total"]
    .loc[lambda d: d.year == d.groupby("entity_name").year.transform("max")]

    )

    # save data
    df.to_csv(PATHS.output / "section_4_2_download.csv", index=False)

    # create chart
    (df
     .assign(value_annotation = lambda d: format_large_numbers(d.value, other_dec=0))
     .sort_values(by="entity_name")
     .to_csv(PATHS.output / "section_4_2_chart.csv", index=False)
     )





def chart_into_2():
    """ """

    df = pd.read_csv(PATHS.output / "total_health_expenditure.csv")

    (df
     .loc[lambda d: (d.year == 2022)
                    & (d.entity_name.isin(['Low income', "Lower middle income", "Upper middle income", "High income"]))
                    & (d.unit == "USD constant (2022)")
     ]
     .assign(share = lambda d: d.value/d.value.sum()*100)
     .to_csv(PATHS.output / "section_into_2_chart.csv", index=False)
     )



def chart_intro_3():
    """ """


    (pd.read_csv(PATHS.output / "total_health_expenditure.csv")
     .loc[lambda d:(d.entity_name == "Africa")
                   & (d.unit == "USD constant (2022)")
     ]
    .assign(value_annotation = lambda d: format_large_numbers(d.value))
     .to_csv(PATHS.output / "section_intro_3_chart.csv", index=False)
     )



if __name__ == "__main__":

    chart_1_1()
    chart_1_2()

    chart_2_1()
    chart_2_2()
    chart_2_3()

    chart_3_1()

    chart_4_1()
    chart_4_2()

    chart_into_2()
    chart_intro_3()










