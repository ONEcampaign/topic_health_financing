# Health Financing topic page

This repository contains the research powering the Health Financing topic page. The 
analysis relies on the WHO Global Health Expenditure Database (GHED), which provides
comprehensive national level data on health expenditure. Other values and country groupings are
calculated based on this data

## Requirements
To run the analysis Python >=3.10 is required. Dependencies are managed by poetry 
and can be installed by running `poetry install`.

## Repository structure
The repository is structured as follows:
- scripts: Contains the scripts used to download and process the data, and create the visualizations for the page
- raw_data: Contains the raw data downloaded from GHED and other sources (not trakced by git)
- output: Contains the processed data and visualizations

## Methodology
The analysis is based on the WHO Global Health Expenditure Database (GHED) 
which provides comprehensive national level data on health expenditure. National level values are
displayed as reported by GHED. The analysis also generates estimates for regional and other groupings, and calculates
other indicators based on the GHED data.

__1. Regional and other groupings__

Aggregated values for regions and other country groupings are calculated based on national data reported in GHED.
The analysis generates estimated values for the following regions and groupings:
- Income level: Low income, Lower middle income, Upper middle income, High income based on World Bank classification
- "Africa" and "Africa (Low and lower middle income" based on UN region classification and World Bank income classification

Aggregates are only calculated where data is adequately complete, based on the following criteria:
Aggregates are calculated for each year where at least 95% of countries belonging to the grouping have data available, 
after forward filling missing values up to 2 years. Consideration is made for the year of establishment of a country,
e.g. South Sudan is not considered in aggregates for Africa before 2011.
Income level aggregates are generated based on the most recent income classification, and this 
classification is maintained for all years in the data, regardless of changes in classification.
Aggregate values are made up to 2022, as 2023 reported values are preliminary where available 

__2. Indicators__

The analysis caluculates the following indicators not reported in GHED:
`Domestic private health expenditure excluding out-of-pocket expenditure` is calculated as the difference between total private health expenditure and out-of-pocket expenditure.
Total Domeatic Private Expenditure is made up of compulsory prepayment other, and unspecified, than FS.3 (FS.4), voluntary prepayment (FS.5), other
domestic revenues (FS.6) and unspecified revenues of health care financing schemes (FS.nec). From this FS.61, out-of-pocket expenditure (OR "Revenues from households")
is subtracted to get the value of Domestic private health expenditure excluding out-of-pocket expenditure.
This indicator is calculated for all units used in the analysis including US$ 2022 constant prices, and as a percent of total health expenditure.

__Other Notes__

US$ values are shows in 2022 constant prices to allow comparisons among countries and over time.
GDP values and population values used for example as "% of GDP" or "per capita", are derived from the IMF
World Economic Outlook and UN Population Division respectively, retrieved from the GHED release. 



