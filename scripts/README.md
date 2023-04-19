# Charts

This module contains a series of scripts and sub-modules to download, process, and analyse data from the GHED database
and the OECD DAC CRS. Additionally, it can download population data from the UN World Population Prospects, and income
level information from the World Bank.

## config.py
This file contains the main configuration variables, including the constant base years, the years under analysis, etc.
Additionally, it defines the paths to different folders for this project.

## tools.py
This file contains helper functions to conduct the analysis and transformations for the topic page.

The script includes several main functions:

1. `get_weo_indicator`: Fetches a single economic indicator from the World Economic Outlook dataset.
2. `bn2units`: Converts billions to units.
3. `mn2units`: Converts millions to units.
4. `year2date`: Converts a year to a date object.
5. `lcu2gdp`: Converts Local Currency Units (LCUs) to share of GDP.
6. `value2pc`: Converts values to per capita figures.
7. `value2gov_spending_share`: Converts values to a percentage of government spending.
8. `value2gdp_share`: Converts values to a percentage of GDP.
9. `fill_gaps_by_group`: Fills missing values in a dataset using forward filling.
10. `add_total_counts_by_group`: Adds total counts for each group in a dataset.
11. `filter_by_threshold`: Filters a dataset based on a threshold.
12. `african_countries`: Returns a dictionary of African ISO codes and their names.
13. `income_levels`: Returns a dictionary of ISO codes and their corresponding income level.

## read_data.py
This file contains functions to read data from the ONE's MongoDB database.