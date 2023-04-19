# Charts

This module contains functions to create the data needed for charts and dynamic text for the topic page.

## common.py

This file contains helper functions to conduct the analysis and transformations for the topic page. Some of the key
functions included are:

- `_validate_additional_grouper()` -> list: Validates and converts the optional additional grouper
  provided as a string or a list.
- `_filter_african_countries()` -> pd.DataFrame: Assigns the name 'Africa' as 'country_name' to all
  African countries in the DataFrame.
- `per_capita_by_income()` -> pd.DataFrame: Calculates the per capita spending for each income group based on a
  DataFrame of spending data.
- `per_capita_africa()` ->
  pd.DataFrame: Calculates the per capita spending for Africa as a whole based on a DataFrame of spending data.
- `total_by_income()` -> pd.DataFrame:
  Calculates the total spending for each income group based on a DataFrame of spending data.
- `total_africa()` -> pd.DataFrame: Calculates the total spending for Africa as a whole based on a DataFrame of spending
  data.
- `combine_income_countries()` -> pd.DataFrame: Combines income, country, and Africa data into a single DataFrame.
- `get_version()` -> pd.DataFrame: Retrieves a specified version of the data, with optional
  additional columns, year filters, and other filters.
- `update_key_number()` -> None: Updates the key number in a JSON file with new values from a
  provided dictionary.
- `reorder_by_income()` -> pd.DataFrame: Reorders a DataFrame by income levels.
- `flag_africa()` -> pd.DataFrame: Identifies African countries in a DataFrame and flags non-African
  countries as 'Other'.
- `per_capita_spending()` -> pd.DataFrame: Calculates per capita spending for countries and groups.
- `total_usd_spending()` -> pd.DataFrame: Calculates total spending in
  constant USD for countries and groups.
- `spending_share_of_gdp()` -> pd.DataFrame: Calculates spending as a share of GDP for countries and groups.

## overview.py

This file contains functions to generate charts for the "Overview" section, including:

- total_spending_sm(): Creates a chart displaying total health spending in the latest year compared to the previous
  year.
- total_spending_by_income_bar(): Creates a bar chart displaying total health spending by income level for the latest
  year.
- africa_spending_trend_line(): Creates a line chart displaying the time series of total health spending in Africa.
- section1_dynamic_text(): Updates key numbers and generates dynamic text for Section 1.
- section2_dynamic_text(): Updates key numbers and generates dynamic text for Section 2.
- section3_dynamic_text(): Updates key numbers and generates dynamic text for Section 3.

- The functions in this file read data from different CSV files, process the data using Pandas, and save the results as
  new CSV files. Additionally, some functions update key numbers in a JSON file to be used for generating dynamic text.

**Helper Functions**

The file also contains several helper functions for data processing and transformation, including:

- Functions for reading data from CSV files: _read_spending, _read_gov_spending, _read_external, _read_mbd
- Functions for filtering data: _filter_latest2y, _filter_indicator, _filter_income_data, _filter_income_data_rows, _
  filter_africa_data
- Functions for reshaping data: _reshape_vertical
- Functions for summarizing data: _summarise_by_year, _calculate_pct_change, _spending_income
- Functions for generating key numbers and dictionaries: _change_direction, abuja_count, as_smt_dict_total,
  as_smt_dict_shares, as_smt_df
- Functions for updating key numbers in the JSON file: update_key_number

## Section 1: Health Spending Analysis

This section focuses on analyzing health spending data across countries, considering per capita spending, total
spending, share of GDP, and share of government spending.

This script contains the following functions:

- clean_chart(data: pd.DataFrame) -> pd.DataFrame: Sorts and cleans data for the chart.
- spending_share_of_government(usd_constant_data: pd.DataFrame) -> pd.DataFrame: Calculates spending as a share of
  government spending for countries and groups.
- chart1_1_pipeline() -> None: Processes various data sources related to health spending and generates a combined
  dataset that includes information on per capita spending, total spending, share of GDP, and share of government
  spending. The resulting dataset is saved in CSV format to a specified file location.

## Section 2: Health Spending Analysis by Measures

This section focuses on analyzing health spending data across countries and comparing spending levels against various
measures.

This script contains the following functions:

- get_government_spending_shares(constant_usd_data: pd.DataFrame) -> pd.DataFrame: Calculates the share of government
  spending.
- get_gdp_spending_shares(constant_usd_data: pd.DataFrame) -> pd.DataFrame: Calculates the share of GDP spending.
- get_per_capita_spending(constant_usd_data: pd.DataFrame) -> pd.DataFrame: Calculates per capita spending figures.
- read_au_countries() -> list: Reads African Union (AU) member countries.
- filter_au_countries(df: pd.DataFrame) -> pd.DataFrame: Filters a DataFrame to keep only AU countries.
- clean_data_for_chart(df: pd.DataFrame) -> pd.DataFrame: Cleans and reorders data for the Flourish chart.
- chart_2_2_1() -> None: Generates data for chart 1 in section 2 and saves it as a CSV file.
- chart_2_2_2() -> None: Generates data for chart 2 in section 2 and saves it as a CSV file.
- chart_2_3() -> None: Generates data for chart 3 in section 2 and saves it as a CSV file.

## Section 3: Spending by Source

This file contains the main functions to create a chart which looks at the sources of health spending.

The script includes functions for:

- Reading and processing health spending data (both total and out-of-pocket).
- Rebuilding private spending data by merging dataframes.
- Sorting countries by income level.
- Calculating shares of total spending, per capita spending, and spending as a share of GDP.
- Cleaning, reshaping, and preparing data for use in Flourish (a data visualization tool).

## Section 4: Multilateral data

This file contains functions to clean and analyze the multilateral (MDB) donors' data for the health sector. The main
function chart_4_1() reads the raw data, processes it, and outputs a CSV file with the results. 