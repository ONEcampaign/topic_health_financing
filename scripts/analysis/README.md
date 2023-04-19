# Analysis
This module contains scripts to download health expenditure data. It fetches and processes data from various sources,
including multilateral donors, and provides detailed and aggregated views of health spending data by different
categories such as spending by source, spending by disease, and spending by financing scheme.

## Files
### download_indicators.py
This file contains functions to download health expenditure data based on various indicators. Functions include:

- get_current_health_exp: Get current health expenditure data.
- get_health_exp_by_source: Get health expenditure by source data.
- get_health_exp_by_function: Get health expenditure by function data.
- get_health_exp_by_disease: Get health expenditure by disease data.
- get_health_exp_by_financing_scheme: Get health expenditure by financing scheme data.

### create_data_versions.py
This file defines functions to create different versions of health expenditure data. The main function,
save_data_pipeline, saves multiple versions of the data, such as local currency units (LCU), GDP share, USD current, USD
constant, and USD constant per capita. It creates data versions for overall health spending, COVID-19 spending,
government spending, out-of-pocket spending, and spending by disease.

### read_data_versions.py
This file provides a function, read_spending_data_versions, to read different versions of health spending data. It reads
the data versions created by the create_data_versions.py script for various datasets, such as overall health spending,
COVID-19 spending, government spending, and more.

### multilateral.py
This file processes and analyzes data on health-related funding from multilateral donors. It defines health sector codes
and groups, sets up data paths, and includes functions for filtering multilateral donors, adding sectors and broad
sectors columns, summarizing data, and adding income levels and region groups to the data.
