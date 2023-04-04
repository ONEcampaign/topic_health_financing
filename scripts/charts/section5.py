from functools import partial

import pandas as pd
from bblocks import format_number

from scripts.analysis.data_versions import read_spending_data_versions
from scripts.charts.common import (
    combine_income_countries,
    get_version,
    per_capita_by_income,
    total_by_income,
)
from scripts.config import PATHS
from scripts.tools import (
    value2gdp_share,
    value2gdp_share_group,
    value2gov_spending_share,
    value2pc,
)

FULL_DATA = read_spending_data_versions(dataset_name="health_spending_by_disease")

get_spending_version = partial(
    get_version, versions_dict=FULL_DATA, additional_cols=["source", "type", "subtype"]
)

total_spending = get_spending_version(version="usd_constant")

total_spending = total_spending.query("value.notna()")
total_spending = total_spending.query("year.dt.year == 2019")

total_type = total_spending.query("subtype.isna()")

total_type = (
    total_type.assign(year=lambda d: d.year.dt.year)
    .filter(["year", "income_group", "country_name", "source", "type", "value"], axis=1)
    .loc[lambda d: d.source != "total"]
)
