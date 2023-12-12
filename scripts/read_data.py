"""Read data from the Policy Database"""


from oda_data import set_data_path


import pandas as pd
from bblocks import set_bblocks_data_path

from scripts.config import PATHS

DATABASE = "policy_data"
METADATA = "metadata"
COLLECTION_NAME = "ghed"

from policy_dbtools import dbtools as dbt

# Optionally import set_config to create a file with your credentials
# from policy_dbtools.dbtools import set_config

set_bblocks_data_path(PATHS.raw_data)
dbt.set_config_path(PATHS.db_credentials)
set_data_path(PATHS.raw_data)

# Optionally set config to create a file with your credentials
# set_config(
#     username=...,
#     password=...,
#     cluster=...,
#     db=...,
# )


def get_indicator(indicator_code: str, additional_filter: dict = None) -> pd.DataFrame:
    """Get data for a given indicator code"""

    if additional_filter is None:
        _filter = {"indicator_code": indicator_code}
    else:
        _filter = {"indicator_code": indicator_code, **additional_filter}

    # Create a reader to fetch the data
    reader = dbt.MongoReader(
        dbt.AuthenticatedCursor(db_name=DATABASE), collection_name=COLLECTION_NAME
    )

    # Fetch the data
    df = reader.get_df(query=_filter).rename(columns={"entity_code": "iso_code"})

    return df


if __name__ == "__main__":
    sample_indicator = "ghed_current_health_expenditure"

    data = get_indicator(indicator_code=sample_indicator)
