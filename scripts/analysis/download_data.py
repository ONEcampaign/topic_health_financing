"""Download GHED data"""

import bblocks_data_importers as bbdata
import pandas as pd
import numpy as np
import country_converter as coco
from bblocks.dataframe_tools.add import add_income_level_column

from scripts.config import PATHS
from scripts.logger import logger


def clean(df) -> pd.DataFrame:
    """Clean the GHED data"""

    return (df
     .assign(value=lambda d: np.where(d["unit"] == "Millions", d["value"] * 1e6, d["value"]))
     .assign(value=lambda d: np.where(d["unit"] == "Thousands", d["value"] * 1e3, d["value"]))
            .assign(continent = lambda d: coco.CountryConverter().pandas_convert(d.iso3_code, to="continent"))
            .pipe(add_income_level_column, "iso3_code", "ISO3")
            .loc[:, ['iso3_code', 'year', 'indicator_code', 'value', 'continent', 'income_level']]
     )

def download_ghed() -> None:
    """Download ghed data to raw data directory"""

    ghed = bbdata.GHED()
    df = ghed.get_data()
    df = clean(df)

    df.to_csv(PATHS.raw_data / "ghed.csv", index=False)

if __name__ == "__main__":
    download_ghed()
    logger.info("GHED data downloaded")
