import pandas as pd
import requests

from scripts import config
from scripts.logger import logger

INDICATORS = {49: "Total Population"}


def _get_un_locations() -> pd.DataFrame:
    """Download all UN locations using API"""

    locations_url = config.UN_POPULATION_URL + "locations/"

    logger.info(f"Downloading UN locations from {locations_url}")

    return download_un_population_data(locations_url)


def un_population_url(
    indicator: int, start_year: int, end_year: int, locations: int | list[int]
) -> str:
    """Create a URL to download UN population data from the API"""

    if isinstance(locations, list):
        locations = ",".join(map(str, locations))

    logger.debug(f"Downloading UN population data using API")

    return (
        f"{config.UN_POPULATION_URL}data/indicators/{indicator}/"
        f"locations/{locations}/start/{start_year}/end/{end_year}?per_page=500"
    )


def download_un_population_data(url: str) -> pd.DataFrame:
    response = requests.get(url).json()

    df = pd.json_normalize(response["data"])

    while response["nextPage"] is not None:
        new_url = response["nextPage"]
        response = requests.get(new_url).json()
        df = pd.concat([df, pd.json_normalize(response["data"])])

    return df


def download_all_population() -> None:
    """Update the raw UN population data"""

    locations = _get_un_locations()
    ids = locations["id"].to_list()

    url = un_population_url(
        indicator=49,
        start_year=config.UN_POPULATION_YEARS["start_year"],
        end_year=config.UN_POPULATION_YEARS["end_year"],
        locations=ids,
    )

    df = download_un_population_data(url)

    file_path = config.PATHS.raw_data / "un_population_raw.csv"

    df.to_csv(file_path, index=False)

    logger.info(f"Downloaded UN population data to {file_path}")


def raw_un_population_data() -> pd.DataFrame:
    """Read the raw UN population data"""

    file_path = config.PATHS.raw_data / "un_population_raw.csv"

    logger.debug(f"Read UN population data from {file_path}")

    return pd.read_csv(file_path)


def filter_total_population(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.loc[lambda d: d.sex == "Both sexes"]
        .reset_index(drop=True)
        .drop("sex", axis=1)
    )


def filter_median_variant(data: pd.DataFrame) -> pd.DataFrame:
    return data.loc[lambda d: d.variantLabel == "Median"].reset_index(drop=True)


def filter_key_columns(data: pd.DataFrame) -> pd.DataFrame:
    return data.filter(
        ["location", "iso3", "indicator", "timeLabel", "sex", "value"], axis=1
    )


def clean_population_data(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.pipe(filter_total_population)
        .pipe(filter_median_variant)
        .assign(indicator=lambda d: d.indicatorId.map(INDICATORS))
        .pipe(filter_key_columns)
        .rename(
            columns={
                "iso3": "iso_code",
                "timeLabel": "year",
                "location": "location_name",
            }
        )
        .astype({"year": int, "value": int})
    )


def un_population_data() -> pd.DataFrame:
    """Clean dataset containing the median estimates for all available countries"""
    return raw_un_population_data().pipe(clean_population_data)


if __name__ == "__main__":
    download_all_population()
