import json

import pandas as pd
import requests

from scripts import config


def _wb_countries_url(page: int) -> str:
    """Return a URL for the World Bank IDS API to get a list of countries,
    specifying the page number."""
    return f"https://api.worldbank.org/v2/country?per_page=300&page={page}&format=json"


def _get_wb_countries_dict(response: json) -> dict:
    # If there is only one page, return a dictionary, if not, loop to get all data.
    if response[0]["pages"] == 1:
        return {
            c["id"]: {
                "name": c["name"],
                "region": c["region"]["value"],
                "income": c["incomeLevel"]["value"],
                "lending": c["lendingType"]["value"],
            }
            for c in response[1]
        }

    else:
        d = {}
        for page in range(1, response[0]["pages"] + 1):
            url = _wb_countries_url(page=page)
            response = requests.get(url).json()
            d.update(
                {
                    c["id"]: {
                        "name": c["name"],
                        "region": c["region"]["value"],
                        "income": c["incomeLevel"]["value"],
                        "lending": c["lendingType"]["value"],
                    }
                    for c in response[1]
                }
            )
        return d


def download_wb_countries() -> None:
    """
    Download a dictionary of World Bank 3-letter country codes and their names.
    Save as a CSV in the debt folder.
    """
    # Countries list URL
    url = _wb_countries_url(page=1)

    # Fetch data
    response = requests.get(url).json()

    # Get dictionary of country codes and names
    d = _get_wb_countries_dict(response=response)

    # Save the dictionary to json file
    with open(config.PATHS.raw_data / "wb_groupings.json", "w") as f:
        json.dump(d, f)


def wb_countries_dict() -> dict:
    """Return a dataframe of World Bank 3-letter country codes and their names."""

    with open(config.PATHS.raw_data / "wb_groupings.json", "r") as f:
        d = json.load(f)

    return d


def wb_countries_df() -> pd.DataFrame:
    """Return a dataframe of World Bank 3-letter country codes and their names."""
    # load the dictionary
    d = wb_countries_dict()

    return (
        pd.DataFrame.from_dict(d, orient="index")
        .reset_index()
        .rename(columns={"index": "iso_code"})
    )


if __name__ == "__main__":
    download_wb_countries()
