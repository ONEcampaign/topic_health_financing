import pandas as pd

from scripts import config


def read_spending_data_versions(dataset_name: str) -> dict[str, pd.DataFrame]:
    """Read versions of the LCU data"""
    return {
        key: pd.read_feather(config.PATHS.raw_data / f"{dataset_name}_{key}.feather")
        for key in [
            "lcu",
            "gdp_share",
            "usd_current",
            "usd_constant",
            "usd_constant_pc",
        ]
    }


if __name__ == "__main__":
    overall_spending = read_spending_data_versions(dataset_name="health_spending")
    covid_spending = read_spending_data_versions(dataset_name="covid_spending")
    gov_spending = read_spending_data_versions(dataset_name="gov_spending")
    scheme_spending = read_spending_data_versions(
        dataset_name="health_spending_by_source"
    )
    oop_spending = read_spending_data_versions(dataset_name="health_spending_oop")
    disease_spending = read_spending_data_versions(
        dataset_name="health_spending_by_disease"
    )
