from pathlib import Path


class PATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    raw_data = project / "raw_data"
    pydeflate_data = raw_data / ".pydeflate_data"
    output = project / "output"
    scripts = project / "scripts"
    logs = scripts / ".logs"


CONSTANT_YEAR: int = 2020
MULTI_CONSTANT_YEAR: int = 2020
UN_POPULATION_YEARS: dict[str, int] = {"start_year": 2000, "end_year": 2023}
UN_POPULATION_URL: str = "https://population.un.org/dataportalapi/api/v1/"
