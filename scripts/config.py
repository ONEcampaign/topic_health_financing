from pathlib import Path


class PATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    raw_data = project / "raw_data"
    pydeflate_data = raw_data / ".pydeflate_data"
    output = project / "output"
    scripts = project / "scripts"
    db_credentials = scripts / "config.ini"
    logs = scripts / ".logs"


CONSTANT_YEAR = 2022