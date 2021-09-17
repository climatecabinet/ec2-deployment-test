"""Fetches the US Census TIGER Shapefile data.

This code is based almost entirely on open source code written by @jamesturk
at OpenStates, which can be found here --> is.gd/AaD7iB
"""

import us
import requests
from pathlib import Path
from app.config import DATA_RAW_PATH
from utils import print_cr

YEARS_AND_CGR_SESSIONS = [
    (2020, 116),
    (2019, 116),
    (2018, 116),
    (2017, 115),
    (2016, 115),
    (2015, 114),
]
TIGER_RAW_PATH = Path(DATA_RAW_PATH % 'tiger')
TIGER_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER{year}/{region_type_uppercase}/tl_{year}_{parent_region_id}_{region_type}.zip"  # noqa: E501


def get_zip_from_url(url, out_path):
    response = requests.get(url)

    if response.status_code == 200:
        with open(out_path, 'wb') as f:
            f.write(response.content)
    else:
        response.raise_for_status()


def fetch():

    TIGER_RAW_PATH.mkdir(exist_ok=True)

    for year, cgr_session_num in YEARS_AND_CGR_SESSIONS:
        print(f"-/-/---- Fetching TIGER shapes for {year} ----/-/-")

        year_path = TIGER_RAW_PATH / Path(str(year))
        year_path.mkdir(exist_ok=True)

        print_cr("\tHandling state and county shapes")
        for region_type in ['state', 'county']:
            url = TIGER_BASE_URL.format(
                year=year,
                region_type_uppercase=region_type.upper(),
                parent_region_id='us',
                region_type=region_type,
            )

            # if the .zip file already exists, skip download
            if (year_path / url.split('/')[-1]).exists():
                print_cr(f"\tSkipping {region_type} shapes for {year}, already downloaded!")
                continue

            get_zip_from_url(url, year_path / f"tl_{year}_us_{region_type}.zip")

        print_cr("\tHandling congressional district shapes")
        cd_url = TIGER_BASE_URL.format(
            year=year,
            region_type_uppercase="CD",
            parent_region_id='us',
            region_type=f"cd{cgr_session_num}"
        )
        # if the .zip file already exists, skip download
        if (year_path / cd_url.split('/')[-1]).exists():
            print_cr(f"\tSkipping {region_type} shapes for {year}, already downloaded!")
        else:
            get_zip_from_url(cd_url, year_path / f"tl_{year}_us_cd{cgr_session_num}.zip")

        for state in us.STATES + [us.states.PR]:

            print_cr(f"\tHandling SLD shapes in {state.abbr}")

            sldu_path = year_path / Path('sldu')
            sldu_path.mkdir(exist_ok=True)

            sldl_path = year_path / Path('sldl')
            sldl_path.mkdir(exist_ok=True)

            for chamber, chamber_path in [('l', sldl_path), ('u', sldu_path)]:

                # DC and NE are unicameral, and therefore have no lower chamber
                if state.abbr in ('DC', 'NE') and chamber == 'l':
                    continue

                if (chamber_path / f"tl_{year}_{state.fips}_sld{chamber}.zip").exists():
                    print(f"\tSkipping shapes for {state.abbr}, already downloaded!", end="\r")
                    continue

                url = TIGER_BASE_URL.format(
                    year=year,
                    region_type_uppercase=chamber_path.name.upper(),
                    parent_region_id=state.fips,
                    region_type=chamber_path.name,
                )

                get_zip_from_url(url, chamber_path / f"tl_{year}_{state.fips}_sld{chamber}.zip")

        print('\n')
