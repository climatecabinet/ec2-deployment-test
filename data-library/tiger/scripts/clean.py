"""Cleans the US Census TIGER Shapefile data.

This code is based almost entirely on open source code written by @jamesturk
at OpenStates, which can be found here --> is.gd/1K0YAy
"""
import re
import geojson
import zipfile
import subprocess
from pathlib import Path
from utils import print_cr
from app.models import RegionType
from app.config import DATA_RAW_PATH, DATA_CLEANED_PATH, TigerDataset
from app.lookups.ccid import assemble_ccid

TIGER_RAW_PATH = Path(DATA_RAW_PATH % 'tiger')
TIGER_CLEAN_PATH = Path(DATA_CLEANED_PATH % 'tiger')

TK = TigerDataset.Keys


def render_district_type_and_shortcode(reg_type, props):
    """Render district type and shortcode fields from properties object."""

    # In MD, we replace the word "Subdistrict" with the word "District"
    dist_name = props[TK.NAME].replace("Subdistrict", "District")

    # In NJ, we need to remove the word "General" from the district type
    # "General Assembly"
    dist_name = dist_name.replace("General", '').strip()

    # generate the district type
    dist_type = dist_name.split("District")[0].strip()

    # handle Congressional 'At Large' districts seperately
    if reg_type == RegionType.CONGR and "at large" in props[TK.NAME].lower():
        shortcode = "CD AL"
    else:
        # clean the district's name into a shortcode
        shortcode = dist_name.replace('State', '')
        shortcode = re.sub(r'[^A-Z0-9\s]', '', shortcode)
        shortcode = re.sub(
            r'\w D', lambda m: shortcode[m.start()] + shortcode[m.end() - 1], shortcode
        )
        shortcode = shortcode.strip()

    return dist_type, shortcode


def nitpick_geojson(file_path):
    with open(file_path, 'r+') as geo_file:
        geo = geojson.load(geo_file)

        for ftr in geo['features']:
            props = ftr['properties']

            r_type = RegionType.fuzzy_cast(props[TK.TYPE_CODE])

            # make NAME field consistent across all region types
            if 'NAMELSAD' in props.keys():
                props[TK.NAME] = props['NAMELSAD']
                del props['NAMELSAD']

            # build a CCID field for the region
            props[TK.CCID] = assemble_ccid(
                RegionType.fuzzy_cast(props[TK.TYPE_CODE]), props[TK.GEOID]
            )

            if r_type in (RegionType.CONGR, RegionType.SLDU, RegionType.SLDL):
                # make district number field consistent across all district types
                dist_num_key = list(
                    filter(lambda k: re.match(r'SLD[UL]ST|CD11\dFP', k), props.keys())
                ).pop()
                props[TK.DIST_NUM] = props[dist_num_key]
                del props[dist_num_key]

                # in SC, house district names seem to be malformed - where every other
                # name just includes the number, SC house districts include a 'HD-' prefix
                props[TK.NAME] = re.sub(r'HD-0*', '', props[TK.NAME])

                dist_type, shortcode = render_district_type_and_shortcode(r_type, props)
                props[TK.SHORTCODE] = shortcode
                props[TK.DIST_TYPE] = dist_type

        geo_file.seek(0)
        geo_file.truncate()
        geo_file.write(geojson.dumps(geo))


def clean(_):
    TIGER_CLEAN_PATH.mkdir(exist_ok=True)

    for raw_year in [yp for yp in TIGER_RAW_PATH.iterdir() if str(yp.name).isdigit()]:
        (TIGER_CLEAN_PATH / raw_year.name).mkdir(exist_ok=True)
        (TIGER_CLEAN_PATH / raw_year.name / 'sldu').mkdir(exist_ok=True)
        (TIGER_CLEAN_PATH / raw_year.name / 'sldl').mkdir(exist_ok=True)

        # make a working directory for intermediate files
        (working_dir := raw_year / 'temp').mkdir(exist_ok=True)

        for raw_zip in raw_year.glob(r'**/tl*.zip'):
            # see if it already exists in clean, and continue if so
            clean_geo = Path(
                str(raw_zip).replace('/raw-data/', '/data/').replace('.zip', '.geojson')
            )
            if clean_geo.exists():
                print_cr(f"{clean_geo.name} already cleaned, skipping!")
                continue

            # unzip the zip file
            with zipfile.ZipFile(raw_zip, "r") as f:
                f.extractall(working_dir)

            working_shp = working_dir / raw_zip.name.replace('.zip', '.shp')

            print_cr(f"{working_shp} => {clean_geo}")
            subprocess.run(  # create the GeoJSON file
                [
                    "ogr2ogr",
                    "-where",
                    "GEOID NOT LIKE '%ZZ%'",
                    "-t_srs",
                    "crs:84",
                    "-f",
                    "GeoJSON",
                    str(clean_geo),
                    str(working_shp),
                ],
                check=True,
            )

            # nitpick fields in the new geojson file
            nitpick_geojson(clean_geo)

        # remove the temporary zip file from clean
        subprocess.run(['rm', '-rf', str(working_dir)], check=True)
