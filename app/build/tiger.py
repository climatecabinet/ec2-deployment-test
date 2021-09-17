"""Builds Region collection documents from cleaned TIGER geojson files.

"""
import us
import geojson
from us import states
from halo import Halo
from mongoengine.queryset import DoesNotExist
from utils import (
    switch_halo_icon, update_halo_base, update_halo_scroll
)
from app.models import Shape, RegionShape, RegionType
from app.config import TigerDataset as TD

spinner = Halo()
TK = TD.Keys  # for reading TIGER shapefile rows


def load_region_specific_fields(reg, r_type, props):
    """Loads data into fields that are region-type-specific.
    """
    if r_type == RegionType.COUNTY:
        reg.county_fips = props[TK.COUNTY_FIPS]
        reg.county_gnis = props[TK.COUNTY_GNIS]
        # in VA, "counties" whose FIPS codes are 500 or greater are actually
        # independent cities (source -> is.gd/JxcT1G)
        reg.is_independent_city = (
            props[TK.STATE_FIPS] == states.VA.fips and
            int(props[TK.COUNTY_FIPS]) >= 500
        )

    elif r_type in [RegionType.CONGR, RegionType.SLDU, RegionType.SLDL]:
        reg.district_no = props[TK.DIST_NUM]
        reg.district_type = props[TK.DIST_TYPE]
        reg.shortcode = props[TK.SHORTCODE]

        if r_type == RegionType.CONGR:
            reg.district_session = props[TK.CD_SESSION]
        else:
            reg.leg_year = int(props[TK.SL_LEG_YEAR])


def refresh_region_from_geojson(feature, year):
    props = feature['properties']
    state = us.states.lookup(props[TK.STATE_FIPS])
    region_type = RegionType.fuzzy_cast(props[TK.TYPE_CODE])

    shape = Shape(
        year=year,
        shape=feature['geometry'],
        state_abbr=state.abbr,
        geoid=props[TK.GEOID],
        ccid=props[TK.CCID],
        name=props[TK.NAME],
        land_area=props[TK.LAND_AREA],
    )

    # try to pull its associated Region, make a new one if not found
    try:
        region = region_type.cls.objects.get(ccid=props[TK.CCID])
    except DoesNotExist:
        region = region_type.cls(
            state_fips=state.fips,
            state_abbr=state.abbr,
            geoid=props[TK.GEOID],
            ccid=props[TK.CCID],
            name=props[TK.NAME]
        )

        load_region_specific_fields(region, region_type, props)

    # pre-validation saves to help with connecting the two documents via references
    shape.save(validate=False)
    region.save(validate=False)

    shape.region = region
    region.shapes.append(RegionShape(year=year, shape=shape))

    # save again, this time with validation
    region.save()
    shape.save()


def refresh_tiger(states_to_skip):
    year_dirs = sorted(
        [yd for yd in TD.TIGER_DIR.iterdir() if str(yd.name).isdigit()], reverse=True
    )

    for year_dir in year_dirs:
        print(f"\n~~ Handling {year_dir.name} TIGER/Linefile data ~~")
        switch_halo_icon(spinner)
        spinner.start()

        for geo_file in year_dir.glob(r'**/*.geojson'):
            update_halo_base(spinner, f"Handling {geo_file.name}")
            update_halo_scroll(spinner, "opening...")

            with open(geo_file, 'r') as f:
                geo = geojson.load(f)

            for i, feature in enumerate(geo['features']):
                if feature['properties'][TK.STATE_FIPS] in states_to_skip:
                    continue

                update_halo_scroll(spinner, f"{i}/{len(geo['features'])}")

                refresh_region_from_geojson(feature, int(year_dir.name))

        spinner.succeed('Done!')
