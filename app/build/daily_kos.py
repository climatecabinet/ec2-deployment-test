import pandas as pd
from halo import Halo
from utils import (
    find_first_from_regex, switch_halo_icon, update_halo_base, update_halo_scroll
)
from app.lookups.ccid import assemble_ccid
from app.models import (Region, RegionType, RegionFragment)
from app.config import DailyKosDatasets as DK

DK_DIR_TO_TYPES = {
    "congressional-districts-to-counties": (RegionType.CONGR, RegionType.COUNTY),
    "counties-to-congressional-districts": (RegionType.COUNTY, RegionType.CONGR),
    "counties-to-state-house-districts": (RegionType.COUNTY, RegionType.SLDL),
    "counties-to-state-senate-districts": (RegionType.COUNTY, RegionType.SLDU),
    "state-house-districts-to-counties": (RegionType.SLDL, RegionType.COUNTY),
    "state-senate-districts-to-counties": (RegionType.SLDU, RegionType.COUNTY)
}
spinner = Halo()


def add_fragment_from_row(row, o_type, s_type, state, abbr, keys):
    """Adds a relationship between two regions (either interstecting or parent-child) to the
    appropriate region's relationship list.
    """
    update_halo_scroll(spinner, f"{abbr} ~ {row.name}")

    source = s_type.cls.objects.only('id').get(ccid=assemble_ccid(s_type,
                                                                  row[keys.SOURCE],
                                                                  state=state))

    owner = o_type.cls.objects.get(ccid=assemble_ccid(o_type, row[keys.OWNER], state=state))

    owner.update(push__fragments=RegionFragment(region=source,
                                                population=row[keys.POP],
                                                perc_of_whole=row[keys.PERC]))


def get_dk_keys(o_type, s_type, headers):
    """TODO:
        * move this to the Daily Kos clean script
    """
    class Keys():
        def __init__(self, o_type, s_type):
            type_to_key = {
                RegionType.COUNTY: DK.Keys.COUNTY,
                RegionType.CONGR: DK.Keys.CD,
                RegionType.SLDU: DK.Keys.SLDU,
                RegionType.SLDL: DK.Keys.SLDL,
            }
            self.OWNER = find_first_from_regex(type_to_key[o_type], headers)
            self.SOURCE = find_first_from_regex(type_to_key[s_type], headers)
            self.POP = find_first_from_regex(DK.Keys.POP, headers)
            self.PERC = find_first_from_regex(DK.Keys.PERC, headers)

    return Keys(o_type, s_type)


def refresh_daily_kos(state_filter):
    """ Refreshes the daily kos region-relationship data, as well as population data."""
    print("\n~~ Refreshing Daily Kos fragments data ~~")
    switch_halo_icon(spinner)
    spinner.start()
    update_halo_base(spinner, "Clearing previous fragment data...")
    Region.objects().update(unset__fragments=True)

    for dk_dir in [d for d in DK.DATA_DIR.iterdir() if d.is_dir()]:
        owner, source = DK_DIR_TO_TYPES[dk_dir.name]
        update_halo_base(spinner, (f"\tHandling {owner.name}-owned {source.name} fragments"))

        for state in dk_dir.glob("*.csv"):
            if (abbr := state.name.split(".")[0]) not in state_filter:
                if not (df := pd.read_csv(state)).empty:
                    keys = get_dk_keys(owner, source, list(df.columns))

                    apply_args = (owner,
                                  source,
                                  assemble_ccid(RegionType.STATE, abbr),
                                  abbr,
                                  keys)

                    df.apply(add_fragment_from_row, args=apply_args, axis=1)

    spinner.succeed("Done!")
