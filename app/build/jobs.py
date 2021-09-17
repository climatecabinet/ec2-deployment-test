"""Refreshes the Jobs data.
"""
import pandas as pd
from halo import Halo
from utils import switch_halo_icon, update_halo_base, update_halo_scroll
from app.models import Region, JobsData, JobsStat, JobsCounts, RegionType
from app.lookups.ccid import assemble_ccid
from app.config import JobsDataset as JD

JK = JD.JobsKeys
spinner = Halo()


def refresh_region_jobs(row, len_df):
    """Refreshs jobs data for a single region."""
    counts = JobsCounts(solar=row[JK.COUNT_SOLAR_JOBS],
                        energy=row[JK.COUNT_ENERGY_JOBS],
                        total=row[JK.TOTAL_JOBS])

    # wind counts are sparser than energy and solar counts
    if not pd.isna(row[JK.COUNT_WIND_JOBS]):
        counts.wind = row[JK.COUNT_WIND_JOBS]

    mwh_invested = JobsStat(residential=row[JK.RESIDENTIAL_MWH_INVESTED],
                            commercial=row[JK.COMMERCIAL_MWH_INVESTED],
                            utility=row[JK.UTILITY_MWH_INVESTED],
                            total=row[JK.TOTAL_MWH_INVESTED])

    dollars_invested = JobsStat(residential=row[JK.RESIDENTIAL_DOLLARS_INVESTED],
                                commercial=row[JK.COMMERCIAL_DOLLARS_INVESTED],
                                utility=row[JK.UTILITY_DOLLARS_INVESTED],
                                total=row[JK.TOTAL_DOLLARS_INVESTED],
                                home_equivalent=row[JK.INVESTMENT_HOMES_EQUIVALENT])

    installations_count = JobsStat(residential=row[JK.COUNT_RESIDENTIAL_INSTALLATIONS],
                                   commercial=row[JK.COUNT_COMMERCIAL_INSTALLATIONS],
                                   utility=row[JK.COUNT_UTILITY_INSTALLATIONS],
                                   total=row[JK.TOTAL_INSTALLATIONS])

    mw_capacity = JobsStat(residential=row[JK.RESIDENTIAL_MW_CAPACITY],
                           commercial=row[JK.COMMERCIAL_MW_CAPACITY],
                           utility=row[JK.UTILITY_MW_CAPACITY],
                           total=row[JK.TOTAL_MW_CAPACITY])

    jobs = JobsData(perc_of_state_jobs=row[JK.PERCENT_OF_STATE_JOBS],
                    counts=counts,
                    mwh_invested=mwh_invested,
                    dollars_invested=dollars_invested,
                    installations_count=installations_count,
                    mw_capacity=mw_capacity,
                    extrapolated=False)

    ccid = assemble_ccid(RegionType.fuzzy_cast(row[JK.GEOTYPE]), row[JK.GEOID])
    region = Region.objects.get(ccid=ccid)
    region.jobs = jobs
    region.save()

    update_halo_scroll(spinner, f"{row.name}/{len_df}")


def refresh_jobs(states_to_skip):
    """ Refreshes the jobs counts """
    print("\n~~ Refreshing Jobs Data ~~")
    switch_halo_icon(spinner)
    spinner.start()

    update_halo_base(spinner, "Opening jobs dataset")
    df = pd.read_csv(JD.DATASET)
    df = df[~df[JK.STATE].isin(states_to_skip)].reset_index(drop=True)  # filter out skip states

    update_halo_base(spinner, "Refreshing jobs data from dataset")
    df.apply(refresh_region_jobs, args=[len(df)], axis=1)

    spinner.succeed("Done!")
