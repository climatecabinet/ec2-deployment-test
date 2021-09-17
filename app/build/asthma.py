"""Builds/ refreshes asthma data for every region in the database.

"""
import pandas as pd
from halo import Halo
from utils import switch_halo_icon, update_halo_base, update_halo_scroll
from app.models import Region, RegionType, AsthmaData
from app.config import AsthmaDataset as AD
from app.lookups.ccid import assemble_ccid
from mongoengine.queryset.visitor import Q

AK = AD.AsthmaKeys
spinner = Halo()


def refresh_region_asthma(row, num_regions):
    """Refreshes the asthma counts for a single region"""
    region = Region.objects.get(
        ccid=assemble_ccid(RegionType.COUNTY, row[AK.COUNTY], state=row[AK.STATE])
    )

    region.update(
        asthma=AsthmaData(
            population=row[AK.POP],
            adult=row[AK.ADULT],
            child=row[AK.CHILD],
            non_white=row[AK.NON_WHITE],
            poverty=row[AK.POVERTY],
            extrapolated=False,
        )
    )

    update_halo_scroll(spinner, f"{row.name}/{num_regions}")


def refresh_asthma(states_to_skip):
    """Refreshes the asthma counts"""
    print("\n~~ Refreshing Asthma Data ~~")
    switch_halo_icon(spinner)
    spinner.start()

    update_halo_base(spinner, "Opening asthma dataset")
    df = pd.read_csv(AD.DATASET)
    df = df[~df[AK.STATE].isin(states_to_skip)].reset_index(drop=True)

    update_halo_base(spinner, "Refreshing asthma data from dataset")
    df.apply(refresh_region_asthma, args=[len(df)], axis=1)

    update_halo_base(spinner, "Extrapolating for regions without direct data")
    target_regions = Region.objects.only('id', 'fragments', 'ccid', 'asthma')(
        Q(state_abbr__nin=states_to_skip)
        & Q(fragments__exists=True)
        & (Q(asthma__exists=False) | Q(asthma__extrapolated=True))
    )

    for i, region in enumerate(target_regions):
        region.update(
            asthma=region.extrapolate_count(AsthmaData, RegionType.COUNTY, 'asthma')
        )

        update_halo_scroll(spinner, f"{i}/{len(target_regions)}")

    spinner.succeed("Done!")
