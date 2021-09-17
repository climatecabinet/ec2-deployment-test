import pandas as pd
from pathlib import Path
from app.config import DATA_RAW_PATH, DATA_CLEANED_PATH, JobsDataset as JD
from app.models import RegionType

JK = JD.JobsKeys


# check this, make sure no tomfoolery in here...
def clean(_):
    df = pd.read_csv(Path(DATA_RAW_PATH % 'jobs', 'all.csv'))

    # make sure JK.GEOTYPE is RegionType fuzzy-castable
    df[JK.GEOTYPE] = df[JK.GEOTYPE].replace({
        "county": RegionType.COUNTY.full,
        "state": RegionType.STATE.full,
        "cd": RegionType.CONGR.full,
        "sldu": RegionType.SLDU.full,
        "sldl": RegionType.SLDL.full
    })

    # make sure columns containing numbers are interpretable as ints (remove punctuation)
    cols_with_nums = list(
        set(df.columns).difference(set([JK.STATE, JK.GEOTYPE, JK.NAME, JK.GEOID, 'sourceURL']))
    )
    df[cols_with_nums] = df[cols_with_nums].replace({r'[^\d\.]': ''}, regex=True).astype(float)

    # the totalDollarsInvested column is getting rounded pretty harshly at the source, so we
    # redo the summation ourselves to preserve precision
    df[JK.TOTAL_DOLLARS_INVESTED] = df.apply(
        lambda r: (r[JK.RESIDENTIAL_DOLLARS_INVESTED] +
                   r[JK.COMMERCIAL_DOLLARS_INVESTED] +
                   r[JK.UTILITY_DOLLARS_INVESTED]),
        axis=1
    )

    df.to_csv(Path(DATA_CLEANED_PATH % 'jobs', 'all.csv'), index=False)
