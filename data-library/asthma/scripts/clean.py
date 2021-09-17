import os
import os.path
import pandas as pd
from app.config import DATA_RAW_PATH, DATA_CLEANED_PATH, AsthmaDataset as AD

AK = AD.AsthmaKeys


def clean(_):
    # first, open the raw dataset, something like...
    df = pd.read_csv(os.path.join(DATA_RAW_PATH % 'asthma', 'all-raw.csv'))

    # Convert case counts from strings to floats
    for key in [AK.ADULT, AK.CHILD, AK.NON_WHITE, AK.POVERTY, AK.POP]:
        df[key] = df[key].apply(lambda num: float(num.replace(',', '')))

    # Washington DC, since it is techincally a state-level district, must
    # have a falsey value in the County column
    df.loc[df[AK.COUNTY] == 'Washington, DC', AK.COUNTY] = ''

    # In Louisiana, Counties are called 'parishes', but the word 'County'
    # has been appended to the end of these names in this dataset
    def _clean_parishes(r):
        return r[AK.COUNTY].rsplit(" ", 1)[0] \
               if r[AK.STATE] == 'LA' \
               else r[AK.COUNTY]
    df[AK.COUNTY] = df.apply(lambda r: _clean_parishes(r), axis=1)

    # finally, write the dataframe to a csv in this dataset's cleaned data folder...
    df.to_csv(os.path.join(DATA_CLEANED_PATH % 'asthma', 'all.csv'))
