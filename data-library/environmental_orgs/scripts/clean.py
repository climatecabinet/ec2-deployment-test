import pandas as pd
from app.config import EnvironmentalOrgsDataset as EOD


def clean(_):
    df = pd.read_csv(EOD.RAW_DATASET)

    # clean off the column headers
    df = df.rename(columns={
        'State abbrev': 'state_abbr',
        'Organization': 'name',
        'Organization Site': 'site',
    })

    df.to_csv(EOD.DATASET, index=False)
