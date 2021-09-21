import shutil
from pathlib import Path
from app.config import DATA_DIR


README_STUB = '''# {name_title} Data Pipeline
This is a stub README for the {name} dataset. A one-liner description should go here,
and the following questions should be answered:

### 1. What is the source of this dataset? (Include a link if possible)

### 2. How should this data be joined/ loaded into the database?

### 3 . How should the fetched/ cleaned data be stored in the **data** folder?

### 4. How to fetch this data from the world?

### 5. How to clean this data?


'''

CLEAN_STUB = '''import os
import os.path
import pandas as pd
from app.config import DATA_RAW_PATH, DATA_CLEANED_PATH


def clean(target_states=None):
    # first, import the raw data as a DataFrame, something like:
    # df = pd.read_csv(os.path.join(DATA_RAW_PATH % '{name}', 'all-raw.csv'))

    """ cleaning operations go here """

    # finally, write the DataFrame to the cleaned data folder, something like:
    # df.to_csv(os.path.join(DATA_CLEANED_PATH % '{name}', 'all.csv'))

    raise NotImplementedError
'''

FETCH_STUB = '''def fetch():
    """Implement with code that automates scraping/ downloading process for this dataset"""
    raise NotImplementedError
'''

INIT_STUB = '''from .fetch import fetch
from .clean import clean
'''


def init_new_dataset(name):
    # make sure name doesn't already exist
    if Path(DATA_DIR, name).exists():
        raise ValueError(
            f"The data-library folder '{name}' already exists, "
            "please choose a different name."
        )

    # make the dirs
    Path(DATA_DIR, name, 'data').mkdir(parents=True)
    Path(DATA_DIR, name, 'raw-data').mkdir(parents=True)
    Path(DATA_DIR, name, 'scripts').mkdir(parents=True)

    with open(Path(DATA_DIR, name, 'README.md'), 'x') as f:
        f.write(
            README_STUB.format(name_title=name.title().replace('_', ' '), name=name)
        )

    with open(Path(DATA_DIR, name, 'scripts', 'clean.py'), 'x') as f:
        f.write(CLEAN_STUB.format(name=name))

    with open(Path(DATA_DIR, name, 'scripts', 'fetch.py'), 'x') as f:
        f.write(FETCH_STUB)

    with open(Path(DATA_DIR, name, 'scripts', '__init__.py'), 'x') as f:
        f.write(INIT_STUB)

    print("Done! An empty dataset directory has been initialized at:")
    print(f"\t{Path(DATA_DIR, name)}")

    print(
        "\n### DRY RUN ###\nOnly dry runs of this helper function are allowed in the"
        f" reduced verion of this application, {Path('data-library', name)} was"
        " immediately unlinked."
    )
    shutil.rmtree(Path(DATA_DIR, name))
