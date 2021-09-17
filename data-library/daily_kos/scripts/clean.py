#!/usr/bin/env python
# coding: utf-8

import re
import pandas as pd
# import os, os.path
from pathlib import Path
import numpy as np
from app.config import DATA_RAW_PATH, DATA_CLEANED_PATH

SKIP_SHEETS = ['NC (2018)', 'VA (pre-2019)']
OUTPUT_ROOT = Path(DATA_CLEANED_PATH % 'daily_kos')

# Counties ↔ Congressional District correspondences
XLSX_CONGRESSIONAL = Path(DATA_RAW_PATH % 'daily_kos' +
                          "/Counties ↔ congressional district overlaps.xlsx")
XLSX_STATE = Path(DATA_RAW_PATH % 'daily_kos' +
                  "/Counties ↔ legislative district correspondences.xlsx")


CHUNK_TITLES_TO_OUTPUTS = {
    "Counties to Congressional Districts": ['counties-to-congressional-districts/%s.csv'],
    "Congressional Districts to Counties": ['congressional-districts-to-counties/%s.csv'],
    "Counties to State Senate Districts": ['counties-to-state-senate-districts/%s.csv'],
    "State Senate Districts to Counties": ['state-senate-districts-to-counties/%s.csv'],
    "Counties to State House Districts": ['counties-to-state-house-districts/%s.csv'],
    "State House Districts to Counties": ['state-house-districts-to-counties/%s.csv'],
    "Counties to Legislative Districts": ['counties-to-state-senate-districts/%s.csv',
                                          'counties-to-state-house-districts/%s.csv'],
    "Legislative Districts to Counties": ['state-senate-districts-to-counties/%s.csv',
                                          'state-house-districts-to-counties/%s.csv']
}

CORRESPONDENCES = {
    XLSX_CONGRESSIONAL: [('A:D', 'counties-to-congressional-districts/%s.csv'),
                         ('F:I', 'congressional-districts-to-counties/%s.csv')],
    XLSX_STATE: [('A:D', 'counties-to-state-senate-districts/%s.csv'),
                 ('F:I', 'state-senate-districts-to-counties/%s.csv'),
                 ('K:N', 'counties-to-state-house-districts/%s.csv'),
                 ('P:S', 'state-house-districts-to-counties/%s.csv')]
}


def clean_sheet_contents(sheet_name, df):
    """Automates data-level changes that would probably be done by hand"""

    # drop empty rows
    df = df.replace('', float('nan'))
    df = df.dropna(how='any')

    # clean the ".[number]" substrings out of column names
    df.columns = df.columns.map(lambda n: re.sub(r'.[1-9]+', '', n))

    # clean the newlines out of column names
    df.columns = df.columns.str.replace(r"\n", " ", regex=True)

    # ensure Senate Districts read as ints instead of floats
    if 'SD #' in df.columns and df['SD #'].dtype == float and not df['SD #'].isna().any():
        df = df.astype({'SD #': int})

    # drop extraneous index column (is.gd/7LuhHt)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # do some county-specific cleaning
    if 'County' in df.columns:
        # strip extraneous whitespace off the end of County names
        df['County'] = df['County'].apply(lambda n: n.strip() if isinstance(n, str) else n)

        # remove the space in the McKean, PA county name
        if sheet_name == "PA":
            df['County'] = df['County'].replace({'Mc Kean': 'McKean'})

        # correct the misspelling of some counties in NC
        if sheet_name == "NC":
            df['County'] = df['County'].replace({'Curriticuk': 'Currituck',
                                                 'Vae': 'Vance',
                                                 'Alamae': 'Alamance',
                                                 'Lioln': 'Lincoln',
                                                 'Buombe': 'Buncombe',
                                                 'Yaey': 'Yancey'})

        # remove the incorrect space in "LaSalle" County, IL
        if sheet_name == "IL":
            df['County'] = df['County'].replace({'La Salle': 'LaSalle'})

        # correct the misspelling of some counties in IN
        if sheet_name == "IN":
            df['County'] = df['County'].replace({'De Kalb': 'DeKalb',
                                                 'La Porte': 'LaPorte'})

        # insert a missing space in "De Baca" County, NM
        if sheet_name == "NM":
            df['County'] = df['County'].replace({'DeBaca': 'De Baca'})

        # correct the names of some boroughs in AK (county equivalent)
        if sheet_name == "AK":
            df['County'] = df['County'].replace({'Wrangell City and': 'Wrangell City and Borough',
                                                 'Anchorage Borough': 'Anchorage',
                                                 'Juneau Borough': 'Juneau',
                                                 'Sitka Borough': 'Sitka',
                                                 'Yakutat Borough': 'Yakutat'})

        if sheet_name == "VA":
            # clarify that 'Bedford' in VA refers to Bedford County, not Bedford City,
            # which was previously an independent city with a separate FIPS code from
            # the County of the same name.
            df['County'] = df['County'].replace({'Bedford': 'Bedford County',
                                                 'Bedford (R)': 'Bedford County'})

            # add the word "County" to all rows representing Counties, as opposed to independent
            # cities (helpful in daily_kos build later)
            df['County'] = df['County'].where(
                df['County'].str.lower().str.contains(r'city|county'),
                df['County'] + ' county'
            )

    # in VT's lower chamber, we need to add a hypen between words and numbers in district names
    if sheet_name == 'VT' and (dists := [d for d in ['HD #', 'SD #'] if d in df.columns]):
        for d in dists:
            df[d] = df[d].str.replace(' ', '-')

    # make sure 'At Large' districts are properly labelled
    if 'CD #' in df.columns and df['CD #'].dtype == np.object:
        df['CD #'] = df['CD #'].replace({'AL': 'at large'})

    return df


def get_sheet_chunks(df, state):
    """Returns a sliced-up version of the dataframe. keys are the output path of the chunk,
    and values are the actual dataframe chunk"""
    chunks = []

    # in LA, counties are referred to as 'parishes'
    df.columns = df.columns.str.replace(re.compile('(Parishes|Boroughs)'), 'Counties')

    # for simplicity's sake, replace instances of 'Assembly' with 'House'
    df.columns = df.columns.str.replace('Assembly', 'House')

    # NE's legislature has only an upper house and no lower house
    if state == 'NE':
        df.columns = df.columns.str.replace('Legislative', 'State Senate')

    for title in df.columns[~df.columns.str.match('Unnamed')]:
        col_loc = df.columns.get_loc(title)
        chunk = df.iloc[:, col_loc:col_loc+4]

        chunk.columns = chunk.iloc[0]
        chunk = chunk.drop(0)

        chunks.append((CHUNK_TITLES_TO_OUTPUTS[title], chunk))

    return chunks


def clean(_):
    for xlsx_path in (XLSX_CONGRESSIONAL, XLSX_STATE):
        df = pd.read_excel(xlsx_path, sheet_name=None)

        # make output directories
        for output in set(d for d_list in CHUNK_TITLES_TO_OUTPUTS.values() for d in d_list):
            (OUTPUT_ROOT / Path(output % 'foo')).parent.mkdir(parents=True, exist_ok=True)

        for sheet_name, sheet_content in df.items():
            if sheet_name in SKIP_SHEETS:
                continue

            # clean parentheticals off sheet name
            sheet_name = sheet_name.split()[0]

            chunks = [(outputs, clean_sheet_contents(sheet_name, chunk))
                      for outputs, chunk in get_sheet_chunks(sheet_content, sheet_name)]

            # Write
            for outputs, chunk in chunks:
                for output in outputs:
                    chunk.to_csv(OUTPUT_ROOT / Path(output % sheet_name), index=False)
