""" A helper function for finding malformed bill entries in the clicab curation dataset.
"""
import re
import pandas as pd
from pathlib import Path
from importlib import import_module
from app import ClimateCabinetDBManager as CCDB
from app.config import GEN_USER, CliCabCurationDataset as CCCD
from app.build.clicab_curation import _get_bill_from_row

fetch_module = import_module('data-library.clicab_curation.scripts.fetch')
clean_module = import_module('data-library.clicab_curation.scripts.clean')
KEYS = CCCD.CliCabCurationKeys
RAW_DATA_PATH = Path.cwd() / 'app' / 'helpers' / 'clicab_curation_raw.csv'
PROBLEM_DATA_OUTPUT = Path.cwd() / 'app' / 'helpers' / 'test_clicab_curation_output.csv'


def find_raw_error_title(row):
    if not re.match(clean_module.VALID_BILL_NUM_REGEX, row[KEYS.BILL_NUM]):
        return "invalid bill number - cannot be cleaned programmatically"

    if row[KEYS.STATE] == "MA":
        return "cannot currently handle bills in MA"

    if not pd.isna(row[KEYS.AMENDMENT]) and pd.isna(row[KEYS.ROLL_CALLS]):
        return "cannot handle amendment bills without roll call ids specified"

    if pd.isna(row[KEYS.PRO_ENV]):
        return f"column '{KEYS.PRO_ENV}' must be specified"

    if pd.isna(row[KEYS.CHAMBER]):
        return f"column '{KEYS.CHAMBER}' must be specified"

    if (row[KEYS.BILL_NUM] in ["SCR39", "ACR127"] and row[KEYS.STATE] == "NJ"
            and row[KEYS.YEAR] == 2019):
        return "this bill needs manual review, possibly not environmental"

    return None


def find_clean_error_title(row):
    bill = _get_bill_from_row(row)

    if not bill.roll_calls:
        return "no roll call votes found for this bill in LegiScan database"

    return None


def find_errors(row, errors, error_title_func):
    if (error_title := error_title_func(row)):
        if error_title in errors.keys():
            errors[error_title].append(row.name)
        else:
            errors[error_title] = [row.name]


def test_clicab_curation(db=None, is_local=False):
    print("Fetching raw data...", end="\r")
    fetch_module.fetch(RAW_DATA_PATH)

    print("Finding errors in raw data...", end="\r")
    errors = {}
    raw = pd.read_csv(RAW_DATA_PATH)
    raw[KEYS.BILL_NUM_HEAD] = float('nan')
    raw[KEYS.BILL_NUM_TAIL] = float('nan')
    raw = raw.apply(clean_module.clean_row, axis=1)
    raw.apply(find_errors, args=[errors, find_raw_error_title], axis=1)

    print("Finding errors in clean data...", end="\r")
    clean = raw.drop([i for k in errors.keys() for i in errors[k]])
    db_man = CCDB(GEN_USER, db_name=db, local=is_local, quiet=True)
    db_man.connect(quiet=True)
    clean.apply(find_errors, args=[errors, find_clean_error_title], axis=1)
    db_man.disconnect(quiet=True)

    print(f"Dumping found errors to file in {str(PROBLEM_DATA_OUTPUT)}")
    raw['problem type'] = None
    for error_title in errors.keys():
        raw.loc[errors[error_title], 'problem type'] = error_title

    RAW_DATA_PATH.unlink()
    raw.to_csv(PROBLEM_DATA_OUTPUT)

    print(f"Done! Total number of errors found: {len([i for v in errors.values() for i in v])}")
