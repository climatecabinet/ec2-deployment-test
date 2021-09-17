"""Fetches the Google Sheet listing environmental state groups and saves it locally.
"""
from utils import get_sheets_bot_client
from app.config import EnvironmentalOrgsDataset as EOD

WORKBOOK_FILE_ID = '1cPE7LE-4b_Hf9PU9UW41r90155zolbtI1hxnVA9rBKA'
WORKBOOK_NAME = 'State Scorecard Source List'
SHEET_NAME = 'Sheet1'


def fetch():
    client = get_sheets_bot_client()

    wkbk = client.open_by_key(WORKBOOK_FILE_ID)

    df = wkbk.worksheet_by_title(SHEET_NAME).get_as_df()
    df.to_csv(EOD.RAW_DATASET, index=False)
