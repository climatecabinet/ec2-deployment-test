from __future__ import print_function
import io
import pickle
import pygsheets
from pathlib import Path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.metadata',
          'https://www.googleapis.com/auth/drive']

BOT_AUTH_SECRETS_PATH = Path(Path.cwd(), 'utils', 'credentials.json')
BOT_AUTH_TOKEN_PATH = Path(Path.cwd(), 'utils', 'token.pickle')


def get_creds():
    """Either loads pre-existing login credentials or guides the user through the login flow"""

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if BOT_AUTH_TOKEN_PATH.exists():
        with open(BOT_AUTH_TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(BOT_AUTH_SECRETS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(BOT_AUTH_TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)

    return creds


def check_for_secrets():
    if not BOT_AUTH_SECRETS_PATH.exists():
        raise Exception(
            f"Climate Cabinet Bot Error:\n\n"
            f"Unable to locate credentials file needed to run this function. If this is your \n"
            f"first time running the script, you'll need to download the 'credential.json' file \n"
            f"from the Climate Cabinet password management vault before proceeding. Reach out \n"
            f"to a CliCab admin for access and help making this happen. Once this file is \n"
            f"downloaded, please put it in the following location:\n\n\t"
            f"{BOT_AUTH_SECRETS_PATH}"
        )


def get_drive_bot_client():
    check_for_secrets()
    return build('drive', 'v3', credentials=get_creds())


def get_sheets_bot_client():
    check_for_secrets()
    return pygsheets.authorize(custom_credentials=get_creds())


def save_file(file_id, file_name, client, output):
    """is.gd/JVqv8S"""
    fh = io.BytesIO()
    request = client.files().get_media(fileId=file_id)

    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)

    with open((output_path := Path(output) / Path(file_name)), 'wb') as f:
        f.write(fh.read())

    return output_path
