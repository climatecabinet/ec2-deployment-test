import us
import json
import pandas as pd
from flatten_json import flatten
from mongoengine.queryset.visitor import Q
from utils import get_drive_bot_client, get_sheets_bot_client
from app import ClimateCabinetDBManager as CCDBM
from app.config import HELPERS_EXPORT_DIR, GEN_USER
from app.models import Session, ReprRole

MANUAL_VOTING_WKBKS_DIR = "1KAKYyH3Gny_47_s7BZ2GiQSmHN1Qivab"


def generate_manual_voting_reps_list():
    with CCDBM(GEN_USER, local=True, quiet=True):
        HELPERS_EXPORT_DIR.mkdir(exist_ok=True, parents=True)

        # get a google sheets client for grabbing workbooks
        sheets = get_sheets_bot_client()

        # grab every state sheet in the folder that has a usps abbr for a name
        drive = get_drive_bot_client()
        all_wbkbs_meta = drive.files().list(
            q=f"'{MANUAL_VOTING_WKBKS_DIR}' in parents and trashed = false",
            fields='files(name, id)'
        ).execute()['files']

        state_wkbks_metas = list(filter(
            lambda f: f['name'] in [s.abbr for s in us.STATES], all_wbkbs_meta
        ))

        for state_meta in state_wkbks_metas:
            print(f"Generating state reps lists for {state_meta['name']}")

            wkbk = sheets.open_by_key(state_meta['id'])

            for sheet in [s for s in wkbk.worksheets() if s.title != 'background']:
                # pull the session
                year_start, category = sheet.title.split(' ', 1)
                year_start = year_start.split('-')[0]
                session = Session.objects.get(
                    Q(state_abbr=state_meta['name']) &
                    (Q(year_start=int(year_start)) | Q(year_end=int(year_start))) &
                    Q(category=category)
                )

                reps_columns = ['last_name', 'first_name', 'office_district_ccid', 'ballotpedia']
                all_df = pd.DataFrame(columns=reps_columns)

                # pull all the reps for that session, one chamber at a time
                for rep_type in [ReprRole.REP.full, ReprRole.SEN.full]:
                    df = pd.DataFrame(map(
                        lambda rep: {
                            k: v
                            for k, v in flatten(json.loads(rep.to_json())).items()
                            if k in reps_columns
                        },
                        [
                            r.representative
                            for r in session.representatives
                            if r.role == rep_type
                        ]
                    ))

                    all_df = all_df.append(
                        df.sort_values(by='last_name'),
                        ignore_index=True
                    )

                output = f"{state_meta['name']}_{year_start}_{category.replace(' ', '_')}.csv"
                all_df.to_csv(HELPERS_EXPORT_DIR / output, index=False)

        print(f"Done! Outputs can be found at:\n\t{HELPERS_EXPORT_DIR}")
