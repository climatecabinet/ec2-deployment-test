import json
import sys
import pathlib
import pandas as pd
from mongoengine import DoesNotExist, MultipleObjectsReturned
from app.config import GEN_USER
from app import ClimateCabinetDBManager as CCDB
from app.models import Representative, RollCall, Region


def find_rep(rep_name):
    with CCDB(GEN_USER, db_name='production', ensure_db=True):
        print(f"\nAttempting to find '{rep_name}' in database...")

        try:
            rep = json.loads(Representative.objects.get(name=rep_name).to_json())
        except DoesNotExist:
            print(f"\nUh oh! Looks like we couldn't find a rep named '{rep_name}'. "
                  f"Please make sure your name is a (case-sensitive) match to the "
                  f"rep's name in the database and try again.")
            sys.exit()

        except MultipleObjectsReturned:
            print(f"\nCannot complete the request, multiple documents found with rep "
                  f"name '{rep_name}'")
            sys.exit()

        print("Found! Cleaning vote records...")

        rep['clean_votes'] = []
        for vote in rep['votes']:
            vote['district'] = Region.objects.only("ccid").get(id=vote['district']['$oid']).ccid
            roll_call = json.loads(RollCall.objects.get(id=vote['roll_call']['$oid']).to_json())
            vote = {**vote, **roll_call}

            vote['chambers'] = ", ".join(vote['chambers'])

            del vote['_id']
            del vote['roll_call']

            rep['clean_votes'].append(vote)

        new_file = pathlib.Path.home() / 'Desktop' / f'{"_".join(rep["name"].lower().split())}.csv'

        df = pd.DataFrame(rep['clean_votes'])
        df.to_csv(new_file)
        print(f"Done! File can be found at:\n\n\t{new_file}")
