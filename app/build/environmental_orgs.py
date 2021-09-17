"""Builds/ refreshes asthma data for every region in the database.

"""
import pandas as pd
from halo import Halo
from utils import switch_halo_icon, update_halo_base, update_halo_scroll
from app.models import State, EnvironmentalOrg
from app.config import EnvironmentalOrgsDataset as EOD

KEYS = EOD.Keys
spinner = Halo()


def refresh_environmental_orgs(states_to_skip):
    """ Refreshes the asthma counts """
    print("\n~~ Refreshing Environmental Orgs Data ~~")
    switch_halo_icon(spinner)
    spinner.start()

    update_halo_base(spinner, "Opening dataset")
    df = pd.read_csv(EOD.DATASET)
    df = df[~df[KEYS.STATE_ABBR].isin(states_to_skip)].reset_index(drop=True)

    update_halo_base(spinner, "Loading data from dataset")

    def handle_row(row, num_states):
        state = State.objects.get(state_abbr=row[KEYS.STATE_ABBR])
        state.update(
            push__environmental_organizations=EnvironmentalOrg(
                name=row[KEYS.NAME],
                website=row[KEYS.SITE]
            )
        )
        update_halo_scroll(spinner, f"Organizations loaded: {row.name}/{num_states}")
    df.apply(handle_row, args=[len(df)], axis=1)

    spinner.succeed("Done!")
