"""The main database manager class.
"""
import re
from datetime import datetime
from getpass import getpass
from mongoengine import connect, disconnect, get_connection
from pymongo.errors import OperationFailure
from utils import print_cr

from app.config import (
    ATLAS_URI,
    SKIP_STATES,
    TARGET_STATES,
    GEN_WELCOME,
    BUILD_USER,
    GEN_USER,
    GEN_PWD,
    ALL_STATES,
)
from app.build import refresh_tiger, refresh_asthma, refresh_daily_kos, refresh_jobs


class ClimateCabinetDBManager:
    def __init__(self, user, db_name=None, ensure_db=False, local=False, quiet=False):
        self._ensure_db = ensure_db
        self._local = local
        self._quiet = quiet
        self.db_name = db_name
        self.user = user

        if not local:
            if user == BUILD_USER:
                print(
                    "Connecting to this database requires the adminstrative "
                    "password... \n(please enter below)"
                )
                self.pwd = getpass()
            elif user == GEN_USER:
                self.pwd = GEN_PWD
            else:
                raise ValueError(
                    "CCDB Manager Error - unable to connect to Atlas cluster with "
                    f"username '{user}'. Valid options include '{BUILD_USER}' "
                    f"or '{GEN_USER}'."
                )

    def __enter__(self):
        self.connect(quiet=self._quiet)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect(quiet=self._quiet)

    def _get_skip_states(self, targets_only):
        return (
            [s for state in ALL_STATES - TARGET_STATES for s in state]
            if targets_only
            else [s for state in SKIP_STATES for s in state]
        )

    def _get_production_db_name(self):
        """Finds and returns the name of the current production database.

        All production databases following a specific naming convention, which is
        the world 'production' followed by the date the database was built and deployed,
        all of which is separated by dashes. The database on the cloud server with
        the most recent build date is considered the current production database.
        """
        connect(
            host=(
                "mongodb://127.0.0.1:27017/TEMP"
                if self._local
                else ATLAS_URI.format(usr=self.user, pwd=self.pwd, db="TEMP")
            )
        )

        dbs = list(
            filter(
                lambda db: re.match(r'production-[\d]{1,2}-[\d]{1,2}-[\d]{4}', db),
                get_connection().list_database_names(),
            )
        )

        disconnect()

        if not len(dbs):  # if no production databases found, raise an error
            raise ValueError(
                "CCDB Manager Error - unable to find a"
                f" {'local' if self._local else 'remote'} production database with"
                " correct name formatting."
            )

        return sorted(
            dbs,
            key=lambda db: datetime(
                year=int(db.split('-')[3]),
                month=int(db.split('-')[1]),
                day=int(db.split('-')[2]),
            ),
        ).pop()

    def connect(self, quiet=False):
        if not quiet:
            print(GEN_WELCOME)

        # if no db name is provided, get the current production database from local or cloud
        if not self.db_name:
            self.db_name = self._get_production_db_name()

        connect(
            host=(
                f"mongodb://127.0.0.1:27017/{self.db_name}"
                if self._local
                else ATLAS_URI.format(usr=self.user, pwd=self.pwd, db=self.db_name)
            )
        )

        try:  # ensure that we've successfully connected to the cluster
            get_connection().server_info()
        except OperationFailure:
            raise Exception(
                "\n\nCCDB Manager Error - unable to connect to database "
                f"'{self.db_name}' with credentials provided. Please "
                "check password and try again."
            )

        # if necessary, make sure the database we're connecting to exists
        if (
            self._ensure_db
            and self.db_name not in get_connection().list_database_names()
        ):
            raise Exception(
                "\n\nCCDB Manager Error - no database by the name "
                f"'{self.db_name}' currently exists."
            )

        if not quiet:
            print(
                f"Connection established with {'local' if self._local else 'remote'} "
                f"database:\n\t{self.db_name}"
            )

    def disconnect(self, quiet=False):
        if not quiet:
            print("\nDisconnecting from the database.")
        disconnect()

    def build(self, datasets=None, targets_only=None, slim=None):
        states_to_skip = self._get_skip_states(targets_only)

        print(f"\nDatabase build beginning at {(start := datetime.now())}")

        refresh_tiger(states_to_skip)
        refresh_environmental_orgs(states_to_skip)
        refresh_daily_kos(states_to_skip)
        refresh_asthma(states_to_skip)
        refresh_jobs(states_to_skip)

        print(
            f"\nDatabase build ending at {datetime.now()}, a total "
            f"runtime of {datetime.now() - start}\n"
        )

    def refresh(self, datasets, targets_only, slim):
        states_to_skip = self._get_skip_states(targets_only)

        print(f"\nDatabase build beginning at {(start := datetime.now())}")

        if 'environmental_orgs' in datasets:
            refresh_environmental_orgs(states_to_skip)
        if 'daily_kos' in datasets:
            refresh_daily_kos(states_to_skip)
        if 'asthma' in datasets:
            refresh_asthma(states_to_skip)
        if 'jobs' in datasets:
            refresh_jobs(states_to_skip)

        print(
            f"\nDatabase build ending at {datetime.now()}, a total "
            f"runtime of {datetime.now() - start}\n"
        )
