import us
import argparse
from haikunator import Haikunator
from importlib import import_module
from app.config import (
    BUILD_USER,
    GEN_USER,
    CLI_BUILD_ENTRY_NAMES,
    CLI_FETCH_CLEAN_ENTRY_NAMES,
    STATE_ABBR_TO_FIPS,
    ALL_STATES,
)
from app import ClimateCabinetDBManager as CCDB


def get_parsed_args():
    """Determines the subcommand and parses CLI arguments"""
    meta_parser = argparse.ArgumentParser()

    subs_desc = "the permitted operations performable through the run.py script"
    subparsers = meta_parser.add_subparsers(
        title='subcommand', dest="operation", description=subs_desc
    )

    # setup parser for building a new database
    new_db_parser = subparsers.add_parser('new-db', help='Builds a new database')
    new_db_parser.add_argument(
        "--target",
        '-t',
        action="store_true",
        help=(
            "if present, only the states present in the global list TARGET_STATES"
            " (found in config.py) are used to build/refresh the database."
        ),
    )
    new_db_parser.add_argument(
        "--local",
        "-l",
        action="store_true",
        help=(
            "if present, a connection is made with a database running on localhost, as"
            " opposed to the cloud prodcution database"
        ),
    )
    new_db_parser.add_argument(
        "--slim",
        "-s",
        action="store_true",
        help=(
            "if present, the database being built or refreshed should contain only"
            " environmental bills, as opposed to every bill in LegiScan's database"
            " since 2010."
        ),
    )
    new_db_parser.add_argument(
        "--database", '-db', help="the name of the Atlas database to connect to"
    )
    new_db_parser.add_argument(
        "--datasets",
        '-ds',
        choices=CLI_BUILD_ENTRY_NAMES,
        nargs="*",
        help="the datasets to refresh data from",
    )

    # setup parser for rebuilding a specific dataset
    refresh_parser = subparsers.add_parser(
        'refresh', help='Refreshes the data from a list of specific datasets'
    )
    refresh_parser.add_argument(
        "datasets",
        choices=CLI_BUILD_ENTRY_NAMES,
        nargs="*",
        help="the datasets to refresh data from",
    )
    refresh_parser.add_argument(
        "--target",
        '-t',
        action="store_true",
        help=(
            "if present, only the states present in the  global list TARGET_STATES"
            " (found in config.py) are used to build/refresh the database."
        ),
    )
    refresh_parser.add_argument(
        "--local",
        "-l",
        action="store_true",
        help=(
            "if present, a connection is made with a database running on localhost, as"
            " opposed to the cloud prodcution database."
        ),
    )
    refresh_parser.add_argument(
        "--slim",
        "-s",
        action="store_true",
        help=(
            "if present, the database being built or refreshed should contain only"
            " environmental bills, as opposed to every bill in LegiScan's database"
            " since 2010."
        ),
    )
    refresh_parser.add_argument(
        "--database",
        '-db',
        help="the name of the Atlas database to connect to",
        required=True,
    )

    # setup parser for running data fetching scripts (scraping/ downloading external data)
    fetch_parser = subparsers.add_parser(
        'fetch', help='Runs a data-library\'s fetching script.'
    )
    fetch_parser.add_argument("dataset", choices=CLI_FETCH_CLEAN_ENTRY_NAMES)

    # setup parser for running data cleaning scripts (raw-data --> data ready for db consumption)
    clean_parser = subparsers.add_parser(
        'clean', help='Runs a data-library\'s cleaning script.'
    )
    clean_parser.add_argument("dataset", choices=CLI_FETCH_CLEAN_ENTRY_NAMES)
    clean_parser.add_argument(
        "--states",
        "-s",
        nargs='*',
        choices=STATE_ABBR_TO_FIPS.keys(),
        default=[],
        help="the state(s) to clean data for",
    )

    flean_parser = subparsers.add_parser(
        'flean',
        help="Runs a data-library's fetching and cleaning scripts, in that order.",
    )
    flean_parser.add_argument('dataset', choices=CLI_FETCH_CLEAN_ENTRY_NAMES)
    flean_parser.add_argument(
        "--states",
        "-s",
        nargs='*',
        choices=STATE_ABBR_TO_FIPS.keys(),
        default=[],
        help="the state(s) to fetch and clean data for",
    )

    # setup parser for running helper functions
    util_parser = subparsers.add_parser(
        'helper', help='Runs a script from the helpers directory.'
    )
    util_parser.add_argument("func")

    return meta_parser.parse_known_args()


def parse_unknown_args(unknowns):
    kwds = {}
    flags = list(filter(lambda arg: arg.find("--") > -1, unknowns))

    for i, flag in enumerate(flags):
        f = unknowns.index(flag)
        n = unknowns.index(flags[i + 1]) if i < len(flags) - 1 else len(unknowns)

        vals = unknown[f + 1 : n]

        if len(vals) > 1:
            kwds[flag.replace('--', '')] = vals
        elif len(vals) == 1:
            kwds[flag.replace('--', '')] = vals.pop()
        else:
            kwds[flag.replace('--', '')] = True

        del unknown[f:n]

    return (unknown, kwds)


if __name__ == '__main__':
    args, unknown = get_parsed_args()

    if args.operation == 'new-db':
        db_name = (
            args.database if args.database else Haikunator().haikunate(token_length=0)
        )
        with CCDB(BUILD_USER, db_name=db_name, local=args.local) as db:
            db.build(datasets=args.datasets, targets_only=args.target, slim=args.slim)

    elif args.operation == 'refresh':
        with CCDB(
            BUILD_USER, db_name=args.database, ensure_db=True, local=args.local
        ) as db:
            db.refresh(datasets=args.datasets, targets_only=args.target, slim=args.slim)

    elif args.operation in ('fetch', 'clean', 'flean'):
        data_scripts = import_module(f'data-library.{args.dataset}.scripts')

        if args.operation in ('fetch', 'flean'):
            data_scripts.fetch()

        if args.operation in ('clean', 'flean'):
            targets_raw = [
                state for abbr in args.states for state in ALL_STATES if abbr in state
            ]
            target_states = [s for state in targets_raw for s in state]

            data_scripts.clean(target_states)

    elif args.operation == 'helper':
        helper_args, kwds = parse_unknown_args(unknown)
        helper = getattr(import_module("app.helpers"), args.func)
        helper(*helper_args, **kwds)
