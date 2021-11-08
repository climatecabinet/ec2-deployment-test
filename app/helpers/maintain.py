import configparser
from pathlib import Path
from app.config import (
    DATA_DIR,
    CLI_FETCH_CLEAN_ENTRY_NAMES,
)
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from importlib import import_module

HISTORY_CONFIG_FILENAME = "maintenance_history.cfg"
DATA_CONFIG_FILENAME = "maintenance.cfg"
SUPPORTED_OPS = ["fetch", "manual_fetch"]

due_for_manual = [];

def maintain():
    # we loop through all fetch/clean candidates and check which needs maintenance
    for entry in CLI_FETCH_CLEAN_ENTRY_NAMES:
        print("--------")
        print("processing "+entry+"...")

        # we fetch the maintenance interval settings for this data pipeline
        # these are stored in config files in each data folder, and are manually set/configured for each data pipeline.
        # look inside a config file for details, eg data-library/asthma/maintenance.cfg
        settings = configparser.ConfigParser()
        settings.read(Path(DATA_DIR, entry, DATA_CONFIG_FILENAME))

        # we store history of ops run in this process as a simple text config in the respective data folders
        # this is completely managed by the process, no human modifications anticipated
        # ergo we don't track these in git (or should we?)
        history = configparser.ConfigParser()
        history.read(Path(DATA_DIR, entry, "data", HISTORY_CONFIG_FILENAME))

        # check if we have records of the last run
        if 'history' in history: # we do
          for op in SUPPORTED_OPS:
              if op + '_interval' in settings: # we have a supported op with interval settings
                  if op in history['history']:
                      # get the last time we ran this op
                      lastop = datetime.strptime(history['history'][op], '%Y-%m-%d').date()
                      print("last " + op + " was at " + str(lastop))
                      # calc next time we ought to run this op
                      nextop = calc_next_date(lastop, settings[op + '_interval'])
                      print("next " + op + " is " + str(nextop));
                      # see if we're due
                      if date.today() >= nextop: # we are!
                          print("performing " + op + "...")
                          do_op(op, entry)
                          if op != "manual_fetch": # don't mark for manual fetch - remind instead!
                              record_op(op, history, date.today())
                              update_history(history, entry)
                      # else no action required
                  # no history entry for this op, we go ahead and do it once
                  else:
                      print("missing history entry for " + op)
                      print("performing " + op + "...")
                      do_op(op, entry)
                      if op != "manual_fetch":
                          record_op(op, history, date.today())
                          update_history(history, entry)
              # no interval settings means op unsupported, do nothing

        # no known history, so we do everything for the first time
        else:
          print("first run! doing everything once...")
          history['history'] = {}
          for op in SUPPORTED_OPS:
              # not all pipelines support all ops
              # we only run if an interval setting is present in the config
              if op + '_interval' in settings:
                  print("performing " + op + "...")
                  do_op(op, entry)
                  if op != "manual_fetch":
                      record_op(op, history, date.today())
                      update_history(history, entry)
              # else do nothing
          update_history(history, entry)

    # at the end, we print out an exhortation for manual fetches that are due and hope it is heeded :)
    if len(due_for_manual) > 0:
        print("\n--------------------------------------\n")
        print(" WARNING: The following pipelines are due for a manual fetch:\n")
        for item in due_for_manual:
          print(" * " + item)
        print("\n Once fresh data is added to ./data-library/<item>/raw-data, run 'clean' as necessary, then run 'python run.py mark_fetched <item>' to remove that item from this warning.")
        print("\n--------------------------------------\n")


# this function updates the maintenance history record for pipeline "item",
# marking it as if operation "op" has been performed today
# this is now called after manual fetches/cleans by default, and is
# also used to update maintenance status for manual fetch items
def mark_op(op, item):
    if item in CLI_FETCH_CLEAN_ENTRY_NAMES:
        history = configparser.ConfigParser()
        history.read(Path(DATA_DIR, item, "data", HISTORY_CONFIG_FILENAME))
        record_op(op, history, date.today())
        update_history(history, item)
        print("Data pipeine " + item + " has been marked with '" + op + "'!")
    else:
        print("Invalid pipeline name, aborting!")


def calc_next_date(last_done, settings):
    next_do = last_done
    next_do += timedelta(days=int(settings['days']))
    next_do += timedelta(days=(7*int(settings['weeks'])))
    next_do += relativedelta(months=int(settings['months']))
    next_do += relativedelta(years=int(settings['years']))
    return next_do

def update_history(history, entry):
    with open(Path(DATA_DIR, entry, "data", HISTORY_CONFIG_FILENAME), 'w') as configfile:
        history.write(configfile)

def do_op(op, entry):
    data_scripts = import_module(f'data-library.{entry}.scripts')
    if op == "fetch":
        data_scripts.fetch()
        data_scripts.clean([]) # also clean immediately after fetch
    if op == "manual_fetch": # we can't do manual fetch, this basically just means due for a reminder
        print(entry + " is due for manual fetching!")
        due_for_manual.append(entry) # put it in the list for the reminder/warning at the end


def record_op(op, history, rundate):
    history['history'][op] = str(rundate)
    if op == "fetch":
        history['history']['clean'] = str(rundate) # we do clean concurrently with fetches
