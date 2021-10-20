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
SUPPORTED_OPS = ["fetch", "clean"]

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
        history.read(Path(DATA_DIR, entry, HISTORY_CONFIG_FILENAME))

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
                          history['history'][op] = str(date.today())
                          update_history(history, entry)
                      # else no action required
                  # no history entry for this op, we go ahead and do it once
                  else:
                      print("missing history entry for " + op)
                      print("performing " + op + "...")
                      do_op(op, entry)
                      history['history'][op] = str(date.today())
                      update_history(history, event)
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
                  history['history'][op] = str(date.today())
                  update_history(history, entry)
              # else do nothing
          update_history(history, entry)

def calc_next_date(last_done, settings):
    next_do = last_done
    next_do += timedelta(days=int(settings['days']))
    next_do += timedelta(days=(7*int(settings['weeks'])))
    next_do += relativedelta(months=int(settings['months']))
    next_do += relativedelta(years=int(settings['years']))
    return next_do

def update_history(history, entry):
    with open(Path(DATA_DIR, entry, HISTORY_CONFIG_FILENAME), 'w') as configfile:
        history.write(configfile)

def do_op(op, entry):
    data_scripts = import_module(f'data-library.{entry}.scripts')
    if op == "fetch":
        data_scripts.fetch()
    elif op == "clean":
        data_scripts.clean([])
