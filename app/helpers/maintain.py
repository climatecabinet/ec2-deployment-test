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
exception_list = [];

# this function is meant to be run regularly as on a cron
# will check through the maintenance status for all SUPPORTED_OPS on all pipelines
# running those that are due, printing reminders, recording exceptions
# and updating the maintenance status records with any changes
def maintain():
    # grab the summarized status from the config files
    ret = get_maintain_status()

    # loop through each pipeline
    for entry in ret:
        # we initialize the history config file object
        # needed because we may need to update them
        # TODO: maybe move this so we don't directly reference any config files in maintain()?
        history = configparser.ConfigParser()
        history.read(Path(DATA_DIR, entry, "data", HISTORY_CONFIG_FILENAME))
        print("--------")
        print("processing "+entry+"...")

        # loop through each supported op
        for op in ret[entry]:
            if 'next_op' in ret[entry][op]: # we have a supported op with interval settings
                # see if we're due
                if date.today() >= ret[entry][op]['next_op']: # we are!
                    print("performing " + op + "...")
                    try:
                        do_op(op, entry)
                        if op != "manual_fetch": # don't mark for manual fetch - remind instead!
                            record_op(op, history, date.today())
                            update_history(history, entry)
                    except Exception as e:
                        print("**Encountered exception: " + str(e))
                        # append to exception list so we can summarize at the end
                        exception_list.append("Exception encountered while running '" + op + \
                                                  "' on '" + entry + "': " + str(e))
                        record_exception(e, op, history, date.today())
                        update_history(history, entry)
                else:
                    print("not due, no action required")
            # else op isn't supported/configured so no action required

    # at the end, we print out an exhortation for manual fetches that are due and hope it is heeded :)
    if len(due_for_manual) > 0:
        print("\n--------------------------------------\n")
        print(" WARNING: The following pipelines are due for a manual fetch:\n")
        for item in due_for_manual:
          print(" * " + item)
        print("\n Once fresh data is added to ./data-library/<item>/raw-data, run 'clean' as necessary, then run 'python run.py mark_fetched <item>' to remove that item from this warning.")
        print("\n--------------------------------------\n")

    # we also print out a summary of exceptions encountered in the current interation
    if len(exception_list) > 0:
        print("\n--------------------------------------\n")
        print(" ERROR: The following operations encountered unexpected exceptions:\n")
        for item in exception_list:
          print(" * " + item)
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

# this function prints out a readable summary of the maintenance status
# of the pipelines - last successful, last exceptions, next due, etc
def print_maintain_status(printformat):
    ret = get_maintain_status()
    if printformat == "html":
        print("not yet supported")
    elif printformat == "text":
        exceptions = {}
        for entry in ret:
            print("-------")
            print("Status for " + entry + ":")
            print("-------")
            for op in ret[entry]:
              if 'last_op' in ret[entry][op]:
                  print("last successful '" + op + "' was on " + str(ret[entry][op]['last_op']))
              if 'next_op' in ret[entry][op]:
                  print("next '" + op + "' due on " + str(ret[entry][op]['next_op']))
                  if date.today() >= ret[entry][op]['next_op']:
                      print("!!! DUE !!!")
              if 'last_exception' in ret[entry][op]:
                  if ('last_op' not in ret[entry][op]) or (ret[entry][op]['last_exception'] > ret[entry][op]['last_op']):
                      print("** An exception was encountered running '" + op + \
                                  "' on " + str(ret[entry][op]['last_exception']))
                      print("** Exception details: " + ret[entry][op]['last_exception_details'])
            print("\n")

# this function scours the maintenance config files and loads up all the information
# for use in the other functions
def get_maintain_status():
    ret = {}
    for entry in CLI_FETCH_CLEAN_ENTRY_NAMES:
        # we fetch the maintenance interval settings for this data pipeline
        # these are stored in config files in each data folder, and are manually set/configured for each data pipeline.
        # look inside a config file for details, eg data-library/asthma/maintenance.cfg
        settings = configparser.ConfigParser()
        settings.read(Path(DATA_DIR, entry, DATA_CONFIG_FILENAME))

        # we store history of ops run in this process as a simple text config in the respective data folders
        # this is completely managed by the process, no human modifications anticipated
        # ergo we don't track these in git.
        # the most recent exceptions encountered, if applicable, is also recorded here
        history = configparser.ConfigParser()
        history.read(Path(DATA_DIR, entry, "data", HISTORY_CONFIG_FILENAME))

        ret[entry] = {}
        # find run history
        if 'history' in history:
            for op in SUPPORTED_OPS:
                ret[entry][op] = {}
                if op in history['history']:
                    ret[entry][op]['last_op'] = datetime.strptime(history['history'][op], '%Y-%m-%d').date()
            # clean has history recorded, but is not a supported op.
            # clean is always just run after fetch
            if 'clean' in history['history']:
                ret[entry]['clean'] = {}
                ret[entry]['clean']['last_op'] = datetime.strptime(history['history']['clean'], '%Y-%m-%d').date()

        # figure out next run
        # basically adding configured interval with last run info from above
        for op in SUPPORTED_OPS:
            if op + '_interval' in settings:
                if not op in ret[entry]:
                    ret[entry][op] = {}
                if 'last_op' in ret[entry][op]:
                    ret[entry][op]['next_op'] = calc_next_date(ret[entry][op]['last_op'], settings[op + '_interval'])
                else: # if no last run info, things are immediately due, so set to today
                    ret[entry][op]['next_op'] = date.today()

        # also grab exception history:
        if 'exception' in history:
            for op in SUPPORTED_OPS:
                if not op in ret[entry]:
                    ret[entry][op] = {}
                if op + '_date' in history['exception']:
                    ret[entry][op]['last_exception'] = \
                          datetime.strptime(history['exception'][op + '_date'], '%Y-%m-%d').date()
                    ret[entry][op]['last_exception_details'] = \
                          history['exception'][op + '_data']
    return ret

# rest are simple utility functions:
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

def record_exception(e, op, history, rundate):
    if 'exception' not in history:
        history['exception'] = {}
    history['exception'][op + '_date'] = str(rundate)
    history['exception'][op + '_data'] = str(e)
