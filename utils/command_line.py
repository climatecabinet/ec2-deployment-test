import re
import sys
from random import randint


def get_user_choices(choices, prompt, normalize=None, initial=None):
    """ Prompts a user to choose values from a list, and returns the user's choices."""
    result = set([normalize(i) for i in initial]) if initial is not None else {}
    separator_length = len(prompt.split('\n')[-1]) + 30

    while(True):
        if result and result <= set(choices):
            return list(result)
        elif result:
            rejects = [r for r in result if r not in choices]
            print(f"\nOops! We're unable to interpret the following values:\n"
                  f"{', '.join(rejects)}\n\nlet's try that again...")

        print("\n" + prompt +
              " Choose from the following list...\n" +
              f"-\\-\\{'-' * separator_length}\n\n" +
              "\n".join(choices) + "\n"
              )
        raw = input(">>> ")
        result = set(map(lambda s: normalize(s.strip()) if normalize else s.strip(),
                         re.split(r"[, ]+", raw.strip())))


def print_cr(msg):
    sys.stdout.write("\033[K")
    print(msg, end='\r')


def print_scroller(label, count):
    print(f"{count}", end=f"\r{label}", flush=False)


def print_warning(s):
    print(f"\n\t\033[93m{s}\033[0m\n")


def print_fail(s):
    print(f"\n\t\033[91m{s}\033[0m\n")


def switch_halo_icon(spinner):
    options = ['earth', 'moon']
    spinner.spinner = options[randint(0, len(options)-1)]


def update_halo_base(spinner, base):
    spinner.text = base


def update_halo_scroll(spinner, scroll):
    base = spinner.text.split(' ~ ')[0]
    spinner.text = base + f" ~ {scroll}"
