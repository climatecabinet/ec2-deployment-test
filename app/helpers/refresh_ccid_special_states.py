"""Refreshes the district-name-to-geoid data structures underlying the CCID lookup class.
"""
import re
import json
from census import Census
from functools import partial
from num2words import num2words
from unidecode import unidecode
from app.models import RegionType
from app.config import IRREGULAR_DISTRICT_STATES, IRREGULAR_CCID_OUTPUT

CENSUS_API_KEY = "305e67b21181b00e358685e2c58753363954620c"

ORDINAL_PHRASES = [
    ('first', '1st'),
    ('second', '2nd'),
    ('third', '3rd'),
    ('fourth', '4th'),
    ('fifth', '5th'),
    ('sixth', '6th'),
    ('seventh', '7th'),
    ('eight', '8th'),
    ('ninth', '9th'),
    ('tenth', '10th'),
    ('eleventh', '11th'),
    ('twelfth', '12th'),
    ('thirteenth', '13th'),
    ('fourteenth', '14th'),
    ('fifteenth', '15th'),
    ('sixteenth', '16th'),
    ('seventeenth', '17th'),
    ('eighteenth', '18th'),
    ('ninteenth', '19th'),
    ('twentieth', '20th'),
    ('twenty', '20'),
    ('thirty', '30'),
    ('fourty', '40'),
    ('fifty', '50'),
    ('sixty', '60'),
    ('seventy', '70'),
    ('eighty', '80'),
    ('ninty', '90'),
]


def get_possible_names_list(raw_name):
    """Returns a list of acceptable permutations of the district name.
    """
    def _ordinal_words_to_numerals(name):
        # replace every non-ordinal with its numerical equivalent
        for word, num in ORDINAL_PHRASES:
            name = name.replace(word, num)

        if (compounds := re.findall(r'[\d]{2}[-\s]\d+(st|nd|rd|th)', name)):
            for match, _ in compounds:
                tens, ones = re.split(r'[-\s]', re.sub(r'[a-z]', '', name))
                name = name.replace(match, f"{tens+ones}{match[-2:]}")

        return name

    def _ordinal_numerals_to_words(name):
        result = re.findall(r'(\d+(st|nd|rd|th))', name)
        for match, _ in result:
            num = int(match[:-2])
            name = name.replace(match, num2words(num, ordinal=True))
        return name

    def _apply_name_cleaning_funcs(name, funcs):
        if len(funcs) <= 1:
            return [name, funcs[0](name).strip()]

        else:
            post_func_name = funcs[0](name).strip()
            return (
                _apply_name_cleaning_funcs(name, funcs[1:]) +
                _apply_name_cleaning_funcs(post_func_name, funcs[1:])
            )

    raw_name = raw_name.lower()
    funcs = []

    if unidecode(raw_name) != raw_name:
        funcs.append(unidecode)

    if re.search(r'(state|house|senate|district)', raw_name):
        funcs.append(partial(re.sub, r'(state|house|senate|district)', ''))

    # if 'second' can go to '2nd' and etc.
    if _ordinal_words_to_numerals(raw_name) != raw_name:
        funcs.append(_ordinal_words_to_numerals)

    # if '2nd' can go to 'second' and etc.
    if _ordinal_numerals_to_words(raw_name) != raw_name:
        funcs.append(_ordinal_numerals_to_words)

    # replace & with 'and'

    return _apply_name_cleaning_funcs(raw_name, funcs)


def refresh_ccid_special_states():
    c = Census(CENSUS_API_KEY)

    for state, dist_types in IRREGULAR_DISTRICT_STATES.items():
        dist_types = [RegionType.fuzzy_cast(rt) for rt in dist_types]

        dist_names_to_geoid = {}

        for dist_type in dist_types:
            resp_dists = c.acs5.get(['NAME', 'GEO_ID'], geo={
                'for': f'{dist_type.census}:*',
                'in': f'state:{state.fips}'
            })

            for dist in resp_dists:
                geoid = dist['GEO_ID'].split('US')[-1]
                raw_name = re.split(
                    r'\(([\d]{4}|[\d]{2,3}th Congress)\)[,;]', dist['NAME']
                )[0].strip()

                for name in get_possible_names_list(raw_name):
                    dist_names_to_geoid[name] = geoid

            out_path = IRREGULAR_CCID_OUTPUT / f'{state.abbr}_{dist_type.name}.py'

            with open(out_path, 'w') as f:
                json.dump(dist_names_to_geoid, f, indent=4)
                f.write("\n")
