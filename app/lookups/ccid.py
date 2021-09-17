"""Utility functions for working with Climate Cabinet ID's.

The Climate Cabinet ID, or CCID, is Climate Cabinet's official specification for giving each
region a unique identifying code built out of meanigful sub-codes. We're aiming to keep CCID's
as similar to the US Census's GeoID system as possible. There are a small number of cases where
GeoID's can collide, though, which brings about the need for our own ID system (namely, state
legislative districts - both upper and lower - and counties all have indentically structured
GeoID's, so we've added a 'U' to the end of SLDU district codes and an 'L' to the end of SLDL
district codes to differentiate). A full, detailed specification for CCID's can be
found here -> is.gd/qoVlbt

Todo:
    * refactor leveraging the us package for state fips codes
    * refactor using census package to put everything onto a table-lookup basis
    * documentation
        * docstring for each CCID class
        * docstring for each VALID_RAW_PATTERN
        * doctring for each CCID class private member function
        * docstring for assemble_ccid

"""
import re
import us
import json
from addfips import AddFIPS
from app.models import RegionType
from app.config import IRREGULAR_DISTRICT_STATES, IRREGULAR_CCID_OUTPUT


class BaseCCID:
    VALID_RAW_STATE_PATTERN = r"^\d{1,2}$"

    def __init__(self, raw_reg, raw_state=None):
        self.code = self._clean_raw_input(raw_reg, raw_state)

    def _clean_state_input(self, raw_state):
        if re.match(self.VALID_RAW_STATE_PATTERN, str(raw_state)):  # try to handle it as a code
            return str(raw_state).zfill(2)

        try:  # try to handle it as a name
            assert(found_fips := AddFIPS().get_state_fips(raw_state))  # prevent returning if None
            return found_fips
        except (AssertionError, AttributeError):
            pass

        raise ValueError(
            f"CCID Assemble Error - could not interpret '{raw_state}' as a valid State FIPS code, "
            f"USPS abbreviation, or State name."
        )

    def _clean_raw_input(self, raw_reg, raw_state):
        if not isinstance(raw_reg, (int, str, float)):
            raise ValueError(
                f"CCID Assemble Error - invalid region fips code or name passed. '{raw_reg}' must "
                f"be of type int or str, but is type '{type(raw_reg)}.'"
            )

        if isinstance(raw_reg, float) and raw_reg - int(raw_reg):
            raise ValueError(
                f"CCID Assemble Error - invalid region fips code or name passed. If '{raw_reg}' "
                f"is a float, it must not have a fractional component."
            )

        if not isinstance(raw_state, (int, str, type(None))):
            raise ValueError(
                f"CCID Assemble Error - invalid state fips code or name passed. '{raw_state}' "
                f"must be an int, str, or None, but is type '{type(raw_state)}.'"
            )

        state = self._clean_state_input(raw_state) if raw_state else None

        if re.match(self.VALID_RAW_PATTERN, str(raw_reg)):  # try to handle it as a code
            return self._handle_as_fips_code(raw_reg, state)

        if (found_fips := self._handle_as_name(raw_reg, state)):  # try to handle as a name
            return found_fips

        self._raise_invalid_input_error(raw_reg, raw_state)

    @classmethod
    def _is_valid_ccid_format(cls, ccid):
        if not isinstance(ccid, str):
            raise TypeError("CCID value must be a string.")

        if len(ccid) not in (2, 4, 5, 6):
            raise ValueError(
                f"Invalid CCID value - '{ccid}' must be a string of length 2, 4, 5, or 6."
            )

        state = ccid[:2]
        reg = ccid[2:5]
        sld_letter = ccid[5:6]

        if not state.isdigit() or not 1 <= int(state) <= 95:
            raise ValueError(
                f"Invalid CCID value - '{ccid}' must begin with a 2-digit state FIPS code between "
                f"1 (Alabama) and 95 (Palmyra Atoll)."
            )

        if reg and not re.match(r"(^\d{2,3}$|^\d{2}[A,B,C]$|^Z{3}$)", reg):
            raise ValueError(
                f"Invalid CCID value - '{ccid}' must contain a 3-digit, region-specific FIPS code "
                f"after the first 2-digits representing a county or voting district (or 'ZZZ' for "
                f"US Census-created placeholder district)."
            )

        if sld_letter and sld_letter not in ('U', 'L'):
            raise ValueError(
                f"Invalid CCID value - CCID of length {len(ccid)} must end in a 'U' or 'L', and "
                f"must reprepresent a SLDU or SLDL district. This ccid - '{ccid}' - ends in a "
                f"{ccid[-1]}."
            )


class StateCCID(BaseCCID):
    VALID_RAW_PATTERN = r"^\d{1,2}$"
    FULL_LENGTH = 2

    def _handle_as_fips_code(self, raw_state, _):
        return str(raw_state).zfill(self.FULL_LENGTH)

    def _handle_as_name(self, raw_state, _):
        try:
            found_fips = AddFIPS().get_state_fips(raw_state)
        except AttributeError:
            found_fips = None

        return found_fips

    def _raise_invalid_input_error(self, raw_state, _):
        raise ValueError(
            f"CCID Assemble Error - could not interpret '{raw_state}' as a valid State FIPS code, "
            f"USPS abbreviation, or State name."
        )


class CountyCCID(BaseCCID):
    VALID_RAW_PATTERN = r"^\d{1,5}$"
    DIST_LENGTH = 3
    FULL_LENGTH = 5

    def _handle_as_fips_code(self, raw_reg, state):
        if len(str(raw_reg)) > self.FULL_LENGTH - 2:
            reg = str(raw_reg).zfill(self.FULL_LENGTH)

            if state and reg[:2] != state:
                raise ValueError(
                    f"CCID Assemble Error - ambigious input, provided region FIPS code "
                    f"'{reg}' begins with state FIPS '{reg[:2]},' but state FIPS '{state}' "
                    f"passed in as argument. If using the 'state' keyword, please ensure that "
                    f"county/district-specific positional argument is either a full FIPS code "
                    f"with matching state FIPS in first two positions, or does not contain "
                    f"the state FIPS in the first two characters."
                )

            return reg

        else:
            reg = str(raw_reg).zfill(self.DIST_LENGTH)
            return state + reg

    def _handle_as_name(self, raw_reg, state):
        try:
            found_fips = AddFIPS().get_county_fips(raw_reg, state)
        except AttributeError:
            found_fips = None

        return found_fips

    def _raise_invalid_input_error(self, raw_reg, raw_state):
        raise ValueError(
            f"CCID Assemble Error - could not interpret '{raw_reg}' as a valid County FIPS code "
            f"or County name in state '{raw_state}'."
        )


class CongrCCID(BaseCCID):
    VALID_RAW_PATTERN = r"(^\d{1,4}$|^\d{2}ZZ$)"
    DIST_LENGTH = 2
    FULL_LENGTH = 4

    def _handle_as_fips_code(self, raw_reg, state):
        if len(str(raw_reg)) > self.FULL_LENGTH - 2:
            reg = str(raw_reg).zfill(self.FULL_LENGTH)

            if state and reg[:2] != state:
                raise ValueError(
                    f"CongrCCID Assemble Error - ambigious input, provided district FIPS code "
                    f"'{reg}' begins with state FIPS '{reg[:2]},' but state FIPS '{state}' "
                    f"passed in as argument. If using the 'state' keyword, please ensure that "
                    f"the second positional argument passed into assemble is either a full FIPS "
                    f"code with matching state FIPS in first two positions, or does not contain "
                    f"the state FIPS in the first two characters."
                )

            return reg
        else:
            reg = str(raw_reg).zfill(self.DIST_LENGTH)
            if not state:
                raise ValueError(
                    f"CongrCCID Assemble Error - no 'state' keyword argument provided, only "
                    f"recieved district number '{reg}'."
                )
            return state + reg

    def _handle_as_name(self, raw_reg, state_fips):
        # handle 'at large' districts
        if re.sub(r"[^a-z]", "", raw_reg.lower()) == 'atlarge':
            # some "At Large" districts send non-voting reps, and have
            # a different FIPS code as a result
            non_voting = ('11', '60', '66', '69', '72', '78')
            return state_fips + ('00' if state_fips not in non_voting else '98')
        else:
            # handle full district names
            extracted_reg = raw_reg.strip().split(" ")[-1].zfill(self.DIST_LENGTH)

            if re.match(r"^\d{2}$", extracted_reg):
                return state_fips + extracted_reg

        return None  # if we've made it here, we cannot parse this region name input

    def _raise_invalid_input_error(self, raw_reg, raw_state):
        raise ValueError(
                f"CCID Assemble Error  - could not interpret '{raw_reg}' as a valid Congressional "
                f"district FIPS code or name in state '{raw_state}'. If trying to interpret "
                f"district numbers as English phrases (ie: 'Congressional District 3'), make sure "
                f"the district number is written in numerals and comes as the last word of the "
                f"phrase. If the district is an at-large district, please pass only the "
                f"words 'at large'. If the district is a 'ZZ' district, please use the characters "
                f"'ZZ' exactly."
            )


class StateLegCCID(BaseCCID):
    VALID_RAW_PATTERN = r"(" + r"|".join([
        r"^\d{1,5}$",  # standard form
        r"^\d{1,4}[A,B,C]$",  # standard form w/ trailing letter
        r"^0200[A-Z]",  # upper chamber in AK
        r"50[A-Z0-9-][A-Z0-9-][A-Z0-9]",  # upper and lower chambers in VT
        r"^\d{2}ZZZ$"  # land area of undefined district membership, added by US Census
    ]) + ")"

    DIST_LENGTH = 3
    FULL_LENGTH = 5
    SUFFIX = None

    def _handle_as_fips_code(self, raw_reg, state_fips):
        # convert to full FIPS code
        if len(str(raw_reg)) > self.FULL_LENGTH - 2:
            reg = str(raw_reg).zfill(self.FULL_LENGTH)

            if state_fips and reg[:2] != state_fips:
                raise ValueError(
                    f"StateLegCCID Assemble Error - ambigious input, provided district FIPS code "
                    f"'{reg}' begins with state FIPS '{reg[:2]},' but state FIPS '{state_fips}' "
                    f"passed in as argument. If using the 'state' keyword, please ensure that "
                    f"the second positional argument passed into assemble is either a full FIPS "
                    f"code with matching state FIPS in first two positions, or does not contain "
                    f"the state FIPS in the first two characters."
                )
        else:
            if not state_fips:
                raise ValueError(
                    f"StateLegCCID Assemble Error - no 'state' keyword argument provided, only "
                    f"recieved district number '{raw_reg}'."
                )

            reg = state_fips + str(raw_reg).zfill(self.DIST_LENGTH)

        if (  # check if this state and chamber combination is an irregular GEOID case
            (state := us.states.lookup(reg[:2])) in IRREGULAR_DISTRICT_STATES.keys() and
            getattr(RegionType, f"SLD{self.SUFFIX}").census in IRREGULAR_DISTRICT_STATES[state]
        ):
            with open(IRREGULAR_CCID_OUTPUT / f"{state.abbr}_SLD{self.SUFFIX}.py", 'r') as f:
                names_to_geoids = json.load(f)

            if reg not in names_to_geoids.values():
                raise ValueError(
                    f"StateLegCCID Assemble Error - {reg} must be a valid GEOID code for a ",
                    f"district in the state of {state.abbr}."
                )

        return reg + self.SUFFIX

    def _handle_as_name(self, raw_reg, state_fips):
        if (state := us.states.lookup(state_fips)) in IRREGULAR_DISTRICT_STATES.keys():
            # load the correct DIST_NAMES_TO_GEOID map
            with open(IRREGULAR_CCID_OUTPUT / f"{state.abbr}_SLD{self.SUFFIX}.py", 'r') as f:
                names_to_geoids = json.load(f)
            # clean raw_reg (if at all?)
            reg = raw_reg.lower().strip()
            # if it's in keys, return the result + self.suffix
            if reg in names_to_geoids.keys():
                return names_to_geoids[reg.strip()] + self.SUFFIX
        else:
            reg = raw_reg.strip().split(" ")[-1].zfill(self.DIST_LENGTH)

            # if reg.upper() == "ZZZ":
            #     return state + "ZZZ" + self.SUFFIX

            if re.match(r"(^\d{2}[A,B,C]$|^\d{3}$)", reg):
                return state_fips + reg + self.SUFFIX

        return None  # if we've made it here, we cannot parse this region name input

    def _raise_invalid_input_error(self, raw_reg, raw_state):
        raise ValueError(
                f"StateLegCCID Assemble Error  - could not interpret '{raw_reg}' as a valid State "
                f"Legislative district FIPS code or name in state '{raw_state}'. If trying to "
                f"interpret district numbers as English phrases (ie: 'State House District 3'), "
                f"make sure the district number is written in numerals and comes as the last word "
                f"of the phrase. If district is a 'ZZZ' district, please use the string 'ZZZ' "
                f"exactly."
            )


class StateLegUpperCCID(StateLegCCID):
    SUFFIX = "U"

class StateLegLowerCCID(StateLegCCID):
    SUFFIX = "L"


def break_ccid(ccid):
    BaseCCID._is_valid_ccid_format(ccid)
    if len(ccid) == 2:  # state id
        return (RegionType.STATE, ccid, None)
    elif ccid[-1] in ('U', 'L'):  # state leg district
        reg_type = RegionType.SLDU if ccid[-1] == 'U' else RegionType.SLDL
        return (reg_type, ccid[:2], ccid[2:-1])
    elif len(ccid) == 4:  # congressional district
        return (RegionType.CONGR, ccid[:2], ccid[2:])
    else:  # county district
        return (RegionType.COUNTY, ccid[:2], ccid[2:])


def assemble_ccid(reg_type, reg, state=None):
    """Assembles a CCID code for a region."""
    ccid = None

    if reg_type == RegionType.STATE:
        ccid = StateCCID(reg)

    elif reg_type == RegionType.COUNTY:
        ccid = CountyCCID(reg, state)

    elif reg_type == RegionType.CONGR:
        ccid = CongrCCID(reg, state)

    elif reg_type == RegionType.SLDU:
        ccid = StateLegUpperCCID(reg, state)

    elif reg_type == RegionType.SLDL:
        ccid = StateLegLowerCCID(reg, state)

    if not ccid:
        raise ValueError(
            f"CCID Assembly error - unable to interpet region type '{reg_type}' as a "
            f"valid RegionType Enum option."
        )

    return ccid.code
