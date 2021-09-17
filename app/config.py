import us
import os
from pathlib import Path

BUILD_USER = 'build-db'

GEN_USER = 'generate-docs'
GEN_PWD = 'OddPyre-GrassGem-1963'

ATLAS_URI = 'mongodb+srv://{usr}:{pwd}@cluster1.kmeus.mongodb.net/{db}?retryWrites=true&w=majority'

DEFAULT_BATCH_INSERT_SIZE = 500000

DATA_DIR = os.path.join(os.getcwd(), 'data-library')
DATA_SCRIPTS_PATH = os.path.join(DATA_DIR, '%s', 'scripts')
DATA_CLEANED_PATH = os.path.join(DATA_DIR, '%s', 'data')
DATA_RAW_PATH = os.path.join(DATA_DIR, '%s', 'raw-data')

# the names of data-libary entries that can be specifically refreshed
# during a database build/ refresh process
CLI_BUILD_ENTRY_NAMES = {'jobs'}

# the names of data-libary entries that have functional fetch.py and clean.py scripts
CLI_FETCH_CLEAN_ENTRY_NAMES = {'jobs', 'tiger'}

ALL_STATES = {
    ('AL', '01', 'Alabama'),
    ('AK', '02', 'Alaska'),
    ('AZ', '04', 'Arizona'),
    ('AR', '05', 'Arkansas'),
    ('CA', '06', 'California'),
    ('CO', '08', 'Colorado'),
    ('CT', '09', 'Connecticut'),
    ('DE', '10', 'Delaware'),
    ('FL', '12', 'Florida'),
    ('GA', '13', 'Georgia'),
    ('HI', '15', 'Hawaii'),
    ('ID', '16', 'Idaho'),
    ('IL', '17', 'Illinois'),
    ('IN', '18', 'Indiana'),
    ('IA', '19', 'Iowa'),
    ('KS', '20', 'Kansas'),
    ('KY', '21', 'Kentucky'),
    ('LA', '22', 'Louisiana'),
    ('ME', '23', 'Maine'),
    ('MD', '24', 'Maryland'),
    ('MA', '25', 'Massachusetts'),
    ('MI', '26', 'Michigan'),
    ('MN', '27', 'Minnesota'),
    ('MS', '28', 'Mississippi'),
    ('MO', '29', 'Missouri'),
    ('MT', '30', 'Montana'),
    ('NE', '31', 'Nebraska'),
    ('NV', '32', 'Nevada'),
    ('NH', '33', 'New Hampshire'),
    ('NJ', '34', 'New Jersey'),
    ('NM', '35', 'New Mexico'),
    ('NY', '36', 'New York'),
    ('NC', '37', 'North Carolina'),
    ('ND', '38', 'North Dakota'),
    ('OH', '39', 'Ohio'),
    ('OK', '40', 'Oklahoma'),
    ('OR', '41', 'Oregon'),
    ('PA', '42', 'Pennsylvania'),
    ('RI', '44', 'Rhode Island'),
    ('SC', '45', 'South Carolina'),
    ('SD', '46', 'South Dakota'),
    ('TN', '47', 'Tennessee'),
    ('TX', '48', 'Texas'),
    ('UT', '49', 'Utah'),
    ('VT', '50', 'Vermont'),
    ('VA', '51', 'Virginia'),
    ('WA', '53', 'Washington'),
    ('WV', '54', 'West Virginia'),
    ('WI', '55', 'Wisconsin'),
    ('WY', '56', 'Wyoming'),
    ('DC', '11', 'Washington DC', 'District of Columbia'),
    ('AS', '60', 'American Samoa'),
    ('GU', '66', 'Guam'),
    ('MP', '69', 'Northern Mariana Islands'),
    ('PR', '72', 'Puerto Rico'),
    ('VI', '78', 'Virgin Islands', 'U.S. Virgin Islands'),
}

SKIP_STATES = {
    ('AL', '01', 'Alabama'),
    ('AK', '02', 'Alaska'),
    ('AR', '05', 'Arkansas'),
    ('CA', '06', 'California'),
    ('CO', '08', 'Colorado'),
    ('CT', '09', 'Connecticut'),
    ('DE', '10', 'Delaware'),
    ('HI', '15', 'Hawaii'),
    ('ID', '16', 'Idaho'),
    ('IL', '17', 'Illinois'),
    ('IN', '18', 'Indiana'),
    ('IA', '19', 'Iowa'),
    ('KS', '20', 'Kansas'),
    ('KY', '21', 'Kentucky'),
    ('LA', '22', 'Louisiana'),
    ('ME', '23', 'Maine'),
    ('MD', '24', 'Maryland'),
    ('MI', '26', 'Michigan'),
    ('MN', '27', 'Minnesota'),
    ('MS', '28', 'Mississippi'),
    ('MO', '29', 'Missouri'),
    ('MT', '30', 'Montana'),
    ('NE', '31', 'Nebraska'),
    ('NV', '32', 'Nevada'),
    ('NH', '33', 'New Hampshire'),
    ('NJ', '34', 'New Jersey'),
    ('NM', '35', 'New Mexico'),
    ('NY', '36', 'New York'),
    ('NC', '37', 'North Carolina'),
    ('ND', '38', 'North Dakota'),
    ('OH', '39', 'Ohio'),
    ('OK', '40', 'Oklahoma'),
    ('OR', '41', 'Oregon'),
    ('PA', '42', 'Pennsylvania'),
    ('RI', '44', 'Rhode Island'),
    ('SC', '45', 'South Carolina'),
    ('SD', '46', 'South Dakota'),
    ('TN', '47', 'Tennessee'),
    ('UT', '49', 'Utah'),
    ('WA', '53', 'Washington'),
    ('WV', '54', 'West Virginia'),
    ('WI', '55', 'Wisconsin'),
    ('WY', '56', 'Wyoming'),
    ('DC', '11', 'Washington DC', 'District of Columbia'),
    ('AS', '60', 'American Samoa'),
    ('GU', '66', 'Guam'),
    ('MP', '69', 'Northern Mariana Islands'),
    ('PR', '72', 'Puerto Rico'),
    ('VI', '78', 'Virgin Islands', 'U.S. Virgin Islands'),
}

IRREGULAR_DISTRICT_STATES = {
    us.states.AK: ['state legislative district (upper chamber)'],
    us.states.MA: [
        'state legislative district (upper chamber)',
        'state legislative district (lower chamber)',
    ],
    us.states.VT: [
        'state legislative district (upper chamber)',
        'state legislative district (lower chamber)',
    ],
}

IRREGULAR_CCID_OUTPUT = Path.cwd() / 'app' / 'lookups' / 'irregulars'

SKIP_SESSIONS = [64, 127]
"""There are some LegiScan-provided legislative sessions that we're filtering out temporarily,
       And this list contains the legiscan-provided id's of those sessions."""

STATE_FIPS_TO_ABBR = {
    '01': 'AL',
    '02': 'AK',
    '04': 'AZ',
    '05': 'AR',
    '06': 'CA',
    '08': 'CO',
    '09': 'CT',
    '10': 'DE',
    '11': 'DC',
    '12': 'FL',
    '13': 'GA',
    '15': 'HI',
    '16': 'ID',
    '17': 'IL',
    '18': 'IN',
    '19': 'IA',
    '20': 'KS',
    '21': 'KY',
    '22': 'LA',
    '23': 'ME',
    '24': 'MD',
    '25': 'MA',
    '26': 'MI',
    '27': 'MN',
    '28': 'MS',
    '29': 'MO',
    '30': 'MT',
    '31': 'NE',
    '32': 'NV',
    '33': 'NH',
    '34': 'NJ',
    '35': 'NM',
    '36': 'NY',
    '37': 'NC',
    '38': 'ND',
    '39': 'OH',
    '40': 'OK',
    '41': 'OR',
    '42': 'PA',
    '44': 'RI',
    '45': 'SC',
    '46': 'SD',
    '47': 'TN',
    '48': 'TX',
    '49': 'UT',
    '50': 'VT',
    '51': 'VA',
    '53': 'WA',
    '54': 'WV',
    '55': 'WI',
    '56': 'WY',
    '60': 'AS',
    '66': 'GU',
    '69': 'MP',
    '72': 'PR',
    '74': 'UM',
    '78': 'VI',
}

STATE_ABBR_TO_FIPS = {i[1]: i[0] for i in STATE_FIPS_TO_ABBR.items()}

GEN_WELCOME = """
Welcome to the...

 ██████╗██╗     ██╗███╗   ███╗ █████╗ ████████╗███████╗
██╔════╝██║     ██║████╗ ████║██╔══██╗╚══██╔══╝██╔════╝
██║     ██║     ██║██╔████╔██║███████║   ██║   █████╗
██║     ██║     ██║██║╚██╔╝██║██╔══██║   ██║   ██╔══╝
╚██████╗███████╗██║██║ ╚═╝ ██║██║  ██║   ██║   ███████╗
 ╚═════╝╚══════╝╚═╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝

 ██████╗ █████╗ ██████╗ ██╗███╗   ██╗███████╗████████╗
██╔════╝██╔══██╗██╔══██╗██║████╗  ██║██╔════╝╚══██╔══╝
██║     ███████║██████╔╝██║██╔██╗ ██║█████╗     ██║
██║     ██╔══██║██╔══██╗██║██║╚██╗██║██╔══╝     ██║
╚██████╗██║  ██║██████╔╝██║██║ ╚████║███████╗   ██║
 ╚═════╝╚═╝  ╚═╝╚═════╝ ╚═╝╚═╝  ╚═══╝╚══════╝   ╚═╝

                                   database manager/ briefs generator!
"""


class TigerDataset:
    class Keys:
        """All column labels in each TIGER shapefile row"""

        STATE_FIPS = "STATEFP"
        GEOID = "GEOID"
        NAME = "NAME"
        CCID = "CCID"
        DIST_NUM = "DIST_NUM"
        DIST_TYPE = "DIST_TYPE"
        SHORTCODE = "SHORTCODE"
        TYPE_CODE = "MTFCC"
        LAND_AREA = "ALAND"
        STATE_GNIS = "STATENS"
        STATE_ABBR = "STUSPS"
        COUNTY_FIPS = "COUNTYFP"
        COUNTY_GNIS = "COUNTYNS"
        CD_SESSION = "CDSESSN"
        SL_LEG_YEAR = "LSY"

    TIGER_DIR = Path(DATA_CLEANED_PATH % 'tiger')
    RAW_TIGER_DIR = Path(DATA_RAW_PATH % 'tiger')


class DailyKosDatasets:
    """Stores directory-level metavariable for working with Daily Kos data"""

    class Keys:
        COUNTY = r"^(County|Parish)$"
        CD = "CD #"
        SLDU = r"^[SL]D #$"
        SLDL = r"^[HAL]D #$"
        POP = r"^[a-zA-Z]+ Pop. in [a-zA-Z]+$"
        PERC = r"^% of [a-zA-Z]+ in [a-zA-Z]+$"

    DATA_DIR = Path(DATA_CLEANED_PATH % 'daily_kos')


class AsthmaDataset:
    class AsthmaKeys:
        """the column headers for reading the ALA asthma dataset"""

        STATE = 'State'
        COUNTY = 'County'
        POP = 'TotalPop'
        ADULT = 'AAshthma'
        CHILD = 'PedAshtma'
        NON_WHITE = 'NonWhite'
        POVERTY = 'Poverty'

    DATASET = os.path.join(DATA_CLEANED_PATH % 'asthma', 'all.csv')


class JobsDataset:
    """Stores directory-level metavariables for working with polling data"""

    class JobsKeys:
        STATE = 'stateAbbr'
        GEOTYPE = 'geoType'
        NAME = 'name'
        GEOID = 'geoid'

        COUNT_SOLAR_JOBS = 'countSolarJobs'
        COUNT_WIND_JOBS = 'countWindJobs'
        COUNT_ENERGY_JOBS = 'countEnergyJobs'
        TOTAL_JOBS = 'totalJobs'
        PERCENT_OF_STATE_JOBS = 'percentOfStateJobs'

        RESIDENTIAL_MWH_INVESTED = 'residentialMWhInvested'
        COMMERCIAL_MWH_INVESTED = 'commercialMWhInvested'
        UTILITY_MWH_INVESTED = 'utilityMWhInvested'
        TOTAL_MWH_INVESTED = 'totalMWhInvested'

        RESIDENTIAL_DOLLARS_INVESTED = 'residentialDollarsInvested'
        COMMERCIAL_DOLLARS_INVESTED = 'commercialDollarsInvested'
        UTILITY_DOLLARS_INVESTED = 'utilityDollarsInvested'
        TOTAL_DOLLARS_INVESTED = 'totalDollarsInvested'
        INVESTMENT_HOMES_EQUIVALENT = 'investmentHomesEquivalent'

        COUNT_RESIDENTIAL_INSTALLATIONS = 'countResidentialInstallations'
        COUNT_COMMERCIAL_INSTALLATIONS = 'countCommercialInstallations'
        COUNT_UTILITY_INSTALLATIONS = 'countUtilityInstallations'
        TOTAL_INSTALLATIONS = 'countTotalInstallations'

        RESIDENTIAL_MW_CAPACITY = 'residentialMWCapacity'
        COMMERCIAL_MW_CAPACITY = 'commercialMWCapacity'
        UTILITY_MW_CAPACITY = 'utilityMWCapacity'
        TOTAL_MW_CAPACITY = 'totalMWCapacity'

    DATASET = os.path.join(DATA_CLEANED_PATH % 'jobs', 'all.csv')


class EnvironmentalOrgsDataset:
    """Stores directory-level metavariables for working with environmental orgs dataset"""

    class Keys:
        STATE_ABBR = 'state_abbr'
        NAME = 'name'
        SITE = 'site'

    DATASET = Path(
        DATA_CLEANED_PATH % 'environmental_orgs', 'environmental_orgs_all.csv'
    )
    RAW_DATASET = Path(
        DATA_RAW_PATH % 'environmental_orgs', 'State Scorecard Source List.csv'
    )
