import os
import os.path
import time
import requests
import pandas as pd
import asyncio
import json
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
from app.config import DATA_RAW_PATH

STATES_URL = 'https://api.kevalaanalytics.com/geography/states/'
REGIONS_URL = 'http://assessor.keva.la/cleanenergyprogress/geographies?state=%s&type=%s'
HTML_URL = 'http://assessor.keva.la/cleanenergyprogress/analytics?area_type=%s&area_id=%s'
STATES_INPUT_URL = "http://assessor.keva.la/cleanenergyprogress/states?states="

METADATA_CSV = os.path.join(DATA_RAW_PATH % 'jobs', 'jobs_metadata.csv')
STATES_META_OUTPUT_FILE = os.path.join(DATA_RAW_PATH % 'jobs', 'states-cleaned.json')
OUTPUT_CSV = os.path.join(DATA_RAW_PATH % 'jobs', 'all.csv')

REGION_TYPES = [
    ('county', 'counties'),
    ('sldu', 'legislativedistrictsupper'),
    ('sldl', 'legislativedistrictslower'),
    ('cd', 'congressionaldistricts')]

JOB_STAT_KEYS = [
    'countSolarJobs',
    'countWindJobs',
    'countEnergyJobs',
    'totalJobs',
    'percentOfStateJobs',
    'residentialMWhInvested',
    'commercialMWhInvested',
    'utilityMWhInvested',
    'totalMWhInvested',
    'residentialDollarsInvested',
    'commercialDollarsInvested',
    'utilityDollarsInvested',
    'totalDollarsInvested',
    'investmentHomesEquivalent',
    'countResidentialInstallations',
    'countCommercialInstallations',
    'countUtilityInstallations',
    'countTotalInstallations',
    'residentialMWCapacity',
    'commercialMWCapacity',
    'utilityMWCapacity',
    'totalMWCapacity'
]

CSV_KEYS = [
    'stateAbbr',
    'geoType',
    'name',
    'geoid',
    'sourceURL'
]
CSV_KEYS.extend(JOB_STAT_KEYS)

HTML_STRUCTURE = {
    'tables': [
        ['countSolarJobs', 'countWindJobs', 'countEnergyJobs'],
        ['residentialDollarsInvested', 'residentialMWhInvested', 'commercialDollarsInvested',
         'commercialMWhInvested', 'utilityDollarsInvested', 'utilityMWhInvested'],
        ['countResidentialInstallations', 'residentialMWCapacity', 'countCommercialInstallations',
         'commercialMWCapacity', 'countUtilityInstallations', 'utilityMWCapacity'],
    ],
    'totals': [
        ['totalJobs', 'percentOfStateJobs'],
        ['totalDollarsInvested', 'totalMWhInvested', 'investmentHomesEquivalent'],
        ['countTotalInstallations', 'totalMWCapacity']
    ]
}


async def fetch_region_list(url, session, region_type, state_abbr):
    async with session.get(url) as response:
        resp = await response.json()
        print("Got list [%s, %s]" % (region_type, state_abbr))
        return (resp, region_type, state_abbr)


async def process_states(states_and_geoids):
    fetch_tasks = []
    df = pd.DataFrame(columns=['state_abbr', 'region_type', 'geoid', 'name', 'html_url'])

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        for state_abbr, state_geoid in states_and_geoids:
            df = df.append({
                'state_abbr': state_abbr,
                'region_type': 'state',
                'geoid': state_geoid,
                'name': None,
                'html_url': HTML_URL % ('state', state_geoid),
            }, ignore_index=True)

            for (region_type_singular, region_type_plural) in REGION_TYPES:
                url = REGIONS_URL % (state_geoid, region_type_plural)
                task = asyncio.ensure_future(
                    fetch_region_list(url, session, region_type_singular, state_abbr))
                fetch_tasks.append(task)

        responses = await asyncio.gather(*fetch_tasks)

        for response_json, region_type, state_abbr in responses:
            for idx, region in enumerate(response_json['features']):
                if state_abbr in ('DC', 'NE') and region_type == 'sldl':
                    continue  # DC and NE both doesn't have a state house
                props = region['properties']
                df = df.append({
                    'state_abbr': state_abbr,
                    'region_type': region_type,
                    'geoid': props.get('geoid'),
                    'name': props.get('name'),
                    'html_url': HTML_URL % (region_type, props.get('geoid')),
                }, ignore_index=True)
                print("Appended %d regions" % (idx + 1), end='\r')

        return df


def download_jobs_metadata():
    states_json = requests.get(STATES_URL).json()
    states_and_geoids = [(s['usps'], s['geoid']) for s in states_json]

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(process_states(states_and_geoids))
    df = loop.run_until_complete(future)

    df.to_csv(METADATA_CSV, index=False)


async def fetch_states_json_from_url(session, url):
    async with session.get(url) as resp:
        return await resp.json()


async def get_raw_states_json():
    async with ClientSession() as session:
        return await fetch_states_json_from_url(session, STATES_INPUT_URL)


def scrape_energy_job_states():
    states = []

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_raw_states_json())
    input_data = loop.run_until_complete(future)

    for feature in input_data['features']:
        state_data = feature['properties']
        state_data["html_url"] = HTML_URL % (state_data['geography_type'],
                                             state_data['geoid'])
        states.append(state_data)

    output_data = {
        "states": states
    }

    with open(STATES_META_OUTPUT_FILE, 'w') as output_file:
        json.dump(output_data, output_file)


def scrape(metadata, attempt=1):
    url = metadata['html_url']
    _idx = metadata['_idx']
    with requests.get(url) as response:
        row = {
            'stateAbbr': metadata['state_abbr'],
            'geoid': metadata['geoid'],
            'geoType': metadata['region_type'],
            'name': metadata['name'],
            'sourceURL': metadata['html_url'],
        }
        unique_key = url.replace('http://assessor.keva.la/cleanenergyprogress', '')

        if attempt > 3:
            print(f"{_idx}: [{attempt}/3] – {response.status_code} – FAIL  – {unique_key}")
            return None

        if response.status_code >= 400:
            print(f"{_idx}: [{attempt}/3] – {response.status_code} – RETRY  – {unique_key}")
            time.sleep(3)
            return scrape(metadata, attempt + 1)

        html = response.text
        soup = BeautifulSoup(html, 'html5lib')

        row['name'] = soup.find('span', id='geography__name').text.strip()

        outer_divs = soup.find_all('div', class_='analytics_data')

        for keylist, outerdiv in zip(HTML_STRUCTURE['tables'], outer_divs):
            tds = outerdiv.find_all('td', class_='table_data')
            values = [elem.text.strip() for elem in tds[:len(keylist)]]

            for idx, key in enumerate(keylist):
                row[key] = values[idx]

        li_buckets = soup.find_all('li', class_=None)
        if len(li_buckets) != 3:
            print(f"{_idx}: [{attempt}/3] – {response.status_code} – PARSE  – {unique_key}")
            print("li_buckets:", li_buckets)
            print(html)
            raise ValueError

        for keylist, outerli in zip(HTML_STRUCTURE['totals'], li_buckets):
            total_spans = outerli.find_all('span', class_='analytics_total_num')
            totals = [elem.text.strip() for elem in total_spans]
            if metadata['region_type'] == 'state' and keylist[-1] == 'percentOfStateJobs':
                keylist = keylist[:-1]

            if len(totals) == 0:
                for key in keylist:
                    row[key] = 0
            elif len(totals) != len(keylist):
                print(f"{_idx}: [{attempt}/3] – {response.status_code} – PARSE  – {unique_key}")
                print("totals:", totals, keylist)
                print(html)
                raise ValueError
            else:
                for idx, key in enumerate(keylist):
                    row[key] = totals[idx]

        print(f"{_idx}: [{attempt}/3] – {response.status_code} – OK  – {unique_key}")
        return row


def scrape_jobs_data():
    jobs_data = None
    if os.path.exists(OUTPUT_CSV):
        jobs_data = pd.read_csv(OUTPUT_CSV, encoding='ISO-8859-1')
    else:
        jobs_data = pd.DataFrame(columns=CSV_KEYS)

    jobs_metadata = [x for _, x in pd.read_csv(METADATA_CSV, encoding='ISO-8859-1').iterrows()]
    processed_urls = set(jobs_data['sourceURL'])

    batch = []
    batch_size = 100

    for i, metadata_row in enumerate(jobs_metadata):
        url = jobs_metadata[i]['html_url']

        if url in processed_urls:
            print("Skipped: %d" % i, end='\r')
            if i != len(jobs_metadata) - 1:
                continue

        if url not in processed_urls:
            metadata_row['_idx'] = i
            batch.append(metadata_row)

        if len(batch) >= batch_size or i == len(jobs_metadata) - 1:
            print("\nStarting Batch")
            results = ThreadPool(20).imap_unordered(scrape, batch)

            for data_row in results:
                jobs_data = jobs_data.append(data_row, ignore_index=True)

            jobs_data.to_csv(OUTPUT_CSV, index=False)
            batch = []
            print("Wrote to disk.")

    jobs_data.to_csv(OUTPUT_CSV, index=False)


def fetch():
    """Implement with code that automates scraping/ downloading process for this dataset"""
    download_jobs_metadata()
    scrape_energy_job_states()
    scrape_jobs_data()
