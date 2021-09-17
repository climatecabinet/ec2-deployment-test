import json
import requests
from halo import Halo
from time import sleep
from requests.exceptions import ConnectionError, HTTPError
from app import ClimateCabinetDBManager as CCDBM
from app.config import BUILD_USER
from app.models import Representative
from app.helpers.sync_webflow_cms_secrets import (
    WEBFLOW_API_KEY,
    WEBFLOW_COLLECTION_ID,
    GRAPHQL_API_KEY,
)

GRAPHQL_ENDPOINT = "https://us-west-2.aws.realm.mongodb.com/api/client/v2.0/app/climate-cabinet-production-esyps/graphql"  # noqa: E501
WF_ENDPOINT = 'https://api.webflow.com'
WF_HEADERS = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {WEBFLOW_API_KEY}',
    'Content-Type': 'application/json',
    'Accept-Version': '1.0.0',
}


def _make_api_request(logger, method, endpoint, headers=None, data=None, json=None):
    attempts = 0

    while attempts < 3:
        #  try to make the request
        try:
            resp = requests.request(
                method, endpoint, headers=headers, data=data, json=json
            )

            resp.raise_for_status()
            return resp.json()

        except ConnectionError:
            attempts += 1
            logger.warn(f"ConnectionError on {method} request - attempt {attempts}/3")
            sleep(3)
            logger.start()

        except HTTPError as err:
            raise err

    # log as error and continue
    logger.fail(f"Failing on {method} request - max attempts reached")


@Halo(text="Fetching names + ID of legislators in Webflow...", spinner='dots')
def _get_all_cms_legislators():
    offset = 0

    def _get_cms_legislator_batch(offset):
        sleep(1)  # sleep to prevent hitting Webflow's RateLimit (60 requests/minute)

        resp = requests.get(
            WF_ENDPOINT + f'/collections/{WEBFLOW_COLLECTION_ID}/items?offset={offset}',
            headers=WF_HEADERS,
        )

        resp.raise_for_status()
        return resp.json()

    resp = _get_cms_legislator_batch(offset)

    legis = [legi for legi in resp['items']]
    total = resp['total']

    while len(legis) < total:
        offset += 100
        resp = _get_cms_legislator_batch(offset)
        legis.extend(resp['items'])

    return legis


def sync_webflow_cms():
    # connect to the database
    with CCDBM(BUILD_USER, ensure_db=True, quiet=True):
        # get a mapping of all legiscan_id's for all legislators currently in the CMS,
        # mapped to the item-id and slug for that legislator
        legiscan_to_cms = {
            i['legiscan-id']: (i['_id'], i['slug']) for i in _get_all_cms_legislators()
        }

        legis = Representative.objects(office__is_current=True)

        with Halo(text='Beginning CMS refresh...') as halo:
            for i, legi in enumerate(legis):
                # make an API request for the blurbs for this rep
                query = f"""query {{
                    representative(query: {{legiscan_id: {legi.legiscan_id}}}) {{
                      ccscorecard {{
                        intro
                        votes
                        outro
                      }}
                  }}
                }}"""

                ccdb_resp = _make_api_request(
                    halo,
                    'POST',
                    GRAPHQL_ENDPOINT,
                    headers={
                        'apiKey': GRAPHQL_API_KEY,
                        'Content-Type': 'application/json',
                    },
                    data=json.dumps({'query': query}),
                )

                if 'errors' in ccdb_resp.keys():
                    raise Exception(ccdb_resp['errors'])

                blurbs = ccdb_resp['data']['representative']['ccscorecard']

                legi_item = {
                    'name': legi.full_name,
                    'state': legi.state_abbr,
                    'district': legi.office.district.shortcode,
                    'role': legi.role,
                    'party': legi.party,
                    'climate-cabinet-score': legi.cc_score,
                    'legiscan-id': legi.legiscan_id,
                    'intro': blurbs['intro'],
                    'outro': blurbs['outro'],
                    '_archived': False,
                    '_draft': False,
                }

                for j in range(5):
                    legi_item[f'vote-{j+1}'] = (
                        blurbs['votes'][j] if j < len(blurbs['votes']) else ''
                    )

                if legi['legiscan_id'] in legiscan_to_cms.keys():
                    # TODO(matt): make sure we're only updating the field we need to
                    # TODO(matt): make sure we're only updating the legislators who need it
                    halo.text = (
                        f"Updating item for {legi.full_name} in {legi.state_abbr} "
                        f"{legi_item['district']} - {i}/{len(legis)}"
                    )
                    item_id, legi_item['slug'] = legiscan_to_cms[legi['legiscan_id']]

                    wf_resp = _make_api_request(
                        halo,
                        'PUT',
                        WF_ENDPOINT
                        + f'/collections/{WEBFLOW_COLLECTION_ID}/items/{item_id}',
                        headers=WF_HEADERS,
                        json={
                            'collection_id': WEBFLOW_COLLECTION_ID,
                            'item_id': item_id,
                            'fields': legi_item,
                        },
                    )

                else:
                    halo.text = (
                        f"Creating item for {legi.full_name} in {legi.state_abbr} "
                        f"{legi_item['district']} - {i}/{len(legis)}"
                    )

                    wf_resp = _make_api_request(
                        halo,
                        'POST',
                        WF_ENDPOINT + f'/collections/{WEBFLOW_COLLECTION_ID}/items',
                        headers=WF_HEADERS,
                        json={
                            'collection_id': WEBFLOW_COLLECTION_ID,
                            'fields': legi_item,
                        },
                    )

                if 'err' in wf_resp.keys():
                    raise Exception(wf_resp['err'])

                # sleep to prevent hitting Webflow's RateLimit (60 requests/minute)
                sleep(1)
