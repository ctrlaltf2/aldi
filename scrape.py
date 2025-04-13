#!/usr/bin/env -S uv run
# /// script
# requires-python = "==3.13"
# dependencies = [
#     "httpx"
# ]
# ///
from pathlib import Path
from string import Template
import argparse
import datetime
import httpx
import json
import random
import time

parser = argparse.ArgumentParser(
    prog='aldi-scraper',
    description='Pull down current prices at a specific ALDI store'
)

# store region, found by choosing a store for pickup near you, and looking at the store code in cookies.
# An example total store identifier on the West coast might be 479-030, with 479 being the region code
parser.add_argument('-r', '--region', type=int, required=True, help='Region number (e.g. 479)')
# See above. If your specific store doesn't show up in the search,
# check your physical receipt for the store number.
parser.add_argument('-s', '--store', type=int, required=True, help='Store number (e.g. 40)')

args = parser.parse_args()

# --- Store config
region_number = f'{args.region:03}'
store_number  = f'{args.store:03}'
# This is the usual max page limit
page_limit = 60
# ---

# --- Scraper config
min_sleep = 2433 # ms
max_sleep = 6151 # ms
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Origin': 'https://new.aldi.us',
    'Sec-GPC': '1',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
}
url = Template('https://api.aldi.us/v3/product-search?currency=USD&q=&limit=$limit&offset=$offset&sort=name_asc&servicePoint=$region-$store')
# ---

timestr = (datetime.datetime
           .now()
           .replace(microsecond=0, second=0)
           .isoformat()
           .replace(':', '')
           [:-2]
           )
timestr = f'{timestr}-{region_number}-{store_number}'
output_dir = Path('.') / timestr
output_dir.mkdir(exist_ok=False)

end_index = None
current_index = 0

# track ephemeral failures and do exponential backoff random retry
def gen_time(failures: int, min_sleep: float, max_sleep: float) -> float:
    sleep_time = 0.0
    for i in range(2**failures):
        sleep_time += random.randrange(min_sleep, max_sleep) / 1000

    return sleep_time

failures = 0
while (not end_index) or (current_index <= end_index):
    sleep_time = gen_time(failures, min_sleep, max_sleep)
    print(f'Sleeping for {sleep_time}s...')
    time.sleep(sleep_time)
    this_url = url.substitute(
            {
                'limit': page_limit,
                'offset': current_index,
                'region': region_number,
                'store': store_number,
            }
    )
    print(f'GET {this_url}')
    response = httpx.get(this_url, headers=headers)

    # Exp retry last request if error
    if response.status_code != 200:
        print('Failed, retrying.')
        failures += 1
        continue
    else:
        failures = 0

    js = response.json()

    if end_index is None:
        end_index = int(js['meta']['pagination']['totalCount'])

    # Dump response to file for now
    with open(output_dir / f'{str(current_index)}.json', 'w') as f:
        json.dump(js, f)

    current_index += page_limit
