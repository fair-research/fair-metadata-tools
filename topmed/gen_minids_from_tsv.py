import sys
import csv
import json
import requests
from pprint import pprint

from update_tsv import get_headers
from concierge.api import bag_create
from login import load_tokens, CONCIERGE_SCOPE_NAME
from utils.update_minids import load_identifier_client



DEFAULT_OUTPUT_FILE = 'output.tsv'
KEY_TO_UPDATE = 'File size'
MINID_KEY = 'Argon_GUID'
VERIFY_KEY = 'File_Name'
PRODUCTION_MINIDS = False
LIMIT_NUMBER = 0



RFM_URL = 's3://commons-demo/vcfs/{}'
RFM_KEYS = {
    'filename': 'File_Name',
    'length': 'File size',
    'url': '',
    'md5': 'md5sum'
}

METADATA = {
    'erc.who': 'Nickolaus Saint',
    '_profile': 'erc',
    #'erc.what': '',
    #'erc.when': '',
    # 'Title': '',
    'contentSize': 123455,
}


def make_minids(tsv_filename):
    """Apply new minid changes to a diffing topmed file from our own"""
    with open(tsv_filename) as tsvfd:
        tsv = csv.reader(tsvfd, delimiter='\t')
        tsv_rows = [r for r in tsv]

        tsv_headers = get_headers(tsv_rows, RFM_KEYS['filename'], tsv_filename)
        tsv_keys = {k: tsv_headers.index(v) for k, v in RFM_KEYS.items() if v}
        minid_index = tsv_headers.index(MINID_KEY)

        print(
        'Settings: \n\tUse Production Minids? {}'.format(PRODUCTION_MINIDS))
        print('\tLimit Creation to: {}'.format(LIMIT_NUMBER or 'No Limit'))
        user_input = input('Create {} new minids? Y/N> '.format(len(tsv_rows)))
        if user_input not in ['yes', 'Y', 'y', 'yarr']:
            print('Aborting!')
            return
        else:
            print('This should take 5 minutes...')


        minids_minted = 0
        for row in tsv_rows:
            if not row_missing_column(row, tsv_headers, minid_index):
                print('S', end='')
                continue

            manifest = {k: row[v] for k, v in tsv_keys.items()}
            manifest['url'] = RFM_URL.format(manifest['filename'])
            row_metadata = METADATA.copy()
            row_metadata['erc.what'] = manifest['filename']
            row_metadata['Title'] = bag_fname(manifest['filename'])

            cresp = make_bdbag([manifest],
                               row_metadata,
                               bag_fname(manifest['filename']))

            ic = load_identifier_client()
            minid = ic.get_identifier(cresp['minid']).data
            # loc = minid['location'][0]
            # size = requests.head(loc).headers.get('Content-Length')
            # row_metadata['contentSize'] = size
            #
            # ic.update_identifier(minid['identifier'],
            #                      metadata=json.dumps(row_metadata))
            row[minid_index] = minid['identifier']
            # pprint(requests.get(minid['landing_page'],
            #        headers={'Accept': 'application/vnd.schemaorg.ld+json'}).json())
            print('.', end='')
            sys.stdout.flush()
            minids_minted += 1
            if LIMIT_NUMBER != 0 and minids_minted == LIMIT_NUMBER:
                break

        with open(DEFAULT_OUTPUT_FILE, 'w') as t:
            tout = csv.writer(t, delimiter='\t', lineterminator='\n')
            for row in tsv_rows:
                tout.writerow(row)
            print('Output written to {}'.format(DEFAULT_OUTPUT_FILE))


def row_missing_column(row, headers, column):
    return len(row) == len(headers) and row != headers and not row[column]


def make_bdbag(remote_file_manifest, metadata, bag_name):
    tokens = load_tokens()
    minid = bag_create(remote_file_manifest,
                       tokens[CONCIERGE_SCOPE_NAME]['access_token'],
                       minid_metadata=metadata,
                       minid_test=(not PRODUCTION_MINIDS),
                       bag_name=bag_name,
                       # server='http://localhost:8000'
                       )
    return minid

def bag_fname(filename):
    return filename.replace('.tar.gz', '')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: {} TSV_FILE'
              ''.format(sys.argv[0]))
    else:
        make_minids(sys.argv[1])