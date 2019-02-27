from update_tsv import get_headers
import sys
import csv
import json
import requests
from identifier_client.identifier_api import IdentifierClient
from globus_sdk import AccessTokenAuthorizer
from login import load_tokens


MINIDKEY = 'Argon_GUID'
MINID_SIZE_KEY = 'contentSize'
INPUT = 'topmed-107.tsv'

def load_identifier_client():
    token = load_tokens()['identifiers.globus.org']['access_token']
    ic = IdentifierClient('Identifier',
                          base_url='https://identifiers.globus.org/',
                          app_name='My Local App',
                          authorizer=AccessTokenAuthorizer(token)
                          )
    return ic


def fetch_minids_from_tsv(tsv_fname):
    with open(tsv_fname) as tsvfd:
        tsv = csv.reader(tsvfd, delimiter='\t')
        rows = [r for r in tsv]

    headers = get_headers(rows, MINIDKEY, tsv_fname)
    minid_pos = headers.index(MINIDKEY)
    minids = {r[minid_pos] for r in rows
              if len(r) == len(headers) and r != headers}
    return list(minids)


if __name__ == '__main__':
    minids = fetch_minids_from_tsv(INPUT)
    ic = load_identifier_client()

    updated = 0
    for minid in minids:
        m = ic.get_identifier(minid).data
        metadata = m['metadata']
        r = requests.head(m['location'][0])
        rsize = int(r.headers.get('Content-Length'))
        msize = metadata.get(MINID_SIZE_KEY)
        if rsize != msize:
            print('\nUpdating {} contentSize from {} to {}'.format(
               minid, metadata.get(MINID_SIZE_KEY), rsize
            ))
            metadata[MINID_SIZE_KEY] = rsize
            ic.update_identifier(minid, metadata=json.dumps(metadata))
            updated += 1
        else:
            print('.', end='')
        sys.stdout.flush()

    print('\nChecked {} minids -- updated {}'.format(len(minids),
                                                     updated,
                                                     ))