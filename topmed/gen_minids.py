from gen_records import get_data, SAMPLES
import sys
import csv
import json
from concierge.api import bag_create
from globus_sdk import AccessTokenAuthorizer
from identifier_client.identifier_api import IdentifierClient

from login import load_tokens, CONCIERGE_SCOPE_NAME


PRODUCTION_MINIDS = False
# Limit minid creation to this number Only. set to '0' to turn off limiting.
LIMIT_NUMBER = 1
SAMPLE_TYPE = 'gtex'

IDENTIFIER_API = 'https://identifiers.globus.org/'
TEST_IDENTIFIER_NAMESPACE = 'HHxPIZaVDh9u'
IDENTIFIER_NAMESPACE = 'kHAAfCby2zdn'
# Access token supplied in ~/.globus_identifier


def update_topmed_tsv(minids, sample_metadata):
    """Parse the tsv and return record info"""
    f = sample_metadata['filename']
    s3_url = sample_metadata['s3_name']
    cols = sample_metadata['columns']
    S3_URL, ARGON_GUID = cols.index(s3_url), cols.index('Argon_GUID')
    GTEX_ID = cols.index('GTEX_ID')

    rows = []
    with open(f) as t:
        tin = csv.reader(t, delimiter='\t')
        for row in tin:
            # Non-s3 links aren't real records, ignore them.
            if row[S3_URL].startswith('s3://'):
                minid = minids.get(row[GTEX_ID], '')
                if minid:
                    if len(row) < ARGON_GUID:
                        print('Appending "{}" for "{}"'.format(
                              minid, row[GTEX_ID]
                        ))
                        row.append(minid)
                    else:
                        print('Replacing "{}" with "{}" for {}'.format(
                              row[ARGON_GUID], minid, row[GTEX_ID]))
                        row[ARGON_GUID] = minid
            rows.append(row)

    with open(f, 'w') as t:
        tout = csv.writer(t, delimiter='\t', lineterminator='\n')
        for row in rows:
            tout.writerow(row)

def gen_bdbag(remote_file_manifest, title, bag_name):
    tokens = load_tokens()
    minid = bag_create(remote_file_manifest,
                       tokens[CONCIERGE_SCOPE_NAME]['access_token'],
                       minid_metadata={'title': title},
                       minid_test=(not PRODUCTION_MINIDS),
                       bag_name=bag_name,
                       # server='http://localhost:8000'
                       )
    return minid


def gen_minid(remote_file_manifest):

    tokens = load_tokens()

    ic = IdentifierClient(
        'Identifier',
        base_url=IDENTIFIER_API,
        app_name='Fair Metadata Minid Minter Tool',
        authorizer=AccessTokenAuthorizer(tokens['identifiers.globus.org']['access_token'])
    )

    kwargs = {
        'visible_to': ['public'],
        'location': [remote_file_manifest['url']],
        'checksums': [{
            'function': 'md5',
            'value': remote_file_manifest['md5']
        }],
        'metadata': {
            'title': remote_file_manifest['filename']
        }
    }

    json_kwargs = {k: json.dumps(v) for k, v in kwargs.items()}
    json_kwargs['namespace'] = IDENTIFIER_NAMESPACE if PRODUCTION_MINIDS else TEST_IDENTIFIER_NAMESPACE
    minid = ic.create_identifier(**json_kwargs)
    return minid.data['identifier']


def rebase_topmed(theirs_filename, ours):

    """Apply new minid changes to a diffing topmed file from our own"""
    with open(ours) as otf, open(theirs_filename) as ttf:
        our_tsv = csv.reader(otf, delimiter='\t')
        their_tsv = csv.reader(ttf, delimiter='\t')
        ourrows = [r for r in our_tsv]
        theirrows = [r for r in their_tsv]


        if len(ourrows) != len(theirrows):
            print('Lines in files differ -- Ours: {} theirs: {}'.format(
                len(ourrows), len(theirrows)
            ))
            return

        output_rows = []
        changed = 0
        for ourr, theirr in zip(ourrows, theirrows):
            if ourr[ARGON_GUID].startswith('ark') and \
               ourr[ARGON_GUID] != theirr[ARGON_GUID]:

                theirr[ARGON_GUID] = ourr[ARGON_GUID]
                print('.', end='')
                changed += 1
            output_rows.append(theirr)
        print('\nRecords Changed: {}'.format(changed))

    with open('rebased_topmed.tsv', 'w') as t:
        tout = csv.writer(t, delimiter='\t', lineterminator='\n')
        for row in output_rows:
            tout.writerow(row)


def main():
    data = get_data()
    data = [d for d in data if d[0][0]['Assignment'] == 'Argon']
    if LIMIT_NUMBER is not 0:
        data = data[0:LIMIT_NUMBER]
    print('Settings: \n\tUse Production Minids? {}'.format(PRODUCTION_MINIDS))
    print('\tLimit Creation to: {}'.format(LIMIT_NUMBER or 'No Limit'))
    print('\tFiltering on: Assignment=Argon'.format(LIMIT_NUMBER or 'No Limit'))
    user_input = input('Create {} new minids? Y/N> '.format(len(data)))
    if user_input not in ['yes', 'Y', 'y', 'yarr']:
        print('Aborting!')
        return
    else:
        print('This should take 5 minutes...')

    minids = {}
    for d in data:
        record, manifest = d
        min_crai, min_cram = gen_minid(manifest[0]), gen_minid(manifest[1])
        minids[manifest[0]['filename']] = min_crai
        minids[manifest[1]['filename']] = min_cram
        print('Crai {}, cram {}'.format(min_crai, min_cram))
        print('.', end='')
        sys.stdout.flush()
    print('\n')
    # update_topmed_tsv(minids, SAMPLES[SAMPLE_TYPE])


if __name__ == '__main__':
    main()
