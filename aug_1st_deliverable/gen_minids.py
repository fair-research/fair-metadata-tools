from gen_records import get_data, get_remote_file_manifests, TOPMED_FILENAME
import csv
from globus_sdk import AccessTokenAuthorizer, AuthClient, SearchClient
from concierge.api import create_bag
from login import load_tokens, CONCIERGE_SCOPE_NAME

SERVER = 'https://concierge.fair-research.org/'


def update_topmed_tsv(minids):
    """Parse the tsv and return record info"""
    TSV_COLUMNS = ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Google_URL',
                   'S3_URL', 'Argon_GUID', 'Calcium_GUID', 'Helium_GUID',
                   'Xenon_GUID', 'DOS_URI', 'CRAI_URL', 'md5sum', 'Assignment']
    NWD_ID, S3_URL, ARGON_GUID = (
        TSV_COLUMNS.index('NWD_ID'),
        TSV_COLUMNS.index('S3_URL'),
        TSV_COLUMNS.index('Argon_GUID')
    )

    rows = []
    with open(TOPMED_FILENAME) as t:
        tin = csv.reader(t, delimiter='\t')
        for row in tin:
            # Non-s3 links aren't real records, ignore them.
            if row[S3_URL].startswith('s3://'):
                minid = minids.get(row[NWD_ID], '')
                if minid:
                    print('Replacing "{}" with "{}" for {}'.format(
                          row[ARGON_GUID], minid, row[NWD_ID]))
                    row[ARGON_GUID] = minid
            rows.append(row)

    with open(TOPMED_FILENAME, 'w') as t:
        tout = csv.writer(t, delimiter='\t', lineterminator='\n')
        for row in rows:
            tout.writerow(row)

def gen_bdbag(remote_file_manifest, title):
    tokens = load_tokens()
    minid = create_bag(remote_file_manifest, 'Notused',
                       'notused', title,
                       tokens[CONCIERGE_SCOPE_NAME]['access_token'],
                       server='http://localhost:8000')
    return minid



if __name__ == '__main__':
    data = get_data()
    # Minids tracked by NWD_ID
    minids = {}
    for d in data[0:1]:
        nwd_id = d[0]['NWD_ID']
        manifest = get_remote_file_manifests(d)
        title = ('Topmed Public CRAM/CRAI ID Number: '
                 '{}, {}'.format(nwd_id, d[0]['HapMap_1000G_ID']))
        minid = gen_bdbag(manifest, title)
        minids[nwd_id] = minid['minid_id']

    # minids = {'NWD285363': 'ark:/57799/b9jx3g'}
    update_topmed_tsv(minids)