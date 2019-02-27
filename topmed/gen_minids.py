from gen_records import get_data, SAMPLES
import sys
import csv
from concierge.api import bag_create
from login import load_tokens, CONCIERGE_SCOPE_NAME

PRODUCTION_MINIDS = True
# Limit minid creation to this number Only. set to '0' to turn off limiting.
LIMIT_NUMBER = 0
SAMPLE_TYPE = 'topmed'


def update_topmed_tsv(minids, sample_metadata):
    """Parse the tsv and return record info"""
    f = sample_metadata['filename']
    s3_url = sample_metadata['s3_name']
    cols = sample_metadata['columns']
    S3_URL, ARGON_GUID = cols.index(s3_url), cols.index('Argon_GUID')
    NWD_ID = cols.index('NWD_ID')

    rows = []
    with open(f) as t:
        tin = csv.reader(t, delimiter='\t')
        for row in tin:
            # Non-s3 links aren't real records, ignore them.
            if row[S3_URL].startswith('s3://'):
                minid = minids.get(row[NWD_ID], '')
                if minid:
                    if len(row) < ARGON_GUID:
                        print('Appending "{}" for "{}"'.format(
                              minid, row[NWD_ID]
                        ))
                        row.append(minid)
                    else:
                        print('Replacing "{}" with "{}" for {}'.format(
                              row[ARGON_GUID], minid, row[NWD_ID]))
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


def main():
    data = get_data()
    if LIMIT_NUMBER is not 0:
        data = data[0:LIMIT_NUMBER]
    print('Settings: \n\tUse Production Minids? {}'.format(PRODUCTION_MINIDS))
    print('\tLimit Creation to: {}'.format(LIMIT_NUMBER or 'No Limit'))
    user_input = input('Create {} new minids? Y/N> '.format(len(data)))
    if user_input not in ['yes', 'Y', 'y', 'yarr']:
        print('Aborting!')
        return
    else:
        print('This should take 5 minutes...')

    minids = {}
    for d in data:
        record, manifest = d
        nwd_id = record[0]['NWD_ID']
        hm_id = record[0]['HapMap_1000G_ID']
        if record[0]['Assignment'] == 'Downsample':
            title = 'Topmed Downsample NWD-ID {}'.format(nwd_id)
            bag_name = 'Topmed_Downsample_NWD_ID_{}'.format(nwd_id)
        else:
            title = 'Topmed Public NWD-ID {}'.format(nwd_id)
            bag_name = 'Topmed_Public_NWD_ID_{}'.format(nwd_id)
        minid = gen_bdbag(manifest, title, bag_name)
        print('.', end='')
        sys.stdout.flush()
        minids[nwd_id] = minid['minid']
    print('\n')
    update_topmed_tsv(minids, SAMPLES[SAMPLE_TYPE])


if __name__ == '__main__':
    main()
