from gen_records import get_data, get_remote_file_manifests, TOPMED_FILENAME
import csv
from concierge.api import create_bag
from login import load_tokens, CONCIERGE_SCOPE_NAME

TSV_COLUMNS = ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Google_URL',
               'S3_URL', 'Argon_GUID', 'Calcium_GUID', 'Helium_GUID',
               'Xenon_GUID', 'DOS_URI', 'CRAI_URL', 'md5sum', 'Assignment']
NWD_ID, S3_URL, ARGON_GUID = (
    TSV_COLUMNS.index('NWD_ID'),
    TSV_COLUMNS.index('S3_URL'),
    TSV_COLUMNS.index('Argon_GUID')
)


def update_topmed_tsv(minids):
    """Parse the tsv and return record info"""

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
    minid = create_bag(remote_file_manifest,
                       tokens[CONCIERGE_SCOPE_NAME]['access_token'],
                       metadata = {'title': title}
                       )
    return minid


def rebase_topmed(theirs_filename, ours=TOPMED_FILENAME):

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
    user_input = input('Create {} new minids? Y/N> '.format(len(data)))
    if user_input not in ['yes', 'Y', 'y', 'yarr']:
        print('Aborting!')
        return
    else:
        print('This should take 5 minutes...')

    minids = {}
    for d in data:
        nwd_id = d[0]['NWD_ID']
        manifest = get_remote_file_manifests(d)
        title = ('Topmed Public CRAM/CRAI ID Number: '
                 '{}, {}'.format(nwd_id, d[0]['HapMap_1000G_ID']))
        minid = gen_bdbag(manifest, title)
        minids[nwd_id] = minid['minid_id']

    update_topmed_tsv(minids)


if __name__ == '__main__':
    main()
