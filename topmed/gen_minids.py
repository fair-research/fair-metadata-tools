from gen_records import get_data, SAMPLES
import sys
import csv
from concierge.api import create_bag
from login import load_tokens, CONCIERGE_SCOPE_NAME

PRODUCTION_MINIDS = False


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
                    if len(row) >= ARGON_GUID:
                        print('APPENDING MINID')
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

def gen_bdbag(remote_file_manifest, title):
    tokens = load_tokens()
    minid = create_bag(remote_file_manifest,
                       tokens[CONCIERGE_SCOPE_NAME]['access_token'],
                       minid_metadata={'title': title},
                       test=(not PRODUCTION_MINIDS)
                       )
    return minid


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
    data = get_data()[0:1]
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
        title = ('Topmed Public CRAM/CRAI ID Number: {}, {}'.format(nwd_id,
                                                                    hm_id))
        minid = gen_bdbag(manifest, title)
        print('.', end='')
        sys.stdout.flush()
        minids[nwd_id] = minid['minid']
    print('\n')
    update_topmed_tsv(minids, SAMPLES['downsample'])


if __name__ == '__main__':
    main()