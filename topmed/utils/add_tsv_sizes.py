import sys
import csv
from gen_records import get_data
from update_tsv import get_headers



def update_topmed_tsv(records, tsvf):
    """Parse the tsv and return record info"""

    rows = []
    with open(tsvf) as tsvfd:
        tsv = csv.reader(tsvfd, delimiter='\t')
        rows = [r for r in tsv]

    headers = get_headers(rows, 'realigned_md5sum', tsvf)
    old_headers = get_headers(rows, 'realigned_md5sum', tsvf)
    inloc = headers.index('realigned_md5sum')
    ITEM = 'File size'
    nwdidloc = headers.index('NWD_ID')
    s3loc = headers.index('S3_URL')

    man = {r[0][0]['NWD_ID']: r[1] for r in records}

    headers.insert(inloc, ITEM)

    newrows = []
    for row in rows:
        if row == old_headers:
            print('replacing headers')
            row = headers
        elif len(row) == len(headers) - 1:
            print('.', end='')
            rec = man[row[nwdidloc]]
            cram, crai = rec

            # print('Inserting {} at {}'.format(crai['length'], row[0]))
            if row[s3loc].endswith('.crai'):
                row.insert(inloc, crai['length'])
            else:
                row.insert(inloc, cram['length'])
        else:
            print('skippnig')
        newrows.append(row)

    # from pprint import pprint
    # pprint(newrows)

    with open('newtopmed-107.tsv', 'w') as tsvfd:
        tout = csv.writer(tsvfd, delimiter='\t', lineterminator='\n')
        for row in newrows:
            tout.writerow(row)



if __name__ == '__main__':
    data = get_data()
    update_topmed_tsv(data, sys.argv[1])