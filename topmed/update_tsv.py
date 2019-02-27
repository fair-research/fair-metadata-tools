import sys
import csv
from gen_records import SAMPLES

MINID_KEY = 'Argon_GUID'


def rebase_topmed(old_file, new_file, keyname='Argon_GUID'):
    """Apply new minid changes to a diffing topmed file from our own"""
    with open(new_file) as newfd, open(old_file) as oldfd:
        new_tsv = csv.reader(newfd, delimiter='\t')
        old_tsv = csv.reader(oldfd, delimiter='\t')
        new_rows = [r for r in new_tsv]
        old_rows = [r for r in old_tsv]


        if len(new_rows) != len(old_rows):
            print('Lines in files differ -- New:"{}" {} Old "{}": {}'.format(
                new_file, len(new_rows), oldf, len(old_rows)
            ))
            return

        oldh = get_headers(old_rows, keyname, old_file)
        newh = get_headers(new_rows, keyname, new_file)
        if len(oldh) != len(newh):
            print('Headers differ between {} and {}!'.format(oldf, new_file))
            print('{}: \n\t{}\n{}: \n\t{}'.format(oldf, oldh, new_file, newh))
        oldkey, newkey = oldh.index(keyname), newh.index(keyname)


        output_rows = []
        changed = 0
        for oldr, newr in zip(old_rows, new_rows):
            if len(oldr) != len(oldh) or len(newr) != len(newh):
                print('Invalid line, skipping...')
                continue

            if oldr[oldkey].startswith('ark') and oldr[oldkey] != newr[newkey]:
                print('U', end='')
                newr[newkey] = oldr[oldkey]
                changed += 1
            # elif not newr[newkey]:
            #     print('A', end='')
            #     newr[newkey] = oldr[oldkey]
            #     changed += 1
            else:
                print('.', end='')

            output_rows.append(newr)
        print('\nRecords Changed: {}\n'
              '\tU: Updated\n\tA: Did not exist, added now.\n'
              '\t.: Not changed'.format(changed))

    ask = input('Update {}?'.format(new_file)).strip()
    affirmative = ['y', 'Y', 'yes', 'yarr']
    outf = new_file if ask in affirmative else 'rebased_topmed.tsv'

    with open(outf, 'w') as t:
        tout = csv.writer(t, delimiter='\t', lineterminator='\n')
        for row in output_rows:
            tout.writerow(row)
        print('Output written to {}'.format(outf))

def get_headers(tsv_rows, keyname, filename=''):
    for row in tsv_rows:
        if keyname in row:
            return row

    raise ValueError('{} Unable to find header row given key {}'.format(
        filename, keyname
    ))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: {} FILE_CONTAINING_YOUR_INFO FILE_TO_UPDATE'
              ''.format(sys.argv[0]))
    else:
        rebase_topmed(sys.argv[1], sys.argv[2])