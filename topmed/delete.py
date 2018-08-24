#!/usr/bin/env python

import sys

from login import load_search_client

from ingest import INDEX
# Will preview which entires to delete
QUERY = '*'


def main():

    sc = load_search_client()

    results = sc.post_search(INDEX, {'q': QUERY, 'limit': 110})
    subjects = [ent['subject'] for ent in results['gmeta']]

    print('{p}\n{data}\n{p}'.format(p='////// !Danger Zone! //////',
                                    data='\n'.join(subjects[:10])))
    user_input = input('Delete {} subjects listed above on index "{}"? '
                       'Y/N> '.format(len(subjects),
                                      sc.get_index(INDEX).data['display_name'],
                                      )
                       )

    if user_input in ['yes', 'Y', 'y', 'yarr']:
        print('Deleting entries in series to reduce load. \nWorking', end='')
        for subject in subjects:
            sc.delete_subject(INDEX, subject)
            print('.', end='')
            sys.stdout.flush()
        print('Done!')
    else:
        print('Aborting')


if __name__ == '__main__':
    main()