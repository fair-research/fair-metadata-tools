#!/usr/bin/env python
"""
Helper script to ingest generated data to a search index.

You must have access to a search index for this to work.

"""
import json
import globus_sdk

from login import load_tokens

# 'nihcommons-topmed' index
INDEX = 'd740440b-4f0f-4687-9573-0a7ce2ceda22'
SEARCH_DATA = 'gmeta_ingest_doc.json'

def main():
    with open(SEARCH_DATA) as f:
        ingest_doc = json.loads(f.read())

    tokens = load_tokens()
    auther = globus_sdk.AccessTokenAuthorizer(
        tokens['search.api.globus.org']['access_token'])
    sc = globus_sdk.SearchClient(authorizer=auther)

    preview = [ent['subject'] for ent in ingest_doc['ingest_data']['gmeta']]
    print('\n'.join(preview))
    print('Ingest these to "{}"?'.format(
        sc.get_index(INDEX).data['display_name']))
    user_input = input('Y/N> ')
    if user_input in ['yes', 'Y', 'y', 'yarr']:
        sc.ingest(INDEX, ingest_doc)
        print('Finished')
    else:
        print('Aborting')


if __name__ == '__main__':
    main()
