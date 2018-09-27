import requests
import csv
import sys
import os
from pprint import pprint
from bdbag import bdbag_api
import bagit
import hashlib
import json
from globus_sdk import AccessTokenAuthorizer
from identifier_client.identifier_api import IdentifierClient


sys.path.insert(0, '..')
from login import load_tokens

INPUT_FILE = '../gtex-rnaseq.tsv'
MINID_KEY = 'Argon_GUID'
BAGS_EP = '898e3aae-b8a3-4be2-993b-1cf30c663b84'
WORKSPACE_API = 'https://globus-portal.fair-research.org/4M.4.Fullstacks/' \
                'api/v1/workspaces/'
BAG_DIR = 'bags'
OUTPUT_CSV = 'gtex-rnaseq-argon-results.csv'
CSV_HEADERS = ['gtexid', 'input', 'output', 'size', 'md5', 'location']

MINID_TEST = 'HHxPIZaVDh9u'
MINID_PROD = 'kHAAfCby2zdn'
MINT_PROD = True

# BLACKLISTED_MINIDS = ['ark:/57799/b9SJXtVUT6uTJC', 'ark:/57799/b9NJMc7oAPyVep']
# BLACKLISTED_GTEXID = ['GTEX-XBEC-0002-SM-5SOEV', 'GTEX-XXEK-0001-SM-5JK35']
BLACKLISTED_BAGS = [
    '15375053062719_1_15375154087375.outputs.bdbag.zip',
    '15374773294289_1_15376451019030.outputs.bdbag.zip',
    '15375055772427_1_15375376939148.outputs.bdbag.zip',

]

def get_input_minids():
    with open(INPUT_FILE) as fh:
        reader = csv.reader(fh, delimiter='\t')
        rows = [row for row in reader]
        mindex = rows[0].index(MINID_KEY)
        if mindex < 0:
            raise ValueError('Could not find minid index {}'.format(MINID_KEY))
        filtered = {row[mindex] for row in rows
                    if row[mindex] and row[mindex] != MINID_KEY}
        return list(filtered)

def get_gtex_ws_workspaces():
    tokens = load_tokens()

    ws_tok = tokens['fair_research_data_portal']['access_token']
    headers = {'Authorization': 'Bearer {}'.format(ws_tok)}
    r = requests.get(WORKSPACE_API, headers=headers)
    r.raise_for_status()
    workspaces = r.json()
    gwork = [w for w in workspaces
             if w['metadata'].get('data_id', '').startswith('GTEX')]
    gtex_tasks = [w for w in gwork
                  if w['tasks'][0]['data'].get('job_id', {}).get('workflow_id')
                  ]
    return gtex_tasks

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_gtex_files_by_id():
    lbags = [fname for fname in os.listdir(BAG_DIR)
             if fname.endswith('.zip') and fname not in BLACKLISTED_BAGS]

    bag_info = {}
    for local_bag_archive in lbags:
        local_bag_archive =  os.path.abspath(BAG_DIR + '/' + local_bag_archive)
        local_bag, _ = os.path.splitext(local_bag_archive)
        if not os.path.exists(local_bag):
            try:
                bdbag_api.extract_bag(local_bag_archive,
                                      os.path.dirname(local_bag))
            except RuntimeError:
                continue
        with open(local_bag + '/fetch.txt') as lbfh:
            content = lbfh.read()
            cram_fname = content.split('\t')[0]
            cram_basename = os.path.basename(cram_fname)
            gtex_id = cram_basename.split('.', 1)[0]
            if bag_info.get(gtex_id):
                raise ValueError('Another bag exists with this run info: '
                    '\n1: {}\n2: {}'.format(bag_info.get(gtex_id)['file'],
                    local_bag_archive))
            bag_info[gtex_id] = {
                'id': gtex_id,
                'file': local_bag_archive,
                'basename': os.path.basename(local_bag_archive),
                'size': os.stat(local_bag_archive).st_size,
                'md5': md5(local_bag_archive),
                'location': 'https://bags.fair-research.org/{}'
                            ''.format(os.path.basename(local_bag_archive))
            }


    return bag_info


def fetch_output_minid(gtexid):
    with open(OUTPUT_CSV, 'r') as infh:
        csvin = csv.reader(infh)
        gtexpos = CSV_HEADERS.index('gtexid')
        outpos = CSV_HEADERS.index('output')
        for row in csvin:
            if row[gtexpos] == gtexid:
                return row[outpos]

def mint_minid(file_info):
    namespace = MINID_PROD if MINT_PROD else MINID_TEST
    token = load_tokens()['identifiers.globus.org']['access_token']
    # You must specify the `base_url`
    ic = IdentifierClient('Identifier',
                          base_url='https://identifiers.globus.org/',
                          app_name='My Local App',
                          authorizer=AccessTokenAuthorizer(token)
                          )

    kwargs = {
        'visible_to': ['public'],
        'location': [file_info['location']],
        'checksums': [{
            'function': 'md5',
            'value': file_info['md5']
        }],
        'metadata': {
            'Title': 'TOPMED workflow output results for {}.recab.cram'
                     ''.format(file_info['id']),
            'contentSize': file_info['size']
        }
    }
    kwargs = {k: json.dumps(v) for k, v in kwargs.items()}
    minid = ic.create_identifier(namespace=namespace,
                                 **kwargs)

    return minid.data['identifier']


if __name__ == '__main__':
    workspaces = get_gtex_ws_workspaces()
    fmap = get_gtex_files_by_id()

    rows = [['gtexid', 'input', 'output', 'size', 'md5', 'location', 'status', 'workflow_id']]
    for w in workspaces:
        gtexid = w['metadata']['data_id']
        task = w['tasks'][0]
        file_info = fmap.get(gtexid)
        if file_info:
            output = task['output'][0]['id'] \
                if task['output'] else fetch_output_minid(gtexid)
            if not output:
                output = mint_minid(file_info)
                print('minted: {}'.format(output))
            else:
                print('not minting for: {}'.format(output))
            rows.append([
                gtexid,
                task['input'][0]['id'],
                output,
                file_info['size'],
                file_info['md5'],
                file_info['location'],
                'COMPLETE',
                task['data']['job_id']['workflow_id']
            ])
        else:
            rows.append([
                gtexid,
                task['input'][0]['id'],
                task['output'][0]['id'] if task['output'] else None,
                None,
                None,
                None,
                task['status'],
                task['data']['job_id']['workflow_id']
            ])

    with open(OUTPUT_CSV, 'w') as out:
        csvout = csv.writer(out, rows)
        for row in rows:
            csvout.writerow(row)

    print('{} lines written to {}'.format(len(rows), OUTPUT_CSV))

    # pprint(os.listdir(BAG_DIR))


