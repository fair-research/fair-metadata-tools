"""
This will create minids for runs that don't have them, and update the CSV with
output data for completed runs. This script will only work for the 100 Argon
GTEX WGS samples Nick submitted here:

https://docs.google.com/spreadsheets/d/1uqdLKooVhVS8pKVhFwN6qOq9oMd841ZnY5g7dpnZNgM/edit?&ts=5ba96f47&actionButton=1#gid=244016951

This script requires:
 globus_sdk
    -- pip install globus_sdk
 globus-identifier-client
    -- pip install -e git+git@github.com:globusonline/globus-identifier-client.git#egg=globus-identifier-client
 bdbag
    -- pip install bdbag

Instructions:
You need to copy the output BDBags from the remote location locally in order
to traverse them. Copy them into BAG_DIR listed below from this URL:

https://app.globus.org/file-manager?origin_id=898e3aae-b8a3-4be2-993b-1cf30c663b84&origin_path=%2F

Then run the script:

python minid_for_orphaned_gtex.py

Please update the google doc by using File --> Import --> Upload and pointing it
to the output file generated by this script.

"""
import requests
import csv
import sys
import os
from bdbag import bdbag_api
import hashlib
import json
from globus_sdk import AccessTokenAuthorizer
from identifier_client.identifier_api import IdentifierClient


sys.path.insert(0, '..')
from login import load_tokens

# The local directory with all BDbag archived output
BAG_DIR = 'bags'
# Create Production Minids? Test Minids if False
MINT_PROD = True
# File to grab input minids and GTEX IDs. See:
# https://github.com/dcppc/full-stacks/blob/master/gtex-wgs.tsv
INPUT_FILE = '../gtex-wgs.tsv'
# Contains workflow ids for Nick's runs. This is used instead of calling out
# to the workspaces api, since you are probably not Nick, the correct Nick, or
# are currently unavailable in Alaska.
GTEX_WORKSPACES_INFO = 'workspace_task_info.json'
MINID_KEY = 'Argon_GUID'
# Not currently used. Current location of bags
# BAGS_EP = '898e3aae-b8a3-4be2-993b-1cf30c663b84'
WORKSPACE_API = 'https://globus-portal.fair-research.org/4M.4.Fullstacks/' \
                'api/v1/workspaces/'
OUTPUT_CSV = 'gtex-wgs-argon-results.csv'
CSV_HEADERS = ['gtexid', 'input', 'output', 'size', 'md5', 'location']

MINID_TEST = 'HHxPIZaVDh9u'
MINID_PROD = 'kHAAfCby2zdn'

# Bags that errored out and are invalid. They will match the GTEX IDs for
# next runs, so this script will know to use those bags and ignore these.
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
    # tokens = load_tokens()
    #
    # ws_tok = tokens['fair_research_data_portal']['access_token']
    # headers = {'Authorization': 'Bearer {}'.format(ws_tok)}
    # r = requests.get(WORKSPACE_API, headers=headers)
    # r.raise_for_status()
    # workspaces = r.json()
    with open(GTEX_WORKSPACES_INFO, 'r') as wf_handler:
        workspaces = json.loads(wf_handler.read())

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
            'Title': 'CRAM Alignment file for GTEx Sample {}.recab.cram'
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


