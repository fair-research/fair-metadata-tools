import os
from collections import OrderedDict
from urllib.parse import urlparse
import json

from gen_records import (
    get_organized_records, get_remote_file_manifests,
    check_urls_in_rfm_resolve_correctly)

OUTPUT_FILE = 'output.json'
TOPMED_FILENAME = 'topmed-107.tsv'
TOPMED_S3_BUCKET_NAME = 'cgp-commons-public'

gingest = {
    "@version": "2016-11-09",
    "ingest_type": "GMetaList",
    "ingest_data": {
        "@version": "2016-11-09",
        "gmeta": []
    }
}

entry = {
    "@version": "2016-11-09",
    "visible_to": ["public"],
    "content": None,
    "subject": None
}

# Unique fields are different for each cram/crai file, and get their own
# section within each index entry
UNIQUE_FIELDS = ['Google_URL', 'S3_URL', 'Calcium_GUID', 'Helium_GUID',
                 'Xenon_GUID', 'DOS_URI', 'CRAI_URL', 'md5sum', 'size']
NON_UNIQUE_FIELDS = ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Argon_GUID',
                     'Assignment']


if __name__ == '__main__':
    records = get_organized_records()
    # Note: This hammers the s3 server with 200+ HEAD requests
    # check_urls_in_rfm_resolve_correctly(records)
    rfms = [get_remote_file_manifests(r) for r in records]
    records = list(zip(records, rfms))

    ingest_records = []
    for r in records:
        topmed, manifest = r
        cram, crai = topmed
        if cram['CRAI_URL'] == 'crai':
            cram, crai = crai, cram

        irecord = OrderedDict([(f, cram[f]) for f in NON_UNIQUE_FIELDS])
        irecord['cram'] = OrderedDict([(f, cram[f]) for f in UNIQUE_FIELDS])
        irecord['crai'] = OrderedDict([(f, crai[f]) for f in UNIQUE_FIELDS])
        irecord['remote_file_manifest'] = manifest
        url = urlparse(manifest[0]['url'])
        subject = '{}://{}{}'.format(url.scheme, url.netloc,
                                     os.path.dirname(url.path))
        gmeta = entry.copy()
        gmeta['content'] = irecord
        gmeta['subject'] = subject
        ingest_records.append(gmeta)

    with open(OUTPUT_FILE, 'w') as f:
        f.write(json.dumps(ingest_records, indent=4))
        print('Wrote {} reconds to {}'.format(len(ingest_records), OUTPUT_FILE))


