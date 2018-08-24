import os
from collections import OrderedDict
from urllib.parse import urlparse
import json

from gen_records import RAW_RECORD_OUTPUT

OUTPUT_FILE = 'gmeta_ingest_doc.json'

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

def get_records():
    with open(RAW_RECORD_OUTPUT) as f:
        records = json.loads(f.read())
    return records

def gen_gmeta():
    records = get_records()
    # Note: This hammers the s3 server with 200+ HEAD requests
    # check_urls_in_rfm_resolve_correctly(records)

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

    document = gingest.copy()
    document['ingest_data']['gmeta'] = ingest_records
    return document


if __name__ == '__main__':
    document = gen_gmeta()
    records = get_records()
    with open(OUTPUT_FILE, 'w') as f:
        f.write(json.dumps(document, indent=4))
        print('Wrote {} reconds to {}'.format(len(records), OUTPUT_FILE))


