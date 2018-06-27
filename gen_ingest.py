import os
import csv
import json
from urllib.parse import urlparse
from gen_datacite import gen_datacite_entry
from gen_datacite import add_datacite_entries


sex = [
    '',
    'male',
    'female'
]

smoking_status = [
    'never smoker',
    'smoker',
    'current smoker',
    'unknown'
]

gingest = {
    "@version": "2016-11-09",
    "source_id": "GTEx v7 by Lukasz Lacinski",
    "ingest_type": "GMetaList",
    "ingest_data": {
        "@version": "2016-11-09",
        "gmeta": []
    }
}

with open('GTEx_lung_cancer_demo_data_v2.txt', 'r') as gte:
    readcsv = csv.reader(gte, dialect='excel-tab')

    headers = readcsv.__next__()
    readcsv.__next__()
    descriptions = readcsv.__next__()
    for i in range(len(descriptions)):
        if descriptions[i].startswith('[use text'):
            descriptions[i] = ''

    counter = 1
    list_size = 1
    for record in readcsv:
        # skip empty lines
        if not len(record):
            continue
        # skip records without fastq files
        if not record[0].startswith('GSM'):
            continue
        print(record[0], counter)

        entry = {
            "@version": "2016-11-09",
            "visible_to": ["public"],
            "content": {
                "@context": {
                    "gtex": "http://gtex.globuscs.info/meta/GTEx_v7.xsd",
                    "datacite": "https://schema.datacite.org/meta/kernel-4.1/metadata.xsd"
                },
                "gtex:description": {}
            }
        }
        remote_file_manifest = [
            {
                "filename": None,
                "length": None,
                "md5": None,
                "url": None
            }, {
                "filename": None,
                "length": None,
                "md5": None,
                "url": None
            }
        ]

        for i in range(len(record)):
            if headers[i].startswith('Paths'):
                paths = record[i].replace('ftp.sra.ebi.ac.uk/vol1', 'globus://9e437f9e-7e22-11e5-9931-22000b96db58/gridftp/ena')
                forward_url, reverse_url = paths.split(';')
                entry['subject'] = forward_url
                parsed_forward_url = urlparse(forward_url)
                remote_file_manifest[0]['filename'] = os.path.basename(parsed_forward_url.path)
                remote_file_manifest[0]['url'] = forward_url
                parsed_reverse_url = urlparse(reverse_url)
                reverse_filename = os.path.basename(parsed_reverse_url.path)
                remote_file_manifest[1]['filename'] = os.path.basename(parsed_reverse_url.path)
                remote_file_manifest[1]['url'] = reverse_url
                entry['content']['gtex:forward_path'] = forward_url
                entry['content']['gtex:reverse_path'] = reverse_url
                continue
            if headers[i].startswith('Byte length'):
                forward_length, reverse_length = record[i].split(';')
                remote_file_manifest[0]['length'] = forward_length
                remote_file_manifest[1]['length'] = reverse_length
                entry['content']['gtex:forward_length'] = forward_length
                entry['content']['gtex:reverse_length'] = reverse_length
                continue
            if headers[i].startswith('MD5sum'):
                forward_md5, reverse_md5 = record[i].split(';')
                remote_file_manifest[0]['md5'] = forward_md5
                remote_file_manifest[1]['md5'] = reverse_md5
                entry['content']['gtex:forward_md5'] = forward_md5
                entry['content']['gtex:reverse_md5'] = reverse_md5
                continue

            if headers[i] == 'SEX':
                record[i] = sex[int(record[i])]
                #print(record[i])

            if headers[i] == 'SMOKING_STATUS':
                record[i] = smoking_status[int(record[i])]
                #print(record[i])

            entry['content']['gtex:{}'.format(headers[i])] = record[i]
            if descriptions[i] != '':
                entry['content']['gtex:description']['gtex:{}'.format(headers[i])] = descriptions[i]

        entry['content']['remote_file_manifest'] = remote_file_manifest
        #entry['content']['gtex:datacite'] = gen_datacite_entry()
        add_datacite_entries(entry['content'])

        gingest['ingest_data']['gmeta'].append(entry)

        if counter % list_size == 0:
            fo = open('gingest/GTEx_v7_gingest_{}_{}.json'.format(counter-list_size+1, counter), 'w')
            fo.write(json.dumps(gingest))
            fo.close()
            gingest['ingest_data']['gmeta'] = []
        counter += 1

    if counter % list_size != 1 and list_size != 1:
        fo = open('gingest/GTEx_v7_gingest_rest.json', 'w')
        fo.write(json.dumps(gingest))
        fo.close()
