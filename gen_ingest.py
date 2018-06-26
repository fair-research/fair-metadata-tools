#!/usr/bin/python

"""
The GTEx CSV file seems to be misformatted, so using csv module to deal with
the file requires more effort than it is worth. Let's use a primitive hammer
then.
"""

import json
from gen_datacite import gen_datacite_entry


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

with open('GTEx_lung_cancer_demo_data_v2-1.csv', 'r') as gte:
    lines = gte.read().splitlines()
    field_id = lines[0].split('\t')
    descr = lines[1].split('\t')
    for i in range(len(descr)):
        if descr[i] == '[use text before colon for display text]':
            descr[i] = ''

    #print(field_id)
    #print(descr)

    for c, l in enumerate(lines[3:]):

        entry = {
            "@version": "2016-11-09",
            "visible_to": ["public"],
            "content": {
                "@context": {
                    "gtex": "http://gtex.globuscs.info/meta/GTEx_v7.xsd"
                },
                "gtex:description": {}
            }
        }

        fields = l.split('\t')
        for i in range(len(fields)):

            if field_id[i] == 'Forward_path':
                if fields[i].startswith('ftp.'):
                    fields[i] = 'ftp://' + fields[i]
                elif fields[i] == '':
                    fields[i] = ('ftp://ftp.sra.ebi.ac.uk/vol1/'
                                 + fields[0]
                                 + '/fastq/LC_nRNA_sequence_R1.txt.gz')
                #print(fields[i])
                entry['subject'] = fields[i]

            if field_id[i] == 'Reverse_path':
                if fields[i].startswith('ftp.'):
                    fields[i] = 'ftp://' + fields[i]
                elif fields[i] == '':
                    fields[i] = ('ftp://ftp.sra.ebi.ac.uk/vol1/'
                                 + fields[0]
                                 + '/fastq/LC_nRNA_sequence_R2.txt.gz')
                #print(fields[i])

            if field_id[i] == 'SEX':
                fields[i] = sex[int(fields[i])]
                #print(fields[i])

            if field_id[i] == 'SMOKING_STATUS':
                fields[i] = smoking_status[int(fields[i])]
                #print(fields[i])

            entry['content']['gtex:{}'.format(field_id[i])] = fields[i]
            if descr[i] != '':
                entry['content']['gtex:description']['gtex:{}'.format(field_id[i])] = descr[i]

            entry['content']['gtex:datacite'] = gen_datacite_entry()

        gingest['ingest_data']['gmeta'].append(entry)

        if c % 100 == 0 and c > 0:
            fo = open('gingest/GTEx_v7_gingest_{}_{}.json'.format(c-100, c), 'w')
            fo.write(json.dumps(gingest, indent=4))
            fo.close()
            gingest = {
                "@version": "2016-11-09",
                "source_id": "GTEx v7 by Lukasz Lacinski",
                "ingest_type": "GMetaList",
                "ingest_data": {
                    "@version": "2016-11-09",
                    "gmeta": []
                }
            }
    fo = open('gingest/GTEx_v7_gingest_rest.json', 'w')
    fo.write(json.dumps(gingest, indent=4))
    fo.close()
