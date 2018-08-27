import os
import csv
import json
import grequests
import boto3
from pprint import pprint

RAW_RECORD_OUTPUT = 'data/GEN_RECORDS_OUTPUT.json'
SAMPLES = {
    'topmed': {
        'filename': 'topmed-107.tsv',
        'bucket': 'cgp-commons-public',
        'bucket_s3_prefix': 's3://cgp-commons-public/topmed_open_access/',
        'bucket_http_prefix': ('https://cgp-commons-public.s3.amazonaws.com/'
                               'topmed_open_access/'),
        'columns': ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Google_URL',
                   'S3_URL', 'Argon_GUID', 'Calcium_GUID', 'Helium_GUID',
                   'Xenon_GUID', 'DOS_URI', 'CRAI_URL', 'md5sum', 'Assignment'],
        's3_name': 'S3_URL',
    },
    'downsample': {
        'filename': 'topmed-downsampled.tsv',
        'bucket': 'topmed-workflow-testing',
        'bucket_s3_prefix': 's3://topmed-workflow-testing/topmed-aligner/',
        'bucket_http_prefix': 'https://topmed-workflow-testing'
                              '.s3.amazonaws.com/topmed-aligner/',
        'columns': ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Google_URL',
                    'AWS_URL', 'Calcium_GUID', 'Helium_GUID', 'DOS_URI',
                    'CRAI_URL', 'md5sum', 'File size',
                    'Calcium_realigned_md5sum', 'Argon_GUID'],
        's3_name': 'AWS_URL',
        'extras': {'Assignment': 'Downsample'}
    }

}


def parse_topmed(sample):
    """Parse the tsv and return record info"""
    TSV_COLUMNS = sample['columns']
    s3_name = sample['s3_name']

    with open(sample['filename']) as t:
        tin = csv.reader(t, delimiter='\t')
        tsv_info = [dict(zip(TSV_COLUMNS, row)) for row in tin]

        # Remove all non-real records by checking that the s3 link looks valid
        filtered_info = [item for item in tsv_info
                         if item[s3_name].startswith('s3://')]
        return filtered_info


def get_topmed_s3_file_info(bucket_name):
    """Retrieve data from the data in the S3 Bucket"""
    conn = boto3.client('s3')
    objects = conn.list_objects(Bucket=bucket_name)['Contents']
    mapped_objects = {os.path.basename(object['Key']): object
                      for object in objects}
    return mapped_objects


def get_organized_records(sample_metadata):
    """Get records and set the record size from info in s3 buckets"""
    topmed_records = parse_topmed(sample_metadata)
    s3info = get_topmed_s3_file_info(sample_metadata['bucket'])
    s3_name = sample_metadata['s3_name']

    for rec in topmed_records:
        # Get the size so we can generate remote file manifests
        s3_filename = os.path.basename(rec[s3_name])
        rec['size'] = s3info[s3_filename]['Size']

        # Set the minid field if it doesn't exist
        rec['Argon_GUID'] = rec.get('Argon_GUID', '')
        rec['Xenon_GUID'] = rec.get('Xenon_GUID', '')
        if not rec.get('S3_URL'):
            rec['S3_URL'] = rec[s3_name]


    record_ids = {r['NWD_ID'] for r in topmed_records}
    collected_records = []
    for rid in record_ids:
        collected_records.append([r for r in topmed_records
                                 if r['NWD_ID'] == rid])

    rfms = [get_remote_file_manifests(r, sample_metadata)
            for r in collected_records]
    records = list(zip(collected_records, rfms))
    return records


def get_remote_file_manifests(record, sample_metadata):
    """
    Build these:

    https://github.com/fair-research/bdbag/blob/master/doc/config.md#remote-file-manifest
    :param record:
    :return:
    """
    s3_prefix = sample_metadata['bucket_s3_prefix']
    http_prefix = sample_metadata['bucket_http_prefix']
    s3_name = sample_metadata['s3_name']
    rfms = []
    for file_info in record:
        file_path = file_info[s3_name].replace(s3_prefix, '')
        d = {
            'url': '{}{}'.format(http_prefix, file_path),
            'length': file_info['size'],
            'filename': os.path.basename(file_path),
            'md5': file_info['md5sum']
        }
        for v in d.values():
            if not v:
                pprint(record)
                raise ValueError('Record is missing values')
        rfms.append(d)
    return rfms


def check_urls_in_rfm_resolve_correctly(records):
    """
    Do HEAD requests on all files in remote file manifests to test that the
    links we build point to real files.
    :return:
    """
    rfms = [r[1] for r in records]



    urls, file_lengths = [], []
    for rfm in rfms:
        for record in rfm:
            urls.append(record['url'])
            file_lengths.append(record['length'])

    rs = (grequests.head(u) for u in urls)
    map = grequests.map(rs)

    failures = [url for request, url in zip(map, urls)
                if request.status_code != 200]

    if not failures:
        print('SUCCESS: All "URL"s in the remote file manifests '
              'resolved to files on s3! ({} files checked)'.format(len(urls)))
    else:
        print('FAIL: The following URLs did not resolve: \n{}'
              .format('\n'.join(failures)))

    responses = [(url, length, request.headers['Content-Length'])
                 for request, url, length in zip(map, urls, file_lengths)
                 if int(request.headers['Content-Length']) != int(length)]
    infos = [('RFM Length: {}, Expected: {}, URL: {}'
             ''.format(length, expected, url))
             for url, length, expected in responses]

    if not responses:
        print('SUCCESS: All sizes match the rfms!')
    else:
        print('FAIL: The following URLs have mismatching size: \n{}'
              ''.format('\n'.join(infos)))

def get_data():
    """Contains a list of all topmed datasets. Each set contains two files,
    the cram file and the crai file. Does not include remote file manifests"""
    records = []

    for sample in SAMPLES.values():
        recs = get_organized_records(sample)
        for rs, rfm in recs:
            for entry in rs:
                entry.update(sample.get('extras', {}))
        records.extend(recs)
    return records


if __name__ == '__main__':
    if not os.path.exists('data'):
        os.mkdir('data')

    data = get_data()

    with open(RAW_RECORD_OUTPUT, 'w') as f:
        f.write(json.dumps(data, indent=4))
    print('Wrote {} Records to {} using data from {}'
          ''.format(len(data),
                    RAW_RECORD_OUTPUT,
                    ', '.join(SAMPLES.keys())
                    )
          )

