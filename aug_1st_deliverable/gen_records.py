import os
import csv
import json
import grequests
import boto3

RAW_RECORD_OUTPUT = 'data/GEN_RECORDS_OUTPUT.json'
TOPMED_FILENAME = 'topmed-107.tsv'
TOPMED_S3_BUCKET_NAME = 'cgp-commons-public'
TOPMED_S3_DOWNSAMPLED = 'topmed-workflow-testing'

# Which bucket are we using?
CURRENT_BUCKET = TOPMED_S3_DOWNSAMPLED



def parse_topmed(topmed_filename):
    """Parse the tsv and return record info"""
    # TSV_COLUMNS = ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Google_URL',
    #                'S3_URL', 'Argon_GUID', 'Calcium_GUID', 'Helium_GUID',
    #                'Xenon_GUID', 'DOS_URI', 'CRAI_URL', 'md5sum', 'Assignment']
    TSV_COLUMNS = ['NWD_ID', 'HapMap_1000G_ID', 'SEQ_CTR', 'Google_URL',
                   'AWS_URL', 'Calcium_GUID', 'DOS_URI', 'CRAI_URL', 'md5sum',
                   'File size', 'Calcium_realigned_md5sum']

    with open(topmed_filename) as t:
        tin = csv.reader(t, delimiter='\t')
        tsv_info = [dict(zip(TSV_COLUMNS, row)) for row in tin]

        # Remove all non-real records by checking that the s3 link looks valid
        filtered_info = [item for item in tsv_info
                         if item['AWS_URL'].startswith('s3://')]
        return filtered_info


def get_topmed_s3_file_info(bucket_name):
    """Retrieve data from the data in the S3 Bucket"""
    conn = boto3.client('s3')
    objects = conn.list_objects(Bucket=bucket_name)['Contents']
    mapped_objects = {os.path.basename(object['Key']): object
                      for object in objects}
    return mapped_objects


def get_organized_records():
    """Get records and set the record size from info in s3 buckets"""
    topmed_records = parse_topmed(TOPMED_FILENAME)
    s3info = get_topmed_s3_file_info(CURRENT_BUCKET)

    # from pprint import pprint
    # pprint(s3info)

    # Get the size so we can generate remote file manifests
    for rec in topmed_records:
        s3_filename = os.path.basename(rec['AWS_URL'])
        rec['size'] = s3info[s3_filename]['Size']


    record_ids = {r['NWD_ID'] for r in topmed_records}
    collected_records = []
    for rid in record_ids:
        collected_records.append([r for r in topmed_records
                                 if r['NWD_ID'] == rid])
    return collected_records



def get_remote_file_manifests(record):
    """
    Build these:

    https://github.com/fair-research/bdbag/blob/master/doc/config.md#remote-file-manifest
    :param record:
    :return:
    """
    if CURRENT_BUCKET == TOPMED_S3_BUCKET_NAME:
        s3_prefix = 's3://cgp-commons-public/topmed_open_access/'
        http_prefix = ('https://cgp-commons-public.s3.amazonaws.com/'
                       'topmed_open_access/')
    else:
        s3_prefix = 's3://topmed-workflow-testing/topmed-aligner/'
        http_prefix = ('https://topmed-workflow-testing.s3.amazonaws.com/'
                       'topmed-aligner/')
    rfms = []
    for file_info in record:
        file_path = file_info['AWS_URL'].replace(s3_prefix, '')
        rfms.append({
            'url': '{}{}'.format(http_prefix, file_path),
            'length': file_info['size'],
            'filename': os.path.basename(file_path),
            'md5': file_info['md5sum']
        })
    return rfms


def check_urls_in_rfm_resolve_correctly(records):
    """
    Do HEAD requests on all files in remote file manifests to test that the
    links we build point to real files.
    :return:
    """
    rfms = [get_remote_file_manifests(r) for r in records]


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
    records = get_organized_records()
    return records


if __name__ == '__main__':
    if not os.path.exists('data'):
        os.mkdir('data')

    data = get_data()
    # WARNING!!! This does hundreds of HEAD Requests. Don't spam it.
    # check_urls_in_rfm_resolve_correctly(data)

    with open(RAW_RECORD_OUTPUT, 'w') as f:
        f.write(json.dumps(data, indent=4))
    print('Wrote {} Records to {}.'.format(len(data), RAW_RECORD_OUTPUT))

