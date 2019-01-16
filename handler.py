import boto3
import botocore
import csv
from collections import defaultdict
import re
import json

# Remove nums from PROJECT Codes and PROJECT Series
def extract_nums(code_list):
    for idx, code in enumerate(code_list):
        if re.match(r'[0-9]', code[-1:]):
            new_code = code[:-1]
            code_list[idx] = new_code
    return code_list

# Download File
def download_s3_file(BUCKET_NAME, KEY, file_name):
    try:
        print 'Getting ' + KEY + ' from the ' + BUCKET_NAME + ' bucket...'
        obj_list = s3.meta.client.list_objects(Bucket = BUCKET_NAME, Prefix = 'reports')
        unanet = s3.meta.client.download_file(BUCKET_NAME, KEY, '/tmp/' + file_name)
        return (True, 'Success')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            unanet = s3.meta.client.download_file(BUCKET_NAME, KEY, '/tmp/' + file_name)
            return (False, unanet)
        else:
            raise

# Perform data transformations
def transform(file_name):
    columns = defaultdict(list)
    with open(file_name, 'rb') as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                for (key, val) in row.items():
                    columns[key].append(val)

            all_data = columns
            project_code = columns['Project External System Code']
            project_code.insert(0, 'PROJECT Code')
            all_data['PROJECT Code'] = all_data.pop('Project External System Code')

            project_series = columns['Project Description']
            project_series.insert(0, 'PROJECT Series')
            all_data['PROJECT Series'] = all_data.pop('Project Description')

            project_code = extract_nums(project_code)
            project_series = extract_nums(project_series)

            sub_acct = columns['Person Account']
            sub_acct.insert(0, 'Person Account')

            person_code = columns['Person Code']
            person_code.insert(0, 'Person Code')

            person_first_name = columns['Person First Name']
            person_first_name.insert(0, 'Person First Name')

            person_last_name = columns['Person Last Name']
            person_last_name.insert(0, 'Person Last Name')

            project_code = columns['Project Code']
            project_code.insert(0, 'Project Code')

            project_title = columns['Project Title']
            project_title.insert(0, 'Project Title')

            project_end_date = columns['Project End Date']
            project_end_date.insert(0, 'Project End Date')

            rows = zip(sub_acct, person_code, person_first_name, person_last_name, project_code, project_series, project_code, project_title, project_end_date)

            return rows

# Write to CSV
def csv_file_write(file_name, rows):
    name_items = file_name.split('.')
    new_file_name = '/tmp/' + name_items[0] + '_updated.csv'

    with open(new_file_name, 'wb') as csv_write:
        writer = csv.writer(csv_write)

        for row in rows:
            writer.writerow(row)
    return new_file_name

# Push to S3
def upload_to_s3(BUCKET_NAME, KEY, file_name):
    key_list = KEY.split('/')
    new_key = 'reports/' + key_list[-1]
    try:
        print 'Uploading ' + KEY + ' to the ' + BUCKET_NAME + ' bucket...'
        s3.meta.client.upload_file(file_name, BUCKET_NAME, new_key)
        print 'Uploaded'
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print 'The object does not exist.'
        else:
            raise

s3 = boto3.resource('s3')

# Execute
def transform(event, context):
    BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']
    KEY = event['Records'][0]['s3']['object']['key']
    key_list = KEY.split('/')
    file_name = key_list[-1]
    download = download_s3_file(BUCKET_NAME, KEY, file_name)
    execute = 1

    if download[0]:
        new_file = transform('/tmp/' + file_name)
        write_file = csv_file_write(file_name, new_file)
        new_key_list = write_file.split('/')
        NEW_KEY = new_key_list[-1]
        upload_to_s3(BUCKET_NAME, '/tmp/' + NEW_KEY, write_file)
        event_tup = ('Success', NEW_KEY, BUCKET_NAME)
        return event_tup
    else:
        print 'There was an issue.'
        print download[1]
        event_tup = ('There was an issue', NEW_KEY, BUCKET_NAME)
        return event_tup
