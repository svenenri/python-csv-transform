"""
Module used for performing transformations to a csv file that are needed
to prepare the csv file for ETL process
"""

from collections import defaultdict
import re
import csv
import boto3
import botocore

def extract_nums(code_list):
    """
    Method to remove numbers from the Project Code and Project Series.
    Args:
        code_list: List based of the column passed in from the csv file.

    Returns:
        code_list: List with numbers removed from column information.
    """

    for idx, code in enumerate(code_list):
        if re.match(r'[0-9]', code[-1:]):
            new_code = code[:-1]
            code_list[idx] = new_code
    return code_list

def download_s3_file(bucket_name, key, file_name):
    """
    Method for downloading csv file to be transformed.
    Args:
        bucket_name: Name of S3 bucket where the file is located.
        key: Key associated with file in the S3 Bucket.
        file_name: Name of file to be downloaded from the S3 bucket.

    Returns:
        Tuple outlining the result. A successful download will return a tuple
        containing a True boolean and the String 'Success'. An unsuccessful
        download will return a tuple containing a False boolean and the error
        response.

    Raises:
        Error associated with failed download of the file from the S3 bucket.
    """

    try:
        print 'Getting ' + key + ' from the ' + bucket_name + ' bucket...'
        data_file = s3.meta.client.download_file(bucket_name, key, '/tmp/' +
                                                 file_name)
        return (True, 'Success')
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == '404':
            data_file = s3.meta.client.download_file(bucket_name, key, '/tmp/' +
                                                     file_name)
            return (False, data_file)
        else:
            raise

# Perform data transformations
def transform(file_name):
    """
    Method to complete required data transformations
    Args:
        file_name: Name of file to be read for transformations

    Returns:
        Zipped object containing data to be written to the csv file
    """

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

        rows = zip(
            columns['Person Account'].insert(0, 'Person Account'),
            columns['Person Code'].insert(0, 'Person Account'),
            columns['Person First Name'].insert(0, 'Person Code'),
            columns['Person First Name'].insert(0, 'Person First Name'),
            columns['Person Last Name'].insert(0, 'Person Last Name'),
            columns['Project Code'].insert(0, 'Project Code'),
            columns['Project Title'].insert(0, 'Project Title'),
            columns['Project End Date'].insert(0, 'Project End Date')
        )

        return rows

# Write to CSV
def csv_file_write(file_name, rows):
    """
    Method for writing transformed data to csv file
    Args:
        file_name: Name of file to be created
        rows: Zipped object containing data to be written to the csv file

    Returns:
        Created file
    """

    name_items = file_name.split('.')
    new_file_name = '/tmp/' + name_items[0] + '_updated.csv'

    with open(new_file_name, 'wb') as csv_write:
        writer = csv.writer(csv_write)

        for row in rows:
            writer.writerow(row)
    return new_file_name

def upload_to_s3(bucket_name, key, file_name):
    """
    Method for uploading transformed csv file back to
    the specified S3 bucket
    Args:
        bucket_name: Bucket to upload transformed csv file
        key: Key to associate file in the S3 Bucket
        file_name: Name of file to upload to S3
    """

    key_list = key.split('/')
    new_key = 'reports/' + key_list[-1]
    try:
        print 'Uploading ' + key + ' to the ' + bucket_name + ' bucket...'
        s3.meta.client.upload_file(file_name, bucket_name, new_key)
        print 'Uploaded'
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == '404':
            print 'The object does not exist.'
        else:
            raise

s3 = boto3.resource('s3')

# Execute
def handler(event, context):
    """
    Handler to execute csv transformations when invoked in AWS Lambda.
    Args:
        event:
        context:
    Returns:
        event_tup: Tuple containing the outcome of the Lambda execuction.
        A successful execution returns the String 'Success', the name of the
        transformed file, and the name of the bucket the transformed file was
        uploaded to.
        An unsuccessful execution returns the String 'There was an issue', the
        name of the transformed file, and the bucket that the module attempted
        to upload the transformed file to.
    """

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key_list = key.split('/')
    file_name = key_list[-1]
    download = download_s3_file(bucket_name, key, file_name)
    # execute = 1

    if download[0]:
        new_file = transform('/tmp/' + file_name)
        write_file = csv_file_write(file_name, new_file)
        new_key_list = write_file.split('/')
        new_key = new_key_list[-1]
        upload_to_s3(bucket_name, '/tmp/' + new_key, write_file)
        event_tup = ('Success', new_key, bucket_name)
        return event_tup

    event_tup = ('There was an issue', new_key, bucket_name)
    return event_tup
