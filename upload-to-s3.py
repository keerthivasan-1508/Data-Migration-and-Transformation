import requests
import zipfile
import os
import boto3

def download_zip_file(url, filename):
  r = requests.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
  if r.status_code == 200:
    with open(filename, 'wb') as f:
      r.raw.decode_content = True
      f.write(r.content)
      print('Zip File Downloading Completed')

def extract_zip_file(zip_filename, extract_dir):
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print('Zip File Extraction Completed')

def upload_files_to_s3(directory_path, bucket_name):
    aws_access_key_id= os.environ.get('aws_access_key_id','default')
    aws_secret_access_key= os.environ.get('aws_secret_access_key','default')
    aws_region="us-east-1"
    # Initialize the S3 client
    s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=aws_region)

    # Iterate over all files in the directory
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            # Generate the S3 key (object key) by removing the directory path prefix
            s3_key = "data-migration-to-dynamo"+'/'+file_name
            # Upload the file to S3
            s3.upload_file(file_path, bucket_name,s3_key)
            print(f'Uploaded {file_path} to S3 bucket {bucket_name} with key {s3_key}')


url = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
filename = "companyfacts.zip"
directory = "."
bucket_name = "guvi-capstone"

download_zip_file(url, filename)
extract_zip_file(filename,directory)
upload_files_to_s3(directory, bucket_name)
