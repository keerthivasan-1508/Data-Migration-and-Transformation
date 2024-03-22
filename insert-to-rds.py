import csv
import json
import boto3
import pandas as pd
import mysql.connector

def download_file_from_s3(bucket_name, object_key, local_file_path):
    # Create an S3 client
	aws_access_key_id= os.environ.get('aws_access_key_id','default')
	aws_secret_access_key= os.environ.get('aws_secret_access_key','default')
	aws_region="us-east-1"
    	# Initialize the S3 client
	s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=aws_region)
	
   	# Download the file
	s3.download_file(bucket_name, object_key, local_file_path)
	print(f"File downloaded successfully from s3://{bucket_name}/{object_key} to {local_file_path}")

def pick_json_to_load():
	with open('~/dataset.json', 'r') as openfile:
		data = json.load(openfile)

	table_name = list(data['facts']['dei'].keys())[0]
	#filter result to pick objects under EntityCommonStockSharesOutstanding
	result = data['facts']['dei'][list(data['facts']['dei'].keys())[0]][list(data['facts']['dei'][list(data['facts']['dei'].keys())[0]].keys())[-1]][list(data['facts']['dei'][list(data['facts']['dei'].keys())[0]][list(data['facts']['dei'][list(data['facts']['dei'].keys())[0]].keys())[-1]].keys())[0]]
	return result, table_name

def json_to_csv(result):
	data_file = open('~/jsonoutput.csv', 'w', newline='')
	csv_writer = csv.writer(data_file)
	count = 0
	for data in result:
		if count == 0:
			header = data.keys()
			csv_writer.writerow(header)
			count += 1
		csv_writer.writerow(data.values())
	data_file.close()

def load_csv_to_mysql(csv_file, table_name, host, user, password, database):
    # Establish MySQL connection
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        allow_local_infile=True
    )
    cursor = conn.cursor()

    # Execute the LOAD DATA LOCAL INFILE command

    cursor.execute(f"LOAD DATA LOCAL INFILE '{csv_file}' "
                       f"INTO TABLE {table_name} "
                       "FIELDS TERMINATED BY ',' "
                       "ENCLOSED BY '\"' "
                       "LINES TERMINATED BY '\r\n'")
     

    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()


bucket_name = 'guvi-capstone'
object_key = 'dataset.json'
local_file_path = '~/dataset.json'
csv_file = '~/jsonoutput.csv'
host= os.environ.get('host','default')
mysql_user= os.environ.get('mysql_user','default')
mysql_password= os.environ.get('mysql_password','default')
database = "guvi"


#download_file_from_s3(bucket_name, object_key, local_file_path)
result, table_name = pick_json_to_load()
json_to_csv(result)
load_csv_to_mysql(csv_file,table_name,host,mysql_user,mysql_password,database)
