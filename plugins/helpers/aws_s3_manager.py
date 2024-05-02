import os
import sys
import boto3
import pandas as pd 
import numpy as np 
import pymysql
import mysql.connector
from dotenv import load_dotenv
load_dotenv()


class AWS_S3Manager:

    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    def upload_file(self, local_filename, s3_key):
        
        '''
        arguments: local_filename, s3_key
        
        Upload file to S3_bucket 
        ''' 
        try:
            self.s3_client.upload_file(local_filename, self.bucket_name, s3_key)
            print(f"File '{local_filename}' uploaded to S3 bucket as '{s3_key}'.")

        except Exception as e:
            print(e)
        
        
    def download_file(self, s3_key, local_filename, target_directory="downloads"):
        
        '''
        arguments: local_filename, target_directory,s3_key
        
        Downloading the file to the target directory specified   in the S3_bucket configuration file and 
        rename the file to the local_filename specified in the S3_bucket configuration file.
        '''
        try:

            if not os.path.exists(target_directory):
                os.makedirs(target_directory)# create the target directory if it doesn't exist

            # Construct the full local path
            local_path = os.path.join(target_directory, local_filename)

            # Download the file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"File '{s3_key}' downloaded from S3 bucket and saved as '{local_path}'.")

        except Exception as e:
            print(e)

    def read_csv_from_s3(self, s3_key):
        
        '''
        arguments: s3_key
        
        Reads CSV file from S3 bucket.
        
        returns: data frame 
        '''
        try:
        
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            df = pd.read_csv(obj['Body'])
            return df
        
        except Exception as e:
            print(e)

