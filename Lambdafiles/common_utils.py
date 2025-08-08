# common_utils.py
# This file contains common utility functions for interacting with S3
# It provides functions to download and upload files to S3
# and get the bucket names from environment variables or directly defined

import boto3
import os

s3 = boto3.client('s3')

def download_file_from_s3(bucket_name, key, local_path):
    # Download file from S3
    print(f"Downloading s3://{bucket_name}/{key} to {local_path}")
    s3.download_file(bucket_name, key, local_path)
    if not os.path.exists(local_path):
        raise Exception(f"File not found after download: {local_path}")
    print(f"Downloaded {local_path} (size: {os.path.getsize(local_path)} bytes)")

def upload_file_to_s3(bucket_name, key, local_path, content_type=None):
    # Upload file to S3
    print(f"Uploading {local_path} to s3://{bucket_name}/{key}")
    extra_args = {'ContentType': content_type} if content_type else {}
    s3.upload_file(local_path, bucket_name, key, ExtraArgs=extra_args)
    print(f"Uploaded s3://{bucket_name}/{key}")

def get_bucket_names():
    # Get bucket names
    return {
        'input_bucket': os.environ.get('INPUT_S3_BUCKET', 'cognizant-video-input'),
        'output_bucket': os.environ.get('OUTPUT_S3_BUCKET', 'cognizant-video-output')
    }