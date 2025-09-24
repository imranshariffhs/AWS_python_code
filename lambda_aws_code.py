import boto3
import zipfile
import os
import shutil
from pathlib import Path
import pandas as pd
import json
import numpy as np

# S3 client
s3 = boto3.client('s3')

# Constants
BUCKET_NAME = "aws-python-code-25"
UPLOAD_PREFIX = "uploads/"
ARCHIVE_FOLDER = "archive/"

def check_s3_folder_exists(s3, bucket_name, folder_name):
    """Check if a folder exists in S3"""
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name, 
            Prefix=f"{folder_name}/", 
            MaxKeys=1
        )
        return 'Contents' in response
    except Exception as e:
        print(f"Error checking folder existence: {str(e)}")
        return False

def create_s3_folder(s3, bucket_name, folder_name):
    """Create a folder in S3 by uploading an empty object"""
    try:
        #s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/")
        print(f"Folder '{folder_name}' created in S3 bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error creating folder '{folder_name}': {str(e)}")

def download_and_extract_zip(s3, bucket_name, s3_key, local_zip_path, extract_dir):
    """Download ZIP from S3 and extract it locally"""
    try:
        # Download the ZIP file
        print(f"Downloading {s3_key} to {local_zip_path}")
        s3.download_file(bucket_name, s3_key, local_zip_path)        
        # Extract the ZIP file
        print(f"Extracting {local_zip_path} to {extract_dir}")
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        # Get list of extracted files
        extracted_files = []
        for root, dirs, files_in_dir in os.walk(extract_dir):
            for file in files_in_dir:
                # Get relative path from extract_dir
                rel_path = os.path.relpath(os.path.join(root, file), extract_dir)
                extracted_files.append(rel_path)
                print(f"Found extracted file: {rel_path}")
        
        return extracted_files
        
    except zipfile.BadZipFile:
        print(f"Error: {s3_key} is not a valid ZIP file")
        raise
    except Exception as e:
        print(f"Error downloading/extracting {s3_key}: {str(e)}")
        raise

def upload_extracted_files_to_s3(s3, bucket_name, extract_dir, folder_name, extracted_files,RESULT_FOLDER = "result"):
    """Upload all extracted files to S3 in the designated folder"""
    try:
        
        for file_path in extracted_files:
                local_file_path = os.path.join(extract_dir, file_path)
                s3_key = f"{RESULT_FOLDER}/{file_path}"
                print(f"Uploading {local_file_path} to {s3_key}")
                s3.upload_file(local_file_path, BUCKET_NAME, s3_key)
            
    except Exception as e:
        print(f"Error uploading files: {str(e)}")
        raise

def archive_original_zip(s3, bucket_name, original_key, archive_folder, data_archive_path):
    """Move the original ZIP file to archive folder"""
    try:
        # Create destination key
        zip_filename = original_key.split("/")[-1]
        dest_key = f"{archive_folder.rstrip('/')}/{zip_filename}"
        
        # Copy to archive
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': original_key},
            Key=dest_key
        )
        
        # Delete original
        s3.delete_object(Bucket=bucket_name, Key=original_key)
        
        # Update archive path list
        data_archive_path.append(dest_key)
        print(f"Moved {original_key} to {dest_key}")
        
    except Exception as e:
        print(f"Error archiving {original_key}: {str(e)}")
        raise

def cleanup_local_directory(directory_path):
    """Remove directory and all its contents if it exists"""
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"Cleaned up existing directory: {directory_path}")

def cleanup_local_files(zip_path, extract_dir):
    """Clean up local temporary files"""
    try:
        # Remove ZIP file
        if os.path.exists(zip_path):
            os.remove(zip_path)
            print(f"Cleaned up: {zip_path}")
        
        # Remove extraction directory
        if os.path.exists(extract_dir):
            shutil.rmtree(extract_dir)
            print(f"Cleaned up: {extract_dir}")
            
    except Exception as e:
        print(f"Warning: Could not clean up local files: {str(e)}")

def lambda_handler(event, context):
    # 1️⃣ Check if archive folder exists, create if not
    archive_check = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=ARCHIVE_FOLDER, MaxKeys=1)
    if 'Contents' not in archive_check:
        s3.put_object(Bucket=BUCKET_NAME, Key=ARCHIVE_FOLDER)
        print(f"Folder '{ARCHIVE_FOLDER}' created in S3 bucket '{BUCKET_NAME}'.")
    else:
        print(f"Folder '{ARCHIVE_FOLDER}' already exists in S3.")

    # 2️⃣ List files in uploads/
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=UPLOAD_PREFIX,MaxKeys=1)
    files = response.get('Contents', [])
    count_files = len(files)
    
    if count_files == 0:
        return {"message": f"No files found in '{UPLOAD_PREFIX}'."}

    data = {}
    data_archive_path=[]

    # 3️⃣ Process each .zip file
    for obj in files:
        key = obj['Key']
        # Initialize variables so they are always defined
        local_zip_path, extract_dir = None, None
        if key.endswith(".zip"):
            print(f"Processing {key}")
            print(f'Folder Name {key.split("/")[1].split(".")[0]}')            
            data[key] = []

            # Extract folder name from zip file path
            zip_filename = key.split("/")[-1]  # Get filename from path
            folder_name = Path(zip_filename).stem  # Remove .zip extension
            print(f'Folder Name: {folder_name}')
            
            # Check if folder already exists in S3
            folder_exists = check_s3_folder_exists(s3, BUCKET_NAME, folder_name)
            
            if not folder_exists:
                create_s3_folder(s3, BUCKET_NAME, folder_name)
            else:
                print(f"Folder '{folder_name}' already exists in S3.")
            
            # Set up local paths
            local_zip_path = f"/tmp/{zip_filename}"
            extract_dir = f"/tmp/{folder_name}"
            
        try:
            # Clean up any existing extraction directory
            cleanup_local_directory(extract_dir)
            
            # Create extraction directory
            os.makedirs(extract_dir, exist_ok=True)
            
            # Download and extract ZIP file
            extracted_files = download_and_extract_zip(
                s3, BUCKET_NAME, key, local_zip_path, extract_dir
            )
            
            # Upload extracted files to S3
            upload_extracted_files_to_s3(
                s3, BUCKET_NAME, extract_dir, folder_name, extracted_files
            )
            
            # Store extracted file list in data
            data[key] = extracted_files
            
            # Archive the original ZIP file
            archive_original_zip(s3, BUCKET_NAME, key, ARCHIVE_FOLDER, data_archive_path)
            
        except Exception as e:
            print(f"Error processing {key}: {str(e)}")
            
        finally:
            # Clean up local files
            cleanup_local_files(local_zip_path, extract_dir)

    return {
        "count_files": count_files,
        "file_name": data,
        "move_path":data_archive_path,
        
        "message": "All zip files processed successfully."
    }
