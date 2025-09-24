import boto3
import zipfile
import os

# S3 client
s3 = boto3.client('s3')

# Constants
BUCKET_NAME = "aws-python-code-25"
UPLOAD_PREFIX = "uploads/"
ARCHIVE_FOLDER = "archive/"

def lambda_handler(event, context):
    # 1️⃣ Check if archive folder exists, create if not
    archive_check = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=ARCHIVE_FOLDER, MaxKeys=1)
    if 'Contents' not in archive_check:
        s3.put_object(Bucket=BUCKET_NAME, Key=ARCHIVE_FOLDER)
        print(f"Folder '{ARCHIVE_FOLDER}' created in S3 bucket '{BUCKET_NAME}'.")
    else:
        print(f"Folder '{ARCHIVE_FOLDER}' already exists in S3.")

    # 2️⃣ List files in uploads/
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=UPLOAD_PREFIX)
    files = response.get('Contents', [])
    count_files = len(files)
    
    if count_files == 0:
        return {"message": f"No files found in '{UPLOAD_PREFIX}'."}

    data = {}

    # 3️⃣ Process each .zip file
    for obj in files:
        key = obj['Key']
        if key.endswith(".zip"):
            print(f"Processing {key}")
            data[key] = []

            # Local paths
            local_zip_path = f"/tmp/{os.path.basename(key)}"
            extract_dir = f"/tmp/{os.path.splitext(os.path.basename(key))[0]}"
            os.makedirs(extract_dir, exist_ok=True)

            # Download the zip file
            s3.download_file(BUCKET_NAME, key, local_zip_path)

            # Extract
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            print(f"Extracted {key} to {extract_dir}")

            # List extracted files
            extracted_files = []
            for root, dirs, files_in_dir in os.walk(extract_dir):
                for file in files_in_dir:
                    print(f"Found extracted file: {file}")
                    extracted_files.append(file)
            data[key] = extracted_files

    return {
        "count_files": count_files,
        "file_name": data,
        "message": "All zip files processed successfully."
    }
