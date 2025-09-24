import boto3
import os
import shutil
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get values from environment
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Configure S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def upload_zip_to_s3(local_file_path, s3_key):
    """
    Uploads a local .zip file to S3
    """
    try:
        s3.upload_file(local_file_path, BUCKET_NAME, s3_key)
        print(f"✅ Uploaded {local_file_path} to s3://{BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        print(f"❌ Upload failed for {local_file_path}: {e}")
        return False

if __name__ == "__main__":
    local_dir = "./zips"       # Folder containing zip files
    archive_dir = "./archive"  # Folder where uploaded zips will be moved

    os.makedirs(archive_dir, exist_ok=True)

    # Loop through all .zip files in the local_dir
    for file_name in os.listdir(local_dir):
        if file_name.endswith(".zip"):
            local_zip_path = os.path.join(local_dir, file_name)
            s3_key = f"uploads/{file_name}"

            # Upload to S3
            if upload_zip_to_s3(local_zip_path, s3_key):
                # Move file to archive if upload was successful
                archive_path = os.path.join(archive_dir, file_name)
                shutil.move(local_zip_path, archive_path)
                print(f"📦 Moved {file_name} → {archive_path}")
