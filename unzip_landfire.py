import boto3
import os
import zipfile
import tempfile

def unzip_and_upload_landfire_archives(bucket_name: str, prefix: str = "raw/landfire/"):
    """
    Unzips LANDFIRE .zip files stored in an S3 bucket and uploads the extracted contents
    to a corresponding 'unzipped/' folder in the same prefix structure.
    """
    s3 = boto3.client("s3", region_name="us-east-2")

    # List all ZIP files under the specified prefix
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith('.zip'):
                print(f"Processing {key}")
                with tempfile.TemporaryDirectory() as tmpdir:
                    local_zip_path = os.path.join(tmpdir, os.path.basename(key))
                    unzip_dir = os.path.join(tmpdir, "unzipped")

                    # Step 1: Download the ZIP file
                    s3.download_file(bucket_name, key, local_zip_path)

                    # Step 2: Extract contents
                    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(unzip_dir)

                    # Step 3: Upload extracted files
                    base_prefix = '/'.join(key.split('/')[:-1])
                    for root, _, files in os.walk(unzip_dir):
                        for filename in files:
                            local_file_path = os.path.join(root, filename)
                            s3_key = f"{base_prefix}/unzipped/{filename}"
                            print(f"Uploading: {s3_key}")
                            s3.upload_file(local_file_path, bucket_name, s3_key)

    print("All unzipped files uploaded successfully.")

if __name__ == "__main__":
    unzip_and_upload_landfire_archives(bucket_name="env-data-prod")