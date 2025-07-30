import boto3
import requests
import time

# --- AWS S3 Configuration ---
bucket_name = "env-data-prod"
region = "us-east-2"

# Initialize the S3 client with specified region
s3 = boto3.client("s3", region_name="us-east-2")

# Confirm connection to the S3 Bucket
try:
    s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    print(f"Connected to S3 bucket: {bucket_name}")
except Exception as e:
    print(f"Connection to S3 failed: {e}")
    exit()

# --- List of LANDFIRE files to download and upload to S3 ---
files_to_download = [
    {
        # Existing Vegetation Type (EVT) - CONUS 2024
        "url": "https://landfire.gov/data-downloads/US_250/LF2024_EVT_250_CONUS.zip",
        "s3_key": "raw/landfire/evt/LF2024_EVT_250_CONUS.zip"
    },

    {
        # Fuel Model 40 (FBFM40) - CONUS 2024
        "url": "https://landfire.gov/data-downloads/US_250/LF2024_FBFM40_250_CONUS.zip",
        "s3_key": "raw/landfire/fm40/LF2024_FBFM40_250_CONUS.zip"
    },

    {
        # Forest Canopy Base Height (CBH) - CONUS 2024
        "url": "https://landfire.gov/data-downloads/US_250/LF2024_CBH_250_CONUS.zip",
        "s3_key": "raw/landfire/cbh/LF2024_CBH_250_CONUS.zip"
    },

    {
        # Forest Canopy Bulk Density (CBD) - CONUS 2024
        "url": "https://landfire.gov/data-downloads/US_250/LF2024_CBD_250_CONUS.zip",
        "s3_key": "raw/landfire/cbd/LF2024_CBD_250_CONUS.zip"
    },

    {
        # Forest Canopy Cover (CC) - CONUS 2024
        "url": "https://landfire.gov/data-downloads/US_250/LF2024_CC_250_CONUS.zip",
        "s3_key": "raw/landfire/cc/LF2024_CC_250_CONUS.zip"
    },

    {
        # Forest Canopy Height (CH) - CONUS 2024
        "url": "https://landfire.gov/data-downloads/US_250/LF2024_CH_250_CONUS.zip",
        "s3_key": "raw/landfire/ch/LF2024_CH_250_CONUS.zip"
    },

    {
        # Slope Percent Rise (SlpP) - CONUS 2020
        "url": "https://landfire.gov/data-downloads/US_Topo_2020/LF2020_SlpP_220_CONUS.zip",
        "s3_key": "raw/landfire/slpp/LF2020_SlpP_220_CONUS.zip"
    },

    {
        # Aspect (ASP) - CONUS 2020
        "url": "https://landfire.gov/data-downloads/US_Topo_2020/LF2020_Asp_220_CONUS.zip",
        "s3_key": "raw/landfire/asp/LF2020_Asp_220_CONUS.zip"
    },

    {
        # Elevation (ELEV) - 2020
        "url": "https://landfire.gov/data-downloads/US_Topo_2020/LF2020_Elev_220_CONUS.zip",
        "s3_key": "raw/landfire/elev/LF2020_Elev_220_CONUS.zip"
    },

    {
        # Operational Roads (Roads) - 2023
        "url": "https://landfire.gov/data-downloads/US_240/LF2023_Roads_240_CONUS.zip",
        "s3_key": "raw/landfire/roads/LF2023_Roads_240_CONUS.zip"
    }
]

# --- Function to stream each file directly into S3 ---
def stream_file_to_s3(url, bucket, key, retries=3, backoff=30):
    for attempt in range(1, retries + 1):
        try:
            print(f"Attempt {attempt}: Downloading from {url}")
            response = requests.get(url, stream=True, timeout=600)
            response.raise_for_status()

            print(f"Uploading to s3://{bucket}/{key}")
            s3.upload_fileobj(response.raw, bucket, key)
            print("Upload complete.\n")
            return  # success
        except Exception as e:
            print(f"Attempt {attempt} failed for {key}: {e}")
            if attempt < retries:
                print(f"Retrying in {backoff} seconds...\n")
                time.sleep(backoff)
            else:
                print(f"Failed to upload {key} after {retries} attempts.\n")

# --- Helper functions to improve reliability ---
def get_remote_file_size(url):
    try:
        head = requests.head(url, allow_redirects=True, timeout=60)
        return int(head.headers.get("Content-Length", 0))
    except:
        return 0

def s3_object_exists_and_matches(bucket, key, expected_size):
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return response['ContentLength'] == expected_size
    except s3.exceptions.ClientError:
        return False

# --- Upload all files in the list, skipping any that already exist ---
for file in files_to_download:
    expected_size = get_remote_file_size(file["url"])
    if s3_object_exists_and_matches(bucket_name, file["s3_key"], expected_size):
        print(f"Skipping {file['s3_key']}, already uploaded correctly.")
        continue
    stream_file_to_s3(file["url"], bucket_name, file["s3_key"])