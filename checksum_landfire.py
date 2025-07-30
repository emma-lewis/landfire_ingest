import boto3
import hashlib
import tempfile
import os

# --- AWS Setup ---
bucket_name = "env-data-prod"
s3 = boto3.client("s3", region_name="us-east-2")

# --- LANDFIRE Products with Placeholder Checksums ---
landfire_products = [
    {
        "name": "Existing Vegetation Type (EVT) - CONUS 2024",
        "s3_key": "raw/landfire/evt/LF2024_EVT_250_CONUS.zip",
        "expected_hash": "5e6f68a1c88c269b2edc1780688e04d5"
    },
    {
        "name": "Fuel Model 40 (FBFM40) - CONUS 2024",
        "s3_key": "raw/landfire/fm40/LF2024_FBFM40_250_CONUS.zip",
        "expected_hash": "5906fba8a6c99b37aed0487702f024dd"
    },
    {
        "name": "Forest Canopy Base Height (CBH) - CONUS 2024",
        "s3_key": "raw/landfire/cbh/LF2024_CBH_250_CONUS.zip",
        "expected_hash": "2a46d83cecf5b1d8817efd6ca0539472"
    },
    {
        "name": "Forest Canopy Bulk Density (CBD) - CONUS 2024",
        "s3_key": "raw/landfire/cbd/LF2024_CBD_250_CONUS.zip",
        "expected_hash": "f152b36d3663a5f4528159d0dae8f460"
    },
    {
        "name": "Forest Canopy Cover (CC) - CONUS 2024",
        "s3_key": "raw/landfire/cc/LF2024_CC_250_CONUS.zip",
        "expected_hash": "7946fb38932f2e8c7f855c356ade5729"
    },
    {
        "name": "Forest Canopy Height (CH) - CONUS 2024",
        "s3_key": "raw/landfire/ch/LF2024_CH_250_CONUS.zip",
        "expected_hash": "df0563ac0471d388e36fe7e1e9f7671f"
    },
    {
        "name": "Slope Percent Rise (SlpP) - CONUS 2020",
        "s3_key": "raw/landfire/slpp/LF2020_SlpP_220_CONUS.zip",
        "expected_hash": "66f53096c5f3728e2089be52b2125c22"
    },
    {
        "name": "Aspect (ASP) - CONUS 2020",
        "s3_key": "raw/landfire/asp/LF2020_Asp_220_CONUS.zip",
        "expected_hash": "bbd49eaf6c9cae26c0b3e6d532907b98"
    },
    {
        "name": "Elevation (ELEV) - 2020",
        "s3_key": "raw/landfire/elev/LF2020_Elev_220_CONUS.zip",
        "expected_hash": "d1741fb0feed925e529a97e1a859d1f6"
    },
    {
        "name": "Operational Roads (Roads) - 2023",
        "s3_key": "raw/landfire/roads/LF2023_Roads_240_CONUS.zip",
        "expected_hash": "3236aedc065929d2300a37f477cce496"
    }
]

# --- Function to calculate MD5 hash ---
def calculate_md5(file_path):
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()

# --- Download files from S3 and verify checksums ---
for product in landfire_products:
    print(f"\n--- Checking {product['name']} ---")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, os.path.basename(product['s3_key']))
        print(f"Downloading {product['s3_key']} from s3://{bucket_name}...")
        
        try:
            s3.download_file(bucket_name, product['s3_key'], local_path)
            actual_hash = calculate_md5(local_path)
            
            if actual_hash == product['expected_hash']:
                print("✓ Checksum matches!")
            else:
                print("✗ Checksum mismatch!")
                print(f"Expected: {product['expected_hash']}")
                print(f"Actual:   {actual_hash}")
                if product['expected_hash'].startswith('PLACEHOLDER'):
                    print(f"NOTE: Replace '{product['expected_hash']}' with the actual hash above")
                    
        except Exception as e:
            print(f"Error processing {product['name']}: {e}")
