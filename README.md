# LANDFIRE Data Ingest

This repository contains scripts for downloading LANDFIRE geospatial data and storing it in AWS S3.

## Data Source

Data is streamed from the LANDFIRE Full Extent Downloads page:
https://landfire.gov/data/FullExtentDownloads

## Overview

This toolkit provides three main functions:

1. **Stream LANDFIRE data directly to S3** - Downloads files from LANDFIRE servers and uploads them to S3 without local storage
2. **Extract ZIP files in S3** - Unzips the downloaded archives and stores extracted contents
3. **Validate checksums** - Verifies file integrity using MD5 checksums

## Scripts

### `upload_landfire_to_s3.py`
Streams LANDFIRE data files directly from LANDFIRE servers to S3.

**Features:**
- Downloads 10 LANDFIRE products (EVT, FBFM40, CBH, CBD, CC, CH, SlpP, ASP, ELEV, Roads)
- Streams files directly to S3 without local storage
- Retry logic with exponential backoff
- Skips files that already exist with correct size

**Usage:**
```bash
python upload_landfire_to_s3.py
```

### `unzip_landfire.py`
Extracts ZIP archives stored in S3.

**Features:**
- Processes all ZIP files in the LANDFIRE S3 prefix
- Uploads extracted contents to corresponding unzipped/ folders
- Uses temporary directories to avoid local storage

**Usage:**
```bash
python unzip_landfire.py
```

### `checksum_landfire.py`
Validates integrity of LANDFIRE files in S3 using MD5 checksums.

**Features:**
- Downloads files from S3 temporarily for validation
- Compares actual MD5 hashes against expected LANDFIRE checksums

**Usage:**
```bash
python checksum_landfire.py
```

## Prerequisites

1. **Python 3.x** with required packages:
   ```bash
   pip install boto3 requests owslib
   ```

2. **AWS Credentials** configured via:
   - AWS CLI (`aws configure`)
   - Environment variables
   - IAM roles (if running on EC2)

3. **S3 Access** to the `env-data-prod` bucket in `us-east-2` region
