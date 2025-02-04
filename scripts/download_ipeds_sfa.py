import requests
import os
import datetime
import zipfile

def get_remote_file_size(url):
    """
    Tries to retrieve the remote file size from the 'Content-Length' header via a HEAD request.
    Returns an integer size in bytes, or None if not available or if the file doesn't exist on the remote.
    """
    try:
        resp = requests.head(url, allow_redirects=True, timeout=10)
        # If the file doesn't exist on remote, we might get 404, so check status_code first
        if resp.status_code == 200 and 'Content-Length' in resp.headers:
            return int(resp.headers['Content-Length'])
        else:
            return None
    except Exception as e:
        print(f"HEAD request failed for {url}: {e}")
        return None

def get_local_file_size(filepath):
    """
    Returns the size of the local file in bytes, or None if the file does not exist.
    """
    if os.path.exists(filepath):
        return os.path.getsize(filepath)
    else:
        return None

def download_zip(url, local_path):
    """
    Downloads the file from `url` to `local_path` via a GET request.
    Returns True if download succeeded, False otherwise.
    """
    print(f"Downloading from {url} ...")
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded successfully to {local_path}")
                return True
            else:
                print(f"Failed to download. HTTP Status code: {r.status_code}")
                return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def unzip_file(zip_path, extract_folder):
    """
    Unzips the contents of `zip_path` into `extract_folder`.
    """
    print(f"Unzipping {zip_path} ...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_folder)
        print(f"Extracted contents to {extract_folder}")
    except Exception as e:
        print(f"Error unzipping {zip_path}: {e}")

def download_ipeds_sfa():
    """
    Downloads IPEDS Student Financial Aid (SFA) ZIP files from the base year (13 => 2013-14)
    up through the current year in two-digit format (e.g., 23 => 2023-24).
    
    - Checks remote file size vs. local file size to decide whether to download:
        * If local file doesn't exist or file sizes differ -> download & unzip.
        * If file sizes match -> skip both download & unzip (assuming no change).
    - Handles 404 or missing remote files gracefully (just prints a message).
    """

    BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data/"
    DOWNLOAD_FOLDER = r"C:\IPEDS_Data\SFA"

    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    # Start at 2013-14 => '13', go through the current year in 2-digit form
    start_year = 13
    current_year_2digits = datetime.datetime.now().year % 100  # e.g., 23 if it's 2023

    for sy in range(start_year, current_year_2digits + 1):
        ey = sy + 1  # e.g., 13 -> 14, 14 -> 15
        filename = f"SFA{sy:02}{ey:02}.zip"
        file_url = BASE_URL + filename
        local_zip_path = os.path.join(DOWNLOAD_FOLDER, filename)

        print(f"\n--- Checking {filename} ---")
        
        # 1) Check remote file size
        remote_size = get_remote_file_size(file_url)
        if remote_size is None:
            print(f"Remote file not found or HEAD request failed for {filename} (likely not posted yet). Skipping.")
            continue

        # 2) Check local file size
        local_size = get_local_file_size(local_zip_path)

        # 3) Compare sizes to decide whether to download
        if local_size == remote_size:
            print(f"Local file size matches remote ({remote_size} bytes). Skipping download & unzip.")
            continue
        else:
            print(f"Either file missing or size differs (remote: {remote_size}, local: {local_size}).")
            print(f"Will download {filename} now...")

        # 4) Download
        download_succeeded = download_zip(file_url, local_zip_path)

        # 5) If downloaded/updated, unzip
        if download_succeeded:
            unzip_file(local_zip_path, DOWNLOAD_FOLDER)


if __name__ == "__main__":
    download_ipeds_sfa()
