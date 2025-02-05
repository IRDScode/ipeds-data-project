import os
import requests
import zipfile
import datetime
import pandas as pd

def download_file(url, local_path):
    """
    Utility function to download a file. Returns True if successful.
    """
    print(f"Downloading from {url} ...")
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded to {local_path}")
                return True
            else:
                print(f"Failed to download {url}. Status code: {r.status_code}")
                return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def unzip_and_find_hd_csv(zip_path, extract_folder):
    """
    Unzips the HD zip. Looks for a CSV file that likely contains the HD data, e.g. 'HD2023.csv'.
    Returns the path to that CSV, or None if not found.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_folder)
    except Exception as e:
        print(f"Error unzipping {zip_path}: {e}")
        return None
    
    # We'll look for a file named 'HD2023.csv' or anything that starts with 'hd' or 'HD'.
    for root, dirs, files in os.walk(extract_folder):
        for f in files:
            if f.lower().startswith("hd") and f.lower().endswith(".csv"):
                return os.path.join(root, f)
    return None

def download_latest_hd_file(hd_folder=r"C:\IPEDS_Data\HD"):
    """
    Searches for the most recent IPEDS Header (HD) file by trying HD2023.zip, HD2022.zip, etc.,
    from the current year downward. If found, downloads/unzips it and returns the path to the CSV.
    Otherwise, returns None.
    """
    if not os.path.exists(hd_folder):
        os.makedirs(hd_folder)
    
    base_url = "https://nces.ed.gov/ipeds/datacenter/data/"
    
    current_year = datetime.datetime.now().year

    for year in range(current_year, 2010, -1):  # Adjust lower bound if needed
        hd_zip_name = f"HD{year}.zip"
        hd_url = base_url + hd_zip_name
        print(f"Attempting HEAD for {hd_url}")

        try:
            resp = requests.head(hd_url, allow_redirects=True, timeout=10)
            if resp.status_code == 200:
                # Found it. Download if not local already
                zip_path = os.path.join(hd_folder, hd_zip_name)
                if not os.path.exists(zip_path):
                    if not download_file(hd_url, zip_path):
                        continue
                # Now unzip & find the CSV
                hd_csv = unzip_and_find_hd_csv(zip_path, hd_folder)
                if hd_csv:
                    print(f"Using HD file: {hd_csv}")
                    return hd_csv
        except Exception as e:
            print(f"HEAD request failed for {hd_url}: {e}")

    print("No HD file found in the checked range.")
    return None

def merge_instnm(
    sfa_renamed_csv=r"C:\IPEDS_Data\SFA\combined_ipeds_sfa_renamed.csv",
    output_csv=r"C:\IPEDS_Data\SFA\combined_ipeds_sfa_with_name.csv"
):
    """
    1) Downloads/unzips the latest HD file (e.g., HD2023.zip).
    2) Reads that HD file and the SFA CSV (already renamed).
    3) Renames the SFA "UNITID - Unique identification number of the institution" column back to "UNITID".
    4) Merges on 'UNITID' to get 'INSTNM'.
    5) Saves to output_csv with institution names included.
    """
    # Check we have the SFA data
    if not os.path.exists(sfa_renamed_csv):
        print(f"Could not find combined SFA CSV: {sfa_renamed_csv}")
        return
    
    # Step 1: Download HD
    hd_csv = download_latest_hd_file()
    if not hd_csv:
        print("No HD CSV found; cannot merge institution names.")
        return

    # Step 2: Read HD with 'latin1' or 'cp1252' to avoid UTF-8 decode issues
    try:
        hd_df = pd.read_csv(hd_csv, dtype=str, low_memory=False, encoding='latin1')
    except Exception as e:
        print(f"Error reading HD CSV ({hd_csv}): {e}")
        return
    
    # Step 3: Read your SFA CSV
    try:
        sfa_df = pd.read_csv(sfa_renamed_csv, dtype=str, low_memory=False)
    except Exception as e:
        print(f"Error reading SFA CSV ({sfa_renamed_csv}): {e}")
        return
    
    # If "UNITID" was renamed to "UNITID - Unique identification number of the institution",
    # rename it back so we can merge on 'UNITID' directly.
    old_unitid_col = "UNITID - Unique identification number of the institution"
    if old_unitid_col in sfa_df.columns:
        sfa_df.rename(columns={old_unitid_col: "UNITID"}, inplace=True)
        print(f"Renamed '{old_unitid_col}' back to 'UNITID' for merging.")
    else:
        print(f"Warning: '{old_unitid_col}' column not found. Merge may fail if there's no 'UNITID' at all.")
    
    # Ensure columns exist
    if 'UNITID' not in sfa_df.columns:
        print("SFA CSV missing 'UNITID'. Cannot merge with HD.")
        return
    if 'UNITID' not in hd_df.columns:
        print("HD CSV missing 'UNITID'. Can't merge.")
        return
    if 'INSTNM' not in hd_df.columns:
        print("HD CSV missing 'INSTNM'. Can't merge.")
        return

    # Subset HD to relevant columns
    hd_subset = hd_df[['UNITID','INSTNM']].drop_duplicates()

    # Merge on UNITID (left join)
    merged_df = pd.merge(sfa_df, hd_subset, on='UNITID', how='left')

    print(f"Merged SFA data ({sfa_df.shape[0]} rows) with HD data ({hd_subset.shape[0]} rows).")
    print(f"Result: {merged_df.shape[0]} rows, {merged_df.shape[1]} columns.")

    # Save final
    merged_df.to_csv(output_csv, index=False, encoding='utf-8')
    print(f"Final file with INSTNM: {output_csv}")

if __name__ == "__main__":
    merge_instnm()
