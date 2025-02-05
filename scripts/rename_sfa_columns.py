import os
import re
import requests
import zipfile
import datetime
import pandas as pd

##############################
#  A) Download the Latest Dictionary
##############################

def download_latest_sfa_dictionary(dict_folder=r"C:\IPEDS_Data\SFA\Dict"):
    """
    Checks for the most recent SFA dict file by trying HEAD requests from the current year backward.
    Example pattern: https://nces.ed.gov/ipeds/datacenter/data/SFA2223_Dict.zip
    If found, downloads & unzips the first match.
    
    Returns the path to the unzipped Excel (or CSV) dictionary file, or None if none found.
    """
    if not os.path.exists(dict_folder):
        os.makedirs(dict_folder)
    
    base_url = "https://nces.ed.gov/ipeds/datacenter/data/"
    # We'll check from 2013–14 up to the current year
    start_year = 13
    current_year_2digits = datetime.datetime.now().year % 100  # e.g. 25 for 2025
    
    for sy in reversed(range(start_year, current_year_2digits + 1)):
        ey = sy + 1
        dict_zip_name = f"SFA{sy:02}{ey:02}_Dict.zip"
        dict_url = base_url + dict_zip_name
        
        # HEAD request to see if it exists
        try:
            resp = requests.head(dict_url, allow_redirects=True, timeout=10)
            if resp.status_code == 200:
                # Found it, download & unzip if we haven't yet
                zip_path = os.path.join(dict_folder, dict_zip_name)
                if not os.path.exists(zip_path):
                    if download_file(dict_url, zip_path):
                        dict_file_path = unzip_and_find_dictionary(zip_path, dict_folder)
                        if dict_file_path:
                            return dict_file_path
                else:
                    # Already downloaded previously; just find the dictionary file
                    dict_file_path = unzip_and_find_dictionary(zip_path, dict_folder)
                    if dict_file_path:
                        return dict_file_path
        except Exception as e:
            print(f"HEAD request failed for {dict_url}: {e}")
    print("No SFA dictionary found in the checked range.")
    return None

def download_file(url, local_path):
    """ Utility to download a file via GET. Returns True if successful. """
    print(f"Downloading from {url} ...")
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded dict zip to {local_path}")
                return True
            else:
                print(f"Failed to download. Status code: {r.status_code}")
                return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def unzip_and_find_dictionary(zip_path, extract_folder):
    """
    Unzips the dictionary zip. Looks for .xlsx or .csv that might contain 'varlist'.
    Returns the full file path if found, or None.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_folder)
    except Exception as e:
        print(f"Error unzipping {zip_path}: {e}")
        return None
    
    # Return the first .xlsx or .csv we find.
    for root, dirs, files in os.walk(extract_folder):
        for f in files:
            f_lower = f.lower()
            if f_lower.endswith(".xlsx") or f_lower.endswith(".csv"):
                return os.path.join(root, f)
    return None

##############################
#  B) Build the "short + title" mapping
##############################

def load_sfa_dictionary(dict_file):
    """
    Reads the IPEDS SFA dictionary (Excel or CSV).
    Builds a map: short_name.lower() -> "SHORT_NAME - Full Title"
    """
    if dict_file.lower().endswith(".xlsx"):
        # Requires openpyxl: pip install openpyxl
        try:
            xls = pd.ExcelFile(dict_file, engine='openpyxl')
            sheet_name = 'varlist' if 'varlist' in xls.sheet_names else 0
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
        except Exception as e:
            print(f"Error reading Excel dictionary: {e}")
            return {}
    else:
        # CSV approach
        try:
            df = pd.read_csv(dict_file, dtype=str, low_memory=False)
        except Exception as e:
            print(f"Error reading CSV dictionary: {e}")
            return {}
    
    # Lowercase columns
    df.columns = [c.lower().strip() for c in df.columns]
    if 'varname' not in df.columns or 'vartitle' not in df.columns:
        print("Dictionary file missing 'varname' or 'varTitle'.")
        return {}
    
    var_dict = {}
    for _, row in df.iterrows():
        short = str(row['varname']).lower().strip()
        title = str(row['vartitle']).strip()
        if short and title:
            var_dict[short] = f"{short.upper()} - {title}"
    return var_dict

##############################
#  C) Rename Columns in Combined File
##############################

def rename_sfa_columns(
    combined_csv    = r"C:\IPEDS_Data\SFA\combined_ipeds_sfa.csv",
    renamed_csv_out = r"C:\IPEDS_Data\SFA\combined_ipeds_sfa_renamed.csv"
):
    """
    1) Downloads/unzips the latest SFA dictionary if possible.
    2) Loads the mapping {short -> "SHORT - Title"}.
    3) Reads combined_ipeds_sfa.csv, renames columns found in the dictionary.
    4) Saves renamed CSV to combined_ipeds_sfa_renamed.csv
    """
    if not os.path.exists(combined_csv):
        print(f"Combined CSV not found: {combined_csv}")
        return
    
    # 1) Download the dictionary
    dict_file = download_latest_sfa_dictionary()
    if dict_file is None:
        print("No dictionary available; skipping rename.")
        return
    
    # 2) Build the varname -> "SHORT - Title" map
    var_map = load_sfa_dictionary(dict_file)
    if not var_map:
        print("No valid mapping found in dictionary; columns remain short names.")
    
    # 3) Rename columns in the combined SFA CSV
    try:
        df = pd.read_csv(combined_csv, dtype=str, low_memory=False)
    except Exception as e:
        print(f"Error reading {combined_csv}: {e}")
        return
    
    rename_dict = {}
    for col in df.columns:
        lower_col = col.lower()
        if lower_col in var_map:
            rename_dict[col] = var_map[lower_col]

    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)
        print(f"Renamed {len(rename_dict)} columns using the dictionary.")
    else:
        print("No columns matched the dictionary. Nothing renamed.")

    # 4) Save final
    df.to_csv(renamed_csv_out, index=False, encoding='utf-8')
    print(f"Final renamed CSV saved to: {renamed_csv_out}")

##############################
#  Main Entrypoint
##############################

if __name__ == "__main__":
    rename_sfa_columns()
