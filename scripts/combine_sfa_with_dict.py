deprecated

# import os
# import re
# import requests
# import zipfile
# import datetime
# import pandas as pd

# ##############################
# #  A) Download the Latest Dictionary
# ##############################

# def download_latest_sfa_dictionary(dict_folder=r"C:\IPEDS_Data\SFA\Dict"):
#     """
#     Checks for the most recent SFA dict file by trying HEAD requests from the current year backward.
#     Example pattern: https://nces.ed.gov/ipeds/datacenter/data/SFA2223_Dict.zip
#     If found, downloads & unzips the first match.
    
#     Returns the path to the unzipped Excel or CSV dictionary file, or None if none found.
#     """
#     if not os.path.exists(dict_folder):
#         os.makedirs(dict_folder)
    
#     base_url = "https://nces.ed.gov/ipeds/datacenter/data/"
#     # We'll check from this year down to 2013 (or adjust as needed)
#     start_year = 13
#     current_year_2digits = datetime.datetime.now().year % 100  # e.g. 25 for 2025
    
#     # We'll go in descending order so that we find the newest dictionary first
#     for sy in reversed(range(start_year, current_year_2digits + 1)):
#         ey = sy + 1
#         dict_zip_name = f"SFA{sy:02}{ey:02}_Dict.zip"
#         dict_url = base_url + dict_zip_name
        
#         # HEAD request to see if it exists
#         try:
#             resp = requests.head(dict_url, allow_redirects=True, timeout=10)
#             if resp.status_code == 200:
#                 # Found it, download & unzip
#                 zip_path = os.path.join(dict_folder, dict_zip_name)
#                 if download_file(dict_url, zip_path):
#                     # Unzip and return the path to the dictionary file
#                     dict_file_path = unzip_and_find_dictionary(zip_path, dict_folder)
#                     if dict_file_path:
#                         print(f"Using dictionary: {dict_file_path}")
#                         return dict_file_path
#         except Exception as e:
#             print(f"HEAD request failed for {dict_url}: {e}")
#     print("No dictionary found in the checked range.")
#     return None

# def download_file(url, local_path):
#     """ Utility to download a file via GET. Returns True if successful. """
#     print(f"Downloading dictionary from {url} ...")
#     try:
#         with requests.get(url, stream=True, timeout=30) as r:
#             if r.status_code == 200:
#                 with open(local_path, 'wb') as f:
#                     for chunk in r.iter_content(chunk_size=8192):
#                         f.write(chunk)
#                 print(f"Downloaded dict zip to {local_path}")
#                 return True
#             else:
#                 print(f"Failed to download. Status code: {r.status_code}")
#                 return False
#     except Exception as e:
#         print(f"Error downloading {url}: {e}")
#         return False

# def unzip_and_find_dictionary(zip_path, extract_folder):
#     """
#     Unzips the dictionary zip. Tries to locate an Excel or CSV that likely
#     contains the 'varlist' or codebook. Returns the full file path if found.
#     """
#     try:
#         with zipfile.ZipFile(zip_path, 'r') as zf:
#             zf.extractall(extract_folder)
#     except Exception as e:
#         print(f"Error unzipping {zip_path}: {e}")
#         return None
    
#     # After unzipping, we'll guess there's either an Excel or CSV with "varlist" or "dictionary"
#     # You can adapt this logic if the dictionary files are named differently.
#     for root, dirs, files in os.walk(extract_folder):
#         for f in files:
#             f_lower = f.lower()
#             if f_lower.endswith(".xlsx") or f_lower.endswith(".csv"):
#                 # Return the first match. (If there's more than one, adapt logic as needed.)
#                 return os.path.join(root, f)
#     return None

# ##############################
# #  B) Build the "Short + Title" Mapping
# ##############################

# def load_dictionary_to_mapping(dict_file):
#     """
#     Loads the IPEDS dictionary file (Excel or CSV) and returns a dict mapping:
#        varname.lower() -> "{varname.upper()} - {varTitle}"
#     for easy column renaming.

#     Assumes there's a sheet named 'varlist' if it's Excel.
#     Or if it's a CSV, we read it directly. Adjust logic as needed.
#     """
#     if dict_file.lower().endswith(".xlsx"):
#         # Load the 'varlist' sheet if it exists; fallback to the first sheet
#         try:
#             xls = pd.ExcelFile(dict_file)
#             sheet_name = 'varlist' if 'varlist' in xls.sheet_names else 0
#             df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
#         except Exception as e:
#             print(f"Error reading Excel dictionary: {e}")
#             return {}
#     else:
#         # Assume CSV
#         df = pd.read_csv(dict_file, dtype=str, low_memory=False)

#     # We'll look for columns named 'varname' and 'varTitle' or similar
#     # Convert everything to lowercase for matching
#     df.columns = [c.lower().strip() for c in df.columns]
    
#     if 'varname' not in df.columns or 'vartitle' not in df.columns:
#         print("Dictionary file is missing 'varname' or 'varTitle' columns.")
#         return {}
    
#     df['varname'] = df['varname'].str.lower().str.strip()
#     df['vartitle'] = df['vartitle'].str.strip()
    
#     mapping = {}
#     for _, row in df.iterrows():
#         short = row['varname']
#         title = row['vartitle']
#         if pd.isna(short) or pd.isna(title):
#             continue
        
#         # Create "SHORT - Title"
#         combined = f"{short.upper()} - {title}"
#         mapping[short] = combined
#     return mapping

# ##############################
# #  C) Combine & Rename
# ##############################

# def get_year_from_filename(filename):
#     """
#     Convert 'SFA1314.csv' or 'sfa1314_rv.csv' into something like '2013-14'.
#     """
#     base_name = os.path.basename(filename).lower()
#     base_name = base_name.replace(".csv", "").replace("_rv", "")
#     match = re.search(r'sfa(\d{2})(\d{2})', base_name)
#     if match:
#         start_yr_2dig = int(match.group(1))
#         end_yr_2dig = int(match.group(2))
#         # We'll assume 20xx
#         start_full = 2000 + start_yr_2dig
#         end_full = 2000 + end_yr_2dig
#         return f"{start_full}-{end_full}"
#     return "UnknownYear"

# def find_sfa_csvs(folder):
#     """
#     Same logic as before: pick the _rv file if it exists, otherwise the non-rv, etc.
#     Returns a dict: { "SFA1314": "C:/path/sfa1314_rv.csv", ... }
#     """
#     chosen = {}
#     for f in os.listdir(folder):
#         fn_lower = f.lower()
#         if not fn_lower.endswith(".csv"):
#             continue
#         base_no_ext = fn_lower.replace(".csv", "")
#         if "_rv" in base_no_ext:
#             root = base_no_ext.replace("_rv", "")
#             is_rv = True
#         else:
#             root = base_no_ext
#             is_rv = False
        
#         root_key = root.upper()
#         fullpath = os.path.join(folder, f)
        
#         if root_key not in chosen:
#             chosen[root_key] = fullpath
#         else:
#             # If we had a non-rv and we see rv, prefer rv
#             if is_rv:
#                 chosen[root_key] = fullpath
#     return chosen

# def get_common_columns(file_paths):
#     """
#     Reads only the header row of each CSV to find the intersection of columns.
#     """
#     common_cols = None
#     for fp in file_paths:
#         with open(fp, 'r', encoding='utf-8', errors='replace') as f:
#             header_line = f.readline().strip()
#         cols = [c.strip().lower() for c in header_line.split(",")]
#         col_set = set(cols)
#         if common_cols is None:
#             common_cols = col_set
#         else:
#             common_cols = common_cols.intersection(col_set)
#         if not common_cols:
#             break
#     if not common_cols:
#         print("No common columns found across these files!")
#         return set()
#     return common_cols

# def combine_sfa_files_with_dict(sfa_folder, dict_path, output_csv="combined_ipeds_sfa.csv"):
#     """
#     1) Identify CSVs in sfa_folder (picking _rv if present).
#     2) Find common columns across them.
#     3) Load each CSV with only those columns, add 'year'.
#     4) Rename columns using the dictionary map => "SHORT - Title".
#     5) Save final CSV.
#     """
#     # 1) Gather chosen CSVs
#     chosen_files_dict = find_sfa_csvs(sfa_folder)
#     file_paths = list(chosen_files_dict.values())
#     if not file_paths:
#         print("No SFA CSV files found to combine.")
#         return
    
#     # 2) Intersection of columns
#     common_cols = get_common_columns(file_paths)
#     if not common_cols:
#         return
    
#     # 3) Read & append
#     big_df_list = []
#     for fp in file_paths:
#         try:
#             df = pd.read_csv(fp, dtype=str, usecols=common_cols)
#         except Exception as e:
#             print(f"Error reading {fp}: {e}")
#             continue
#         year_label = get_year_from_filename(fp)
#         df['year'] = year_label
#         big_df_list.append(df)
#     if not big_df_list:
#         print("No dataframes successfully read.")
#         return
    
#     combined_df = pd.concat(big_df_list, ignore_index=True)
    
#     # 4) Build dictionary map => short name + title
#     var_map = load_dictionary_to_mapping(dict_path)
#     if var_map:
#         # Rename columns that appear in var_map
#         rename_dict = {}
#         for c in combined_df.columns:
#             c_lower = c.lower()
#             if c_lower in var_map:
#                 rename_dict[c] = var_map[c_lower]
#         combined_df.rename(columns=rename_dict, inplace=True)
#     else:
#         print("No dictionary mapping found; columns remain short names.")
    
#     # 5) Save
#     out_path = os.path.join(sfa_folder, output_csv)
#     combined_df.to_csv(out_path, index=False, encoding='utf-8')
#     print(f"Final combined file: {out_path} with shape {combined_df.shape}")

# ##############################
# #  Main Orchestrator
# ##############################

# if __name__ == "__main__":
#     # 1) Download the latest dictionary
#     dict_file_path = download_latest_sfa_dictionary(dict_folder=r"C:\IPEDS_Data\SFA\Dict")
    
#     if dict_file_path:
#         # 2) Combine SFA CSVs, rename with dictionary
#         combine_sfa_files_with_dict(
#             sfa_folder=r"C:\IPEDS_Data\SFA",
#             dict_path=dict_file_path,
#             output_csv="combined_ipeds_sfa2.csv"
#         )
#     else:
#         print("Skipping combine because no SFA dictionary was found.")
