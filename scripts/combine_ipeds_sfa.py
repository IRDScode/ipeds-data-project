import os
import re
import pandas as pd

def find_sfa_csvs(folder):
    """
    In the given folder, looks for SFA CSV files in the form 'SFAxxxx.csv' or 'SFAxxxx_rv.csv'
    (or lowercase 'sfaxxxx.csv' / 'sfaxxxx_rv.csv').
    
    Returns a dict of the form:
    {
      "SFA1314": "C:/IPEDS_Data/SFA/sfa1314_rv.csv",  # if rv exists, otherwise sfa1314.csv
      "SFA1415": "C:/IPEDS_Data/SFA/sfa1415.csv",
      ...
    }
    
    Key = base name without '.csv' or '_rv.csv', e.g. 'SFA1314'
    Value = full file path
    """
    all_files = os.listdir(folder)
    
    # We'll store only the final chosen CSV for each base name (with or without _rv).
    chosen_files = {}
    
    for f in all_files:
        fname = f.lower()  # easier to compare
        # We'll look for something like sfa1314 or sfa1314_rv
        # ignoring if it's not a CSV
        if not fname.endswith(".csv"):
            continue
        
        # remove extension
        base_no_ext = fname.replace(".csv", "")
        
        # The simplest pattern is sfa1314 or sfa1314_rv
        # We'll figure out the "root" if there's an "_rv"
        # e.g. sfa1314_rv -> root is "sfa1314", suffix is "_rv"
        if "_rv" in base_no_ext:
            root = base_no_ext.replace("_rv", "")
            is_rv = True
        else:
            root = base_no_ext
            is_rv = False
        
        # We'll store them with an uppercase convention
        # e.g. SFA1314 as the root key
        root_key = root.upper()
        
        full_path = os.path.join(folder, f)
        
        # If we haven't seen this root yet, pick it
        if root_key not in chosen_files:
            chosen_files[root_key] = full_path
        else:
            # If we already picked a non-rv, but now we see rv, prefer rv
            # Or if we see the opposite, we ignore
            current_picked = chosen_files[root_key]
            if "_rv.csv" in fname:
                # prefer rv over non-rv
                chosen_files[root_key] = full_path
            # else keep existing
    return chosen_files


def get_year_from_filename(filename):
    """
    Extracts a year label from the SFA file name (e.g. SFA1314.csv => 2013, or 2013-14).
    You can adapt this logic based on your preference.
    
    If the file name is 'SFA1314', we parse '13' as start year => 2013, '14' => 2014.
    We'll return a string like '2013-14'.
    """
    # Expect something like .../SFA1314 or sfa1314_rv
    base_name = os.path.basename(filename).lower()
    base_name = base_name.replace(".csv", "").replace("_rv", "")
    # base_name might now be 'sfa1314', 'sfa2021', etc.
    
    # Quick regex to capture the two pairs of digits
    match = re.search(r'sfa(\d{2})(\d{2})', base_name)
    if match:
        start_yr_2dig = match.group(1)  # e.g. '13'
        end_yr_2dig = match.group(2)    # e.g. '14'
        
        # Convert to full year. We'll assume 2000+ 
        # If you have older data like SFA9899 => 1998-99, might need logic to handle that
        start_yr_full = 2000 + int(start_yr_2dig)
        end_yr_full = 2000 + int(end_yr_2dig)
        
        return f"{start_yr_full}-{end_yr_full}"
    else:
        return "UnknownYear"


def get_common_columns(file_paths):
    """
    Reads only the header row of each CSV in file_paths, finds the intersection of all columns.
    Returns that set (or list) of common column names.
    """
    common_cols = None
    
    for fp in file_paths:
        # read just the header row
        with open(fp, 'r', encoding='utf-8', errors='replace') as f:
            header_line = f.readline().strip()
        
        # split by comma (assuming no complex quoting issues or commas in headers)
        columns = header_line.split(",")
        # convert to set for easy intersection
        col_set = set([c.strip().lower() for c in columns])
        
        if common_cols is None:
            common_cols = col_set
        else:
            common_cols = common_cols.intersection(col_set)
            
        # If the intersection becomes empty, no sense continuing
        if not common_cols:
            break
    
    if not common_cols:
        print("No common columns found across these files!")
        return set()
    
    return common_cols


def combine_csvs(folder, output_csv="combined_ipeds_sfa.csv"):
    """
    1) Finds all SFA files in `folder` and picks the _rv version if available.
    2) Identifies columns common to ALL files.
    3) Concatenates those columns from each file into one DataFrame,
       adding a 'year' column from the filename.
    4) Writes combined DataFrame to `output_csv`.
    """
    chosen_files_dict = find_sfa_csvs(folder)
    if not chosen_files_dict:
        print("No SFA CSV files found in the folder.")
        return
    
    # We'll get a list of the final chosen CSV paths
    final_file_paths = list(chosen_files_dict.values())
    # 1) Find intersection of columns
    common_col_set = get_common_columns(final_file_paths)
    
    if not common_col_set:
        print("Cannot combine, no common columns.")
        return
    
    # We'll create a big list of DataFrames to concatenate
    df_list = []
    
    for base_key, fp in chosen_files_dict.items():
        # 2) Build a column list (in the original case they appear in the file, or just do sorted)
        # But we only want columns in the intersection
        # We'll re-read with pandas, specifying usecols
        # We want the final column names to be consistent, so let's convert to lowercase
        # so e.g. SFA file might have 'UNITID' or 'UnitID'.
        
        # We can read the first row with `header=0`, but we'll rename them to lowercase
        # We'll do a quick approach: read everything, rename columns to lowercase, keep intersection
        # If you want to be extra safe with quotes or special characters, consider the standard `csv` approach with `quotechar` etc.
        try:
            temp_df = pd.read_csv(fp, dtype=str, low_memory=False)
        except Exception as e:
            print(f"Error reading file {fp}: {e}")
            continue
        
        # rename columns to lowercase
        temp_df.columns = [col.lower().strip() for col in temp_df.columns]
        
        # Filter to only common columns
        keep_cols = [c for c in temp_df.columns if c in common_col_set]
        temp_df = temp_df[keep_cols]
        
        # Add a 'year' column
        year_label = get_year_from_filename(fp)
        temp_df['year'] = year_label
        
        # We can add a 'filename' col if needed for debugging
        # temp_df['source_file'] = os.path.basename(fp)
        
        df_list.append(temp_df)
    
    # 3) Concatenate everything
    if not df_list:
        print("No dataframes to combine. Possibly all read attempts failed.")
        return
    
    combined_df = pd.concat(df_list, ignore_index=True)
    
    # 4) Write out
    output_path = os.path.join(folder, output_csv)
    combined_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Combined dataset with {combined_df.shape[0]} rows and {combined_df.shape[1]} columns saved to {output_path}")


if __name__ == "__main__":
    data_folder = r"C:\IPEDS_Data\SFA"
    combine_csvs(data_folder, output_csv="combined_ipeds_sfa.csv")
