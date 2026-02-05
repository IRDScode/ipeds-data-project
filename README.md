# IPEDS Data Project

Automated pipeline for collecting, processing, and harmonizing Student Financial Aid (SFA) data from the National Center for Education Statistics (NCES) IPEDS database.

## Overview

This project provides a robust workflow to transform raw IPEDS CSV files into a clean, longitudinal dataset with human-readable column names and institution names. It is designed to handle multiple years of data, reconciling variations in reporting formats over time.

## Data Pipeline

The project follows a linear four-step pipeline:

1.  **Download** ([download_ipeds_sfa.py](file:///c:/Users/michael.lane/OneDrive%20-%20Ball%20State%20University/Documents/GitHub/ipeds-data-project/scripts/download_ipeds_sfa.py)): Automatically fetches yearly SFA ZIP files from the NCES Data Center, checking file sizes to avoid redundant downloads.
2.  **Combine** ([combine_ipeds_sfa.py](file:///c:/Users/michael.lane/OneDrive%20-%20Ball%20State%20University/Documents/GitHub/ipeds-data-project/scripts/combine_ipeds_sfa.py)): Merges yearly files into a single master CSV, identifying and retaining columns common across all specific years.
3.  **Rename** ([rename_sfa_columns.py](file:///c:/Users/michael.lane/OneDrive%20-%20Ball%20State%20University/Documents/GitHub/ipeds-data-project/scripts/rename_sfa_columns.py)): Downloads the latest IPEDS dictionary and maps cryptic variable codes (e.g., `SCFA2`) to descriptive titles (e.g., `SCFA2 - Total amount of aid`).
4.  **Merge** ([merge_instnm.py](file:///c:/Users/michael.lane/OneDrive%20-%20Ball%20State%20University/Documents/GitHub/ipeds-data-project/scripts/merge_instnm.py)): Pulls the latest institutional header (HD) data and joins institution names (`INSTNM`) to the dataset using a robust `UNITID` detection logic.

## Prerequisites

- **Python 3.x**
- **Pandas**: `pip install pandas`
- **Requests**: `pip install requests`
- **Openpyxl** (for Excel dictionary parsing): `pip install openpyxl`

## Setup & Usage

1.  **Paths**: By default, the scripts expect a data directory at `C:\IPEDS_Data\SFA`. You may need to create this folder or update the `data_folder` variables in the scripts if using a different OS or location.
2.  **Execution**: Run the scripts in order:
    ```bash
    python scripts/download_ipeds_sfa.py
    python scripts/combine_ipeds_sfa.py
    python scripts/rename_sfa_columns.py
    python scripts/merge_instnm.py
    ```

## Project Structure

- `scripts/`: Contains the pipeline Python scripts.
- `LICENSE`: MIT License.
- `README.md`: Project documentation.
