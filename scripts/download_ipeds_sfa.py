import requests
import os

def download_ipeds_sfa():
    BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data/"
    DOWNLOAD_FOLDER = "C:/IPEDS_Data/SFA/"
    
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    for start_year in range(13, 23):
        end_year = start_year + 1
        filename = f"SFA{start_year:02}{end_year:02}.zip"
        file_url = BASE_URL + filename
        local_path = os.path.join(DOWNLOAD_FOLDER, filename)

        print(f"Attempting to download {filename}...")
        resp = requests.get(file_url, stream=True)
        if resp.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {filename}")
        else:
            print(f"Failed to download {filename}. Status: {resp.status_code}")

if __name__ == "__main__":
    download_ipeds_sfa()
