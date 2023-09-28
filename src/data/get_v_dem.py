# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 20:12:05 2023

@author: fs.egb
"""

import requests
import zipfile
import os 

from pathlib import Path
import  pandas as pd

def get_v_dem(HERE):
    """
    Download the zipfolder from V-dem and store it locally if not already storred

    """
    v_dem_path = Path(HERE/"data"/"external"/"v_dem"/"V-Dem-CY-Core-v13.csv")
    if os.path.isfile(v_dem_path) == False:
        print("downloading V-dem")
        
        # URL of the file to download
        url =   "https://v-dem.net/media/datasets/V-Dem-CY-Core_csv_v13.zip"
        
        # Define the local file name for the downloaded zip file and the destination folder for the unzipped content
        local_zip_file = "V-Dem-CY-Core_csv_v13.zip"
        unzip_folder =  v_dem_path.parent
        
        # Step 1: Download the file
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error downloading the file: {e}")
            exit(1)
        
        # Step 2: Save the downloaded file locally
        with open(local_zip_file, "wb") as file:
            file.write(response.content)
        
        # Step 3: Unzip the file
        try:
            with zipfile.ZipFile(local_zip_file, "r") as zip_ref:
                zip_ref.extractall(unzip_folder)
        except zipfile.BadZipFile as e:
            print(f"Error unzipping the file: {e}")
            exit(1)
        
        print(f"File downloaded and unzipped successfully to {unzip_folder}")
        
        
        os.remove(HERE/"src"/"data"/"V-Dem-CY-Core_csv_v13.zip")
        
        


    return pd.read_csv(v_dem_path)
        
            
            