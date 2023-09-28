# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 18:14:08 2023

@author: fs.egb
"""


from pathlib import Path
import pandas as pd
import os
import requests

def get_grd_wider_data(HERE):
    """ 
    load excel sheet from local repo. Download into repo if file is missing.
    
    """
 
    wider_path = HERE/Path("data","external","fiscal_capacity",
                           "grd_unu_wider","UNU WIDER GRD 2023.xlsx")

    if os.path.isfile(wider_path) == False:
        print("downloading GRD data")
       
      
        url = "https://www.wider.unu.edu/sites/default/files/Data/UNUWIDERGRD_2023_Central.xlsx"
    
    
            
        header = {
          "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"
        }

        r = requests.get(url,  headers = header )
    
        with open(wider_path, "wb") as f:
            f.write(r.content)
            print(f"saved in {wider_path}")
        
    #  PCTGDP : All data % of GDP
    return  pd.read_excel(wider_path, "PCTGDP")
    