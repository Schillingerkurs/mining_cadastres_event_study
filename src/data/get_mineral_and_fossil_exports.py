# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 15:02:46 2023

@author: fs.egb
"""


import pandas as pd 
from tqdm import tqdm
from load_wb_data import load_wb_data

 



def get_mineral_and_fossil_exports(df):
    if 'year' not in df.keys():
        df = df.reset_index()
 
     
 
    iso3_list = set(df['iso3'])
    start = min(df['year'])
    end = max(df['year'] + 1)
    
    if not start < end :
        raise ValueError("The start year needs the beofore the end year")

    
    time_series =range(start,end) 

    frames = []
    for iso3 in tqdm(iso3_list):
        try:
            
            wb_data = load_wb_data(iso3, time_series, var_access = {"NY.GDP.MINR.RT.ZS":"Mineral_rents_of_GDP",
                                                                        "NY.GDP.PETR.RT.ZS":"Oil_rentss_of_GDP",
                                                                        "NY.GDP.NGAS.RT.ZS":"Natural_gas_rents_of_GDP"
                                                                        })
            
            frames.append(wb_data['NY.GDP.MINR.RT.ZS']
                    .merge(wb_data["NY.GDP.PETR.RT.ZS"], left_index = True, right_index = True, validate = "1:1")
                    .merge(wb_data["NY.GDP.NGAS.RT.ZS"], left_index = True, right_index = True, validate = "1:1")
                    )
        
        except ValueError:
            print(f"{iso3} is not in the WB datbase")
            continue
              
    
    return pd.concat(frames)


