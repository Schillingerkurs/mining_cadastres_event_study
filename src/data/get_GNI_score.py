# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 15:17:56 2023

@author: fs.egb
"""



from tqdm import tqdm
import pandas as pd


from load_wb_data import load_wb_data



def get_GNI_score(df):
    
    """
    #  Select wealth level of  a country based on its GNI per capita  in a given year.
    
    
    About the GNI per capita:
    REF: https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups#:~:text=For%20the%20current%202024%20fiscal,those%20with%20a%20GNI%20per
    

    """
    
    
    df = df.reset_index()
    
    iso3_list = set(df['iso3'])
    start = min(df['year'])
    end = max(df['year'] + 1)
    
    if start > 2023 or start < 2000: 
        raise ValueError("The GNI data is not aviable for the selected start date")
        
    if end > 2023 or end < 2000: 
        raise ValueError("The GNI data is not aviable for the selected end date")
    
    if not start < end :
        raise ValueError("The start year needs the beofore the end year")
    

    # # fetch all iso3 coutnrie from the geopandas naturalearth_lowres dataset.
    # all_countries = (gpd
    #      .read_file(gpd.datasets.get_path('naturalearth_lowres'))
    #      )
    
    time_series =range(start,end)   
    frames = []
    for iso3 in tqdm(iso3_list):
        try:
            
            wb_data = load_wb_data(iso3, time_series, var_access = {"NY.GNP.PCAP.CD":"GNI_per_capita"})
        
            frames.append(wb_data["NY.GNP.PCAP.CD"])
        except ValueError:
            print(f"{iso3} is not in the WB datbase")
              


    return pd.concat(frames)
