# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 15:17:56 2023

@author: fs.egb
"""


import geopandas as gpd
from tqdm import tqdm
import pandas as pd
from pandas_datareader import wb


def load_wb_figures(iso3, time_series, var_access):
    
     """ 
     Uses the datreader API to fetch world bank indicator
    
     for indicator list: 
     REF: https://data.worldbank.org/
    
#     """

     g = list(var_access.keys())[0]
     fdi_wb = (wb
                .download(iso3, g, start =     
                          min(time_series),
                          end = max(time_series))
                .reset_index()
              .rename(columns= var_access)
              .assign(iso3 = iso3)
              .drop(columns = ["country"])
              .assign(year = lambda x: x[ "year"].astype(int))
              .set_index( ["iso3", "year"])
              )
     
     return fdi_wb

def get_GNI_score(iso3_list, df):
    
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
    

    # fetch all iso3 coutnrie from the geopandas naturalearth_lowres dataset.
    all_countries = (gpd
         .read_file(gpd.datasets.get_path('naturalearth_lowres'))
         )
    
    time_series =range(start,end)   
    frames = []
    for iso3 in tqdm(iso3_list):
        try:
            frames.append(load_wb_figures(iso3, time_series, var_access = {"NY.GNP.PCAP.CD":"GNI_per_capita"}))
        except ValueError:
            print(f"{iso3} is not in the WB datbase")
              


    return pd.concat(frames)
