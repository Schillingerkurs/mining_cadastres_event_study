# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 14:59:19 2023

@author: fs.egb
"""


from pandas_datareader import wb


def load_wb_data(iso3, time_series, var_access):
    
     """ 
     Uses the datreader API to fetch world bank indicator
    
     for indicator list: 
     REF: https://data.worldbank.org/
    
#     """

     out = {}
     for g in list(var_access.keys()):
         out[g] = (wb
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
     
     return out