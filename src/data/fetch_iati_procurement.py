# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 10:44:24 2023

@author: fs.egb
"""


# from pathlib import Path
import pandas as pd
import pycountry
from tqdm import tqdm
import json    


from get_iso3_mapper import get_iso3_mapper

def fetch_iati_procurement(iso3_list, sector_code =  32210 ):
   """
   
   Fetch all iati projects in countrylist for a specific sector code
   
   REF: http://d-portal.org/ctrack.html#view=search
   
   """ 

   iso2_list = []
   for x in iso3_list:
       try:
          iso2_list.append( pycountry.countries.get(alpha_3= x).alpha_2)
       except AttributeError:
           print(f"{x} is not in the pycountry dataset")
            
       
   
    
   iso2_codes = "%2C".join(iso2_list)
   
   print(f"Fetch sector code {sector_code} for {len(iso2_list)} countries")



   
   url = f"http://d-portal.org/q?select=*&from=act%2Clocation%2Csector%2Ccountry&form=csv&limit=-1&human=1&country_code={iso2_codes}&sector_code={sector_code}&view=map&country_percent=100"

   df = pd.read_csv(url)


   out =  (df
             .assign(day_start = lambda x: pd.to_datetime(x['day_start']))
          .assign(day_end = lambda x: pd.to_datetime(x['day_end']))
          .assign(reporting = lambda x: x['reporting'].astype("category"))
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Congo \(the Democratic Republic of the\)',
                   'Congo, The Democratic Republic of the',regex=True))
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Central African Republic \(the\)',
                   'Central African Republic',regex=True))
          
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Niger \(the\)',
                   'Niger' ,regex=True))
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Tanzania, the United Republic of',
                   'Tanzania',regex=True))
          
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Türkiye',
                   'Turkey',regex=True))
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Dominican Republic \(the\)',
                   'Dominican Republic',regex=True))
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   'Russian Federation \(the\)',
                   'Russia',regex=True))
          
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   "Lao People's Democratic Republic \(the\)",
                   'Lao',regex=True))
          
          .assign(country_code = lambda x: x['country_code'].str.replace(
                   "Bolivia \(Plurinational State of\)",
                   'Bolivia',regex=True))
          
           .assign(iso3 = lambda x: x['country_code']
                  .map(get_iso3_mapper(x['country_code'])))
           
           .assign( iati_code = lambda x: x['aid'].apply(lambda y:
                   y.split("=")[1]))
                 
              )
           
   
   return out






       
      
       
       
       
       




     
     
     
  
  
 
