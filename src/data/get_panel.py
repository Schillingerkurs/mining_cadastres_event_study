# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 16:20:39 2023

@author: fs.egb
"""


import pandas as pd
from pathlib import Path
from tqdm import tqdm
import numpy as np
# local modules
from fetch_iati_procurement import fetch_iati_procurement
from get_grd_wider_data import get_grd_wider_data
from get_v_dem import get_v_dem
from get_GNI_score import get_GNI_score


from get_mineral_and_fossil_exports import get_mineral_and_fossil_exports

def select_v_dem(HERE, 
                 df,
                 rel_vars = { "v2x_corr": "Pol_corruption_v_dem",
                            "v2x_execorr": "Exe_corruption_v_dem",
                            "country_text_id":"iso3",
                            "v2x_delibdem": "deliberative_democracy_ord"
                            }
    
                        ):
  
    v_vars =   ["year"]
    
    iso3_list = set(df.reset_index()['iso3'])
    time_series = set(df.reset_index()['year'])
    
    print(len(iso3_list) ,"countries and", len(time_series)," years in v_dem panel")
    
    v_vars.extend(rel_vars.keys()) 
    
    return (get_v_dem(HERE)
             .query("country_text_id in @ iso3_list")
             .query("year in @ time_series")
            [v_vars]
              .rename(columns  = rel_vars)
             .set_index( ["iso3", "year"])
             )
    
    




def main(HERE):
    grd_data = (get_grd_wider_data(HERE)
          .rename(columns = 
                  {'Caution2 Resource Revenues / taxes are significant but cannot be isolated from total revenues / taxes':"res_rev_not_isolated",
                   "iso":"iso3"})
          .dropna(subset = "iso3")
          )
    
    # make sure the panel is balanced 
     
    time_series = range(min(grd_data['year'].astype(int)),max(grd_data['year'].astype(int))+1)                
    panel_dims = set()
    for y in tqdm(time_series):
        panel_dims.update([(x, y) for x in  set(grd_data['iso3'])])
        
    minig_aid = (fetch_iati_procurement(iso3_list = set(grd_data['iso3']) , sector_code =  32210)
                  .assign(start_year = lambda x: x["day_start"].dt.year)
                  .assign(end_year = lambda x: x["day_end"].dt.year)
                #   .query("sector_percent  > 75 ")
                    .drop_duplicates(subset = "aid")
                    .groupby(["iso3", 'start_year'])['spend'].sum()
                    .reset_index()
                    .rename(columns = {'start_year':"year", 'commitment':"aid_commitments",
                                          "spend": "mining_aid_spend"})
                    .set_index(["iso3","year"])
                  )
        
    
    df = (pd.DataFrame(panel_dims, columns = ["iso3", "year"])
         
          .set_index(["iso3","year"])
          # this df contains the GRD data
          .merge(grd_data.set_index(["iso3","year"])
                [['Total Resource Revenue',"res_rev_not_isolated",'Resource Taxes'  ]],
                 left_index = True,
                 right_index = True,
                 how = "left",
                 validate = "1:1")
          
          .merge(minig_aid,
                 left_index = True,
                 right_index = True,
                 how = "left",
                 validate = "1:1")
         
          .merge(select_v_dem(HERE, grd_data),
                 left_index = True,
                 right_index = True,
                 how = "left",
                 validate = "1:1")
          
          # restrict panel to the first year when mining aid became a thing
          .reset_index()
          .query(f"year >= {min(minig_aid.reset_index()['year'])}")
          
           # drop countries that have no v-dem score
           .dropna(subset =  'deliberative_democracy_ord')
           .assign(year = lambda x: x['year'].astype(int))
           .set_index(["iso3","year"])
          )
    
    
    gni_scroe = get_GNI_score(df)


    commodity_exports = get_mineral_and_fossil_exports(df)
    
    
    
    out = (df
          .merge(gni_scroe,
                 left_index = True,
                 right_index = True,
                 how = "left",
                 validate = "1:1")
          .merge(commodity_exports,
                 left_index = True,
                 right_index = True,
                 how = "left",
                 validate = "1:1")
          .assign(res_rev_not_isolated = lambda x: x['res_rev_not_isolated'].fillna(''))
          .assign(res_rev_not_isolated = lambda x: x['res_rev_not_isolated'].replace(r'^\s*$', np.nan, regex=True))
          .assign(res_rev_not_isolated = lambda x: x['res_rev_not_isolated'].fillna(0))
          .assign(res_rev_not_isolated = lambda x: x['res_rev_not_isolated'].astype(int))
                  
                  
            
          )
    
    out.to_parquet(HERE/"data"/"interim"/"panel.parquet")
    print("\n","*"* 10 ," \n Panel done \n","*"* 10 )
    


if __name__ == "__main__":
    
    HERE = Path(__file__).parent.parent.parent.absolute()
    
    main(HERE)











