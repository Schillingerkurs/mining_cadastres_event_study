# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 15:44:32 2023

@author: fs.egb
"""

from pathlib import Path
import pandas as pd
# import json
pd.options.mode.chained_assignment = None  # default='warn'

import pyreadr
import yaml
from yaml.loader import SafeLoader
import numpy as np
import sys



HERE = Path(__file__).parent.parent.parent.parent.absolute()

sys.path.insert(0, str(HERE/Path("src", "data")))
from mining_licenses import load_mining_license_with_fdi_mappings

import baci


import building_modules


with open(HERE/Path("data",'config.yml')) as f:
    CONFIG = yaml.load(f,Loader=SafeLoader)

    
def get_baci(HERE, relevant_iso):
    oil_exports = {}
    petro_or_gas = ["2710", "2711"]
    print(f"considers the hs codes: {petro_or_gas} in exports")
    
    for iso3_cntry in relevant_iso:
        print(iso3_cntry)

        nat_exports_per_y = (
                baci.get_total_trade(HERE,iso3_cntry)
               .query(f"exporter == '{iso3_cntry}'")
               .assign(hs_4 = lambda x: x['hs_codes'].apply(lambda y: y[0:4]))
               .query("hs_4 in @ petro_or_gas")
               .groupby("t")["v"].sum()
               .to_dict() 
               )
        
        oil_exports.update({iso3_cntry + str(k) : v for k,v in nat_exports_per_y.items()})
        
    return oil_exports
                 


def get_all_exports(HERE, relevant_iso):
    frames = []
    for iso in relevant_iso:
        frames.append(baci.get_total_trade(HERE, iso)
                    .query(F'exporter == "{iso}"')
                    .groupby(['hs_codes','product', "t"])['v'].sum()
                    .reset_index()
                    .assign(iso3 = iso)
                    )
        
    return pd.concat(frames)



mining_entities = load_mining_license_with_fdi_mappings(HERE)
   
relevant_iso  = set(mining_entities.iso3)





tax, labels = building_modules.time_series_fiscal_capacity(relevant_iso,HERE)
fdi = building_modules.get_all_fdi(tax['fiscal_cap'])



grd = (tax['grd_wider']
        .assign(m = lambda x: x['iso']+ x['year'].astype(int).astype(str))
        .set_index("m")
        [['tot_res_rev',"resourcetaxes"]]
        .dropna()
        )


df = (tax['fiscal_cap']
        .assign(m = lambda x: x['ISO']+ x['YEAR'].astype(int).astype(str))
        .query("YEAR >1989")
        .set_index("m")
        .assign(fdi =  pd.Series(fdi) )
        .merge(grd, left_index = True,right_index = True)
        # .assign(fdi_real_pc = lambda x: x['fdi']/(x['WAGES']* x['POPULATION'])
        .set_index(['ISO', 'YEAR'])

        .reset_index()
        )


exports = get_all_exports(HERE, relevant_iso)


iso3_exporets = (exports
                  .groupby(['iso3','t'])['v']
                  .sum().reset_index()
                  .rename(columns = {"v": "exports", 
                                    "t": "YEAR",
                                    'iso3': 'ISO'})
                  )

relevant_columns = ['resourcetaxes',
                        'exports', 
                        # 'DIRECT_NOMINAL', 
                      'fdi', 
                        'RESOURCES_NOMINAL'
                      ]

out = (df
      .merge(iso3_exporets, left_on = ['ISO', 'YEAR'],
              right_on = ['ISO', 'YEAR'], how = "left")
        .set_index(['ISO', 'YEAR'])
              [relevant_columns]
              .reset_index()
      )



out.to_parquet(HERE/"data"/"interim"/"macro_flows"/"exports_fdi_taxation.parquet")









