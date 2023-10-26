# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 16:06:36 2023

@author: fs.egb
"""


import pandas as pd
from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt

from matplotlib import rc


# For latex fonts.
rc('font', **{'family': 'serif', 'serif': ['Computer Modern']})
rc('text', usetex=True)


HERE = Path(__file__).parent.parent.parent.absolute()






#["Africa", "Asia","South America"]



for continent in ["Africa"]:

    relevant_iso = set(gpd
              .read_file(gpd.datasets.get_path('naturalearth_lowres'))
              .query(f'continent == "{continent}"')
              ['iso_a3']
              )
    
    
    
    
    
    
    panel = (pd.read_parquet(HERE/"data"/"interim"/"panel.parquet")
             .query("iso3 in @ relevant_iso")
             .assign(resource_tax_share = lambda x: x['Resource Taxes'] *100)
             .assign(total_resource_revenue_share = lambda x: x['Total Resource Revenue'] *100)
                
               )
    
    
    relevant_lines = (panel
                      .groupby(["year"])[['resource_tax_share',
                                          'Mineral_rents_of_GDP',
                                     #     "total_resource_revenue_share",
                              'Oil_rentss_of_GDP'
                             ]].mean()
                      
                      .rename(columns ={
                              'resource_tax_share': "Resource taxes",
                              'Mineral_rents_of_GDP': 'Mineral rents',
                              'Oil_rentss_of_GDP':"Oil rents",
                             "total_resource_revenue_share":'Total Resource Revenue'
                              })
                      )
    
    
    
    first_year =  min(panel.reset_index()['year'])
    
    last_year =  max(panel.reset_index()['year'])
    
    n_countries = len(set(panel.reset_index()['iso3']))
    
    print(f"{continent}: \n {len(relevant_iso) - n_countries} countries are in\
          the Naturalearth data but not the UNU wider tax panel" )
    
    relevant_lines.plot( color =  ["grey","#006BA2","#DB444B"],
                        ylabel = "\% of GDP",
                        xlabel = "Year",
                        title = f"Income \% of GDP in {continent} between {first_year} \
                        and {last_year}(n={n_countries} ) "
                        )
        
        
         
    plt.savefig(Path(r"C:\Users\fs.egb\wdlpr\linechart")/'taxes_resource_exports_gdp.jpg', 
                bbox_inches='tight',
                dpi = 600)
           
        
