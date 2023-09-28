# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 07:58:58 2023

@author: fs.egb
"""


import pandas as pd
from pathlib import Path
#import pandas as pd
import numpy as np

# for the .loc index SettingWithCopyWarning
# REF: 
pd.options.mode.chained_assignment = None  # default='warn'



from write_rData import write_rData



def get_first_year_aid_treatment(panel):
    """
    get first year of treatment
    """
    
    
    only_treated = (panel
                    .reset_index()
                    .query("mining_aid_spend > 0")  
                    .groupby(["iso3"])['year'].min()
                    
                    )
    
    panel = (panel
             .reset_index()
             .assign(first_year_aid_treatment = lambda x : x["iso3"]
                          .map(only_treated))
             .set_index(["iso3",'year'])
             )
    
    return panel




def set_pol_corruption_quantiles(panel):
    
    """ 
    Group every country into quanitles per year based on the 
    political corruption score
    
    # Outperforms this solution: 
    #https://stackoverflow.com/questions/73552702/pandas-group-by-quantile-position 

    """
    frames = []
    
    for y in set(panel.reset_index()['year']):
        panel_y = panel.query(f"year == {y}")
        
        for number,i in enumerate([.25,.5,.75]):
            current_  = panel_y['Pol_corruption_v_dem'].quantile(i)
            panel_y.loc[panel_y['Pol_corruption_v_dem'] > current_, "pol_cor_quantile" ] = number +1
         
            panel_y.loc[np.isnan(panel_y["pol_cor_quantile"]), 'pol_cor_quantile'] = 0
            
            frames.append(panel_y)
        
    panel = pd.concat(frames).reset_index().drop_duplicates()
    
    panel['pol_cor_quantile'] = panel['pol_cor_quantile'].map(
        {0.0: "low",
         1.0: "middle_low",
         2.0: "middle_high",
         3.0: "high"
         })
    
    return panel
    

def set_resource_reporting_dummy(panel):
    """
    
    Label columns R confirm
    
    
    Set dummy to 1 if the country reports resource revenues in a given year
    according to the GRD dataset
    
    """
    panel = (panel.rename(columns = {'Total Resource Revenue':'tot_res_rev',
                                 'Resource Taxes':'res_tax',
                                 }) 
             )
    
    
    panel.loc[panel['tot_res_rev'].isna() == True, "tot_res_rev_dummy"] = 0    
    panel["tot_res_rev_dummy"] = panel["tot_res_rev_dummy"].fillna(1)
    
    return panel
    


def select_relevant_countries(panel):
    """
    Only countries with somewhat similiar corruption / state capacity levels 
    can be compared. Non-corrupt and or rich countries do not get a mining register.

    """
      
    gni_treshold = 5000
    
    base_year = 2022

    iso3_below5000_2022 =  set(panel
             .query(f"year == {base_year}")
             .query(f"GNI_per_capita < {gni_treshold}")
             .reset_index()
            ['iso3']
             )
  
    print( len(iso3_below5000_2022), "countries have a GNI below", gni_treshold, " in", base_year)
    
    
    return  panel.query("iso3 in @ iso3_below5000_2022")
     
                                




    
def main(HERE):  
    
    panel = (pd.read_parquet(HERE/"data"/"interim"/"panel.parquet")
             .pipe(get_first_year_aid_treatment)
             .pipe(select_relevant_countries)
             .pipe(set_pol_corruption_quantiles)
             .pipe(set_resource_reporting_dummy)
               
               )
              
             
    
    write_rData(Path(HERE),panel, 'country_panel_mining_aid')






if __name__ == "__main__":
    
    
    HERE = Path(__file__).parent.parent.parent.absolute()

    main(HERE)







