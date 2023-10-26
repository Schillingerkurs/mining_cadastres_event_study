# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 11:51:44 2023

@author: fs.egb
"""



import requests
import pandas as pd

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rc
rc('font', **{'family': 'serif', 'serif': ['Computer Modern']})
rc('text', usetex=True)


from scipy import stats


HERE = Path(__file__).parent.parent.parent.absolute()



def load_mineral_outlook(mineral:str)-> pd.DataFrame():
    """
    Retrieve data from an API URL that provides information about minerals, filter it by material (e.g., Copper),
    and load it into a Pandas DataFrame.

    This function sends an HTTP GET request to the specified API URL, parses the JSON response, and converts it
    into a DataFrame. It is useful for quickly obtaining and working with data related to specific minerals
    or materials.

    """
    
    # Define the API URL
    api_url = f"https://api.iea.org/minerals?material={mineral}"
    
    # Send an HTTP GET request to the API
    response = requests.get(api_url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON data from the response
        data = response.json()
    
        # Create a DataFrame from the JSON data
        df = pd.DataFrame(data)
        return df
    else:
        print("Failed to retrieve data from the API. Status code:", response.status_code)




def get_fossil_demand(HERE): 
    """
    
    Group global energy data from the International Energy Agency (IEA) 
    
    Unit: Exa joules ( 10*18 joules)
    
    In the International System of Units (SI), "exa" is a prefix that denotes a factor of 10^18. 
    A joule is the standard unit of energy in the SI system.

    """
     
    oil_path = HERE/ "data"/"external"/"iea"/"WEO2022_AnnexA_Free_Dataset_World.csv"
    
    relevant_prodcuts = ["Oil",'Natural gas']
    fossil_energy = (pd.read_csv(oil_path)
                # .query("SCENARIO == 'Announced Pledges Scenario'")
                # .query("FLOW == 'Total final consumption'")
                # .query("PRODUCT in @ relevant_prodcuts")
                
                # .query("CATEGORY == 'Energy'")
            
                # .pivot(index = "YEAR", columns = "PRODUCT", values = "VALUE")
                
                    )
    
    return fossil_energy
              

    



def get_mineral_demand(mineral_lst):
    
    """
    Check full list of current minerals:    
    REF: https://www.iea.org/data-and-statistics/data-tools/critical-minerals-data-explorer

    """
    frames = []

    for mineral in mineral_lst:
        frames.append(load_mineral_outlook(mineral)
                  .query("Case == 'Base Case'")
                 .query("Scenario == 'Announced Pledges Scenario'")
                 .groupby(["year","Material"])["VALUE"].sum()
                 .reset_index()
                 .pivot(index = "year", columns = "Material", values = "VALUE")
                 
                  )
    
    
    # Unit is kilo tonnes here
    return pd.concat(frames, axis=1)



def plot_dual_scale_line_charts(fossil, minerals, color_mapper):
    # Create two subplots sharing the same x-axis
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    


    # Plot fossil data on the left scale
    for column in fossil.keys():
        ax1.plot(fossil.index, fossil[column], 
                 label=column,
                 color =  color_mapper[column]
                 )
  

    # Plot minerals data on the right scale
    for column in minerals.keys():
        ax2.plot(minerals.index, minerals[column],
                 label=column, linestyle='--',
                 color='#006BA2')

    # Set labels and legends
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Fossil Deamand (Exa Joules)')
    ax2.set_ylabel('Mineral Demand (Kilo Tonnes)')

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    # Save the plot
    plt.savefig(Path(r"C:\Users\fs.egb\wdlpr\linechart")/'global_demand.jpg', 
              bbox_inches='tight',
              dpi = 600)
      




mineral_lst = ["Cobalt", "Copper","Tantalum","Tin","Tungsten"]


# Modify the join statement to include a line break after every two words
joined_str = ',\n'.join([', '.join(mineral_lst[i:i+2]) for i in range(0, len(mineral_lst), 2)])

minerals = (get_mineral_demand(mineral_lst)
            .assign(demand_kt = lambda x: x.sum(axis=1))
            .drop(columns = mineral_lst)
            .rename(columns = {'demand_kt': joined_str})
            )


fossil =  get_fossil_demand(HERE)


color_mapper = {
    'Cobalt, Copper, Tantalum, Tin, Tungsten':"#006BA2", 
     'Natural gas':"grey",
      'Oil':"#DB444B"                          
    }




plot_dual_scale_line_charts(fossil, minerals, color_mapper)


