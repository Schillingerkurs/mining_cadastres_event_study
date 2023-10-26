# -*- coding: utf-8 -*-
"""
Created on Sat Sep 30 11:01:00 2023

@author: fs.egb
"""


import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt



from chart_studio.plotly import plotly as py



import plotly.graph_objects as go


from plotly.subplots import make_subplots

import numpy as np


from plotly.subplots import make_subplots



from matplotlib import rc


# For latex fonts.
rc('font', **{'family': 'serif', 'serif': ['Computer Modern']})
rc('text', usetex=True)





def mpl_to_interactive_html(fig, filename):
    # Convert Matplotlib figure to Plotly figure
    plotly_figure = make_subplots(rows=1, cols=1)
    for ax in fig.get_axes():
        x = ax.get_lines()[0].get_xdata()
        y = ax.get_lines()[0].get_ydata()
        plotly_trace = go.Scatter(x=x, y=y, mode='lines', name=ax.get_title())
        plotly_figure.add_trace(plotly_trace)

    # Set layout properties
    plotly_figure.update_layout(
       # title=fig.suptitle.get_text(),
        xaxis_title=ax.get_xlabel(),
        yaxis_title=ax.get_ylabel(),
    )

    # Create and save the interactive HTML plot
    plotly_figure.write_html(filename)

    print("done")




def load_coef_tables(HERE):
    frames = []
    
            
    color_dict = {'high' :   "#006BA2",
                  'low':  "#DB444B"   
                  }

    for path in ["low","high"]:
            
        frames.append(pd
                .read_csv(HERE/"data"/"interim"/f"coeftable_{path}_corruption.csv")
                .assign(year =  lambda x : x['Unnamed: 0'].str.replace("year::", ""))
                .query("year != 'mining_aid_spend'")
                .assign(year =  lambda x : x['year'].astype(int))
                .set_index('year')
                .drop(columns = ['Unnamed: 0'])
                .rename(columns = {
                    'Pr...t..': "pr_t",
                    'Std..Error':"std_error",
                    't.value' : "t_value"})    
                .assign(type_ = path)
                )
                    
    return pd.concat(frames)
        

def main():
    
    HERE = Path(__file__).parent.parent.parent.absolute()
    
    
    fig, ax = plt.subplots()
     
    
    df = load_coef_tables(HERE)
    
    
        
        
        
        
        
        
    
    #     ax.errorbar(temp.index, temp.Estimate, yerr= temp.std_error, color = color_dict[path])
        
    # ax.axvline(x=0,color = "black")
    
    # ax.axhline(y=0, color = "black")
    
    
    # plt.title('Eventstudy')
    
    # plt.show()
    
    
    # mpl_to_interactive_html(fig, filename = HERE / "reports"/"event_plot.html")
    
    





# mpl_to_interactive_html(fig, 'interactive_plot.html')

# = HERE /"reports" /'eventstudy_plot.html'




if __name__ == "__main__":

    
    main()


