# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 09:10:51 2023

@author: fs.egb
"""

import pyreadr

def write_rData(HERE,input_file, name):
     print(f"starts wrting R file for {name} ")

     pyreadr.write_rdata(HERE/"data" / "interim"/f"{name}.RData",
                         input_file,
                         df_name = name, compress="gzip")
     print(f"{name} done")

    