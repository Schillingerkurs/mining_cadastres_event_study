# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 22:41:14 2023

@author: fs.egb
"""

import pandas as pd
import pycountry


def get_iso3_mapper(input_: pd.Series):
    """ fuzzy match iso3 code with pycountry"""
    c_mapper = {}
    for c in set(input_):
        try:
            c_mapper[c] =  pycountry.countries.search_fuzzy(c)[0].alpha_3
        except LookupError:
            print(f" drop {c}")
            continue
              
    return c_mapper

