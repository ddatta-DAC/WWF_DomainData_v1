import pandas as pd
import numpy as np
import os
import sys
import re
from iso3166 import countries_by_alpha2
from iso3166 import countries_by_alpha3

def get_raw_data():
    file_loc = './../../Data_v2/CITES'
    file_name = 'CITES_list.csv'

    usecols = [
        'Genus',
        'Species',
        'Listing',
        'All_DistributionISOCodes'
    ]

    df = pd.read_csv(
                os.path.join(file_loc,file_name),
                usecols = usecols,
                header=1
    )

    df = df.rename(columns={
        'Genus':'genus',
        'Species':'species',
        'Listing':'listing',
        'All_DistributionISOCodes':'region'
    })

    return df


def convert_ISO2_to_ISO3(input):
    HardCoded_keys = {
        'SU': 'RUS' # The great USSR has ISO code SU
    }
    try:
        list_countries = input.split(',')
    except:
        return None
    res = []
    for c in list_countries:
        try:
            obj_Country = countries_by_alpha2.get(c)
            res.append(obj_Country.alpha3)
        except:
            if c in HardCoded_keys.keys():
                res.append(HardCoded_keys[c])

    res = ';'.join(res)
    return res

'''
Clean up and parse CITES data
'''
def process_df ( cites_df ) :
    # Columns to be present in new CITES_list
    cites_df['region'] = cites_df['region'] .apply(
        convert_ISO2_to_ISO3
    )
    cites_df['genus'] = cites_df['genus'].apply(str.strip)
    cites_df['species'] = cites_df['genus'].apply(str.strip)
    return cites_df


def main():
    df = get_raw_data()
    cites_df = process_df(df)
    op_loc = './../../GeneratedData/CITES'
    op_file_name = 'CITES.csv'
    if not os.path.exists(op_loc):
        os.mkdir(op_loc)

    op_file_path= os.path.join(op_loc,op_file_name)
    cites_df.to_csv(op_file_path, index=False)

