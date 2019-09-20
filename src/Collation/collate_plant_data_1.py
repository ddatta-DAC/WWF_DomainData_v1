import pandas as pd
import numpy as np
import os
import sys

'''
# Remove_families = 
'Leguminosae','Orchidaceae','Cactaceae','Cyatheaceae',
'Euphorbiaceae','Primulaceae','Thymelaeaceae'

Also : all plants ofa genus belongs to same family

CITES :
listing in ['I','II','III']

IUCN :
0 : 'LR/nt', 'LC', 'LR/lc'
1 : Rest

'''
def get_data_sources():
    sources = ['CITES', 'ForestProductKeywords','CommerciallyTraded','IUCN_RedList','WWF_HighRisk']
    file_loc = './../../GeneratedData'
    for source in sources :
        f_path = os.path.join(
            file_loc,
            source,
            source + '.csv'
        )
        df = pd.read_csv(f_path,index_col=None)
        print('source', source , ' | ', df.columns)
    return

def process():
    return

def main():
    return

get_data_sources()