import pandas as pd
import numpy as np
import os
import sys

def get_data():
    data_inp_loc = './../../GeneratedData/Collated'
    data_inp_f_name = 'Complete_Domain_Data.csv'
    path =  os.path.join(
        data_inp_loc,
        data_inp_f_name
    )

    df = pd.read_csv(path, index_col=None)
    return df

def process_aux(df, output_file_name):
    output_loc = './../../GeneratedData/Keywords'

    if not os.path.exists(output_loc):
        os.mkdir(output_loc)

    exclude_words = [ 'spp.']
    output_file_path = os.path.join(
        output_loc,
        output_file_name
    )
    cols = list(df.columns)
    op_list = []

    for col in cols:
        col_series = df[col]
        for item in col_series:
            if type(item) == str:
                parts = item.split(';')
                parts = [ _ for _ in parts if len(_)>1 and _ not in exclude_words]
                op_list.extend(parts)

    # Do a bit of clean up
    op_list = set(op_list)
    op_list = [ _.strip() for _ in op_list]
    series = pd.Series(op_list)
    series.to_csv(output_file_path,index=False)
    return

def process():
    '''
    3 flags exists
    Create 3 output files
    '''

    target_columns = ['sc_name','species','common_names']
    df = get_data()
    flag_dict = {
        'IUCN_RedList': { 'column' : 'iucn_flag', 'op_file': 'IUCN_RedList_keywords.txt'},
        'CITES': {'column': 'cites_flag', 'op_file': 'CITES_keywords.txt'},
        'WWF_High_Risk': {'column': 'cites_flag', 'op_file': 'WWF_High_Risk_keywords.txt'}
    }

    for flag, attr in flag_dict.items():
        col = attr['column']
        f_name = attr['op_file']
        tmp_df =  df.loc[df[col]!=0]
        tmp_df = pd.DataFrame(tmp_df)
        tmp_df = tmp_df[target_columns]

        process_aux(tmp_df,f_name)
    return


process()