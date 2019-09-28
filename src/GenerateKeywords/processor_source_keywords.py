import pandas as pd
import numpy as np
import os
import sys
import inspect

def get_cur_path():
    this_file_path = '/'.join(
        os.path.abspath(
            inspect.stack()[0][1]
        ).split('/')[:-1]
    )

    os.chdir(this_file_path)
    print(os.getcwd())
    return this_file_path

def get_data():
    data_inp_loc = './../../GeneratedData/Collated'
    data_inp_f_name = 'Complete_Domain_Data.csv'
    path =  os.path.join(
        data_inp_loc,
        data_inp_f_name
    )

    df = pd.read_csv(path, index_col=None)
    return df

def process_aux(df, output_file_signature):

    target_columns = ['sc_name', 'common_names']
    df = df[target_columns]
    exclude_words = ['spp.']

    op_f_name = {
        'sc_name': output_file_signature + '_sc_name.txt',
        'common_names' : output_file_signature + '_common_names.txt',
    }

    output_loc = './../../GeneratedData/Keywords'

    if not os.path.exists(output_loc):
        os.mkdir(output_loc)

    for t_col in target_columns:
        output_file_path = os.path.join(
            output_loc,
            op_f_name[t_col]
        )
        op_list = []
        col_series = df[t_col]

        for item in col_series:
            if type(item) != str: continue
            parts = item.split(';')
            # validate
            tmp = []

            for p in parts:

                if len(p) < 2 : continue
                if ' ' not in p and p not in exclude_words:
                    continue
                tmp.append(p)
            op_list.extend(parts)

        # Do a bit of clean up
        op_list = set(op_list)
        op_list = [ _.strip() for _ in op_list ]
        series = pd.Series(op_list)
        series.to_csv(output_file_path,index=False)
    return

def process():
    '''
    3 flags exists
    Create 3 output files
    '''

    df = get_data()
    flag_dict = {
        'IUCN_RedList': {'column' : 'iucn_flag', 'op_file': 'IUCN_RedList'},
        'CITES': {'column': 'cites_flag', 'op_file': 'CITES'},
        'WWF_High_Risk': {'column': 'wwf_high_risk_flag', 'op_file': 'WWF_HighRisk'}
    }

    for flag, attr in flag_dict.items():
        col = attr['column']
        f_name = attr['op_file']
        tmp_df =  df.loc[df[col]!=0]
        tmp_df = pd.DataFrame(tmp_df,copy=True)
        process_aux(tmp_df,f_name)
    return

def main():
    old_path = os.getcwd()
    cur_path = get_cur_path()
    os.chdir(cur_path)
    process()
    os.chdir(old_path)

main()