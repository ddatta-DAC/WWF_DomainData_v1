import pandas as pd
import textacy
import os
import inspect
import sys


def get_cur_path():
    this_file_path = '/'.join(
        os.path.abspath(
            inspect.stack()[0][1]
        ).split('/')[:-1]
    )

    os.chdir(this_file_path)
    print('>>', os.getcwd())
    return this_file_path

# ----
def get_hsCode_df():
    loc = './../../GeneratedData/HSCodes'
    f_name = 'HS_Code_6digit_data.csv'
    f_path = os.path.join(loc, f_name)
    df =  pd.read_csv(f_path)
    return df


def get_KwSource_df():
    loc = './../../GeneratedData/Keywords'
    keys = ['CITES','IUCN_RedList','WWF_HighRisk']
    data_types = ['sc_name','common_names']
    result_dict = {}

    for source in keys:
        result_dict[source] = {}

        for _type in data_types :
            _loc = os.path.join(loc)
            f_path =os.path.join(
                _loc,
                source + '_' +  _type + '.txt'
            )
            tmp_df = pd.read_csv(f_path,index_col=None,header=None)

            list_kws = set(list(tmp_df[0]))
            result_dict[source][_type] = list_kws
        # find the HS codes that match anything out here

    return result_dict


def processor():
    hsc_df = get_hsCode_df()
    source_kw_dict = get_KwSource_df()
    source_HS_Code_list = {}

    for k in source_kw_dict.keys():
        source_HS_Code_list[k] = []

    for i,row in hsc_df.iterrows():
        hs_code = row['hscode_6']
        row_scn = row['sc_names']
        row_kws = row['keywords']
        if type(row_scn) == str :
            _type = 'sc_name'
            row_scn = row_scn.split(';')
            row_scn = set(row_scn)
            # Try and see matches which source
            for source, data in source_kw_dict.items():
                _canditates = set(data[_type])
                flag = len(row_scn.intersection( _canditates ))>0
                if flag :
                    # Add HS code to flag type
                    source_HS_Code_list[source].append(hs_code)

        if type(row_kws) == str :
            _type = 'common_names'
            row_kws = row_kws.split(';')
            row_kws = set(row_kws)
            # Try and see matches which source
            for source, data in source_kw_dict.items():
                _canditates = set(data[_type])
                flag = len(row_kws.intersection( _canditates ))>0
                if flag :
                    # Add HS code to flag type
                    source_HS_Code_list[source].append(hs_code)

    op_file_loc = './../../GeneratedData/HSCodes'
    for source,_list in source_HS_Code_list.items():
        f_name = source + '_' + 'HS_Codes.txt'
        f_path = os.path.join(
            op_file_loc,
            f_name
        )
        _list = list(set(_list))
        series_hscodes = pd.Series(_list)
        series_hscodes.to_csv(
            f_path,
            header = False,
            index=False
        )
    print(source_HS_Code_list)



def main():

    old_path = os.getcwd()
    cur_path = get_cur_path()
    os.chdir(cur_path)
    processor()
    os.chdir(old_path)

main()