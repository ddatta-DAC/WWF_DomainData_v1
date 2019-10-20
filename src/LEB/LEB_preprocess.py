import os
import pandas as pd
import sys
import numpy as np

sys.path.append('./..')
sys.path.append('./../..')
sys.path.append('./../../..')

import pandas as pd
import numpy as np
import os
import sys
import inspect
from collections import defaultdict

class country_iso_code_fetcher:
    def __init__(self):
        self.SourceFile = '../../Data_v2/Auxillary/country_IsoCode.csv'
        self.df = pd.read_csv(self.SourceFile,index_col=0)
        self.iso_code_dict = defaultdict(lambda: None)
        for i,row in self.df.iterrows():
            self.iso_code_dict [row['country_name']] = row['iso_code']

        return

    def get_iso_code(self,c_name):
        return self.iso_code_dict[c_name]

ISO_CODE_OBJ = country_iso_code_fetcher()


def get_cur_path():
    this_file_path = '/'.join(
        os.path.abspath(
            inspect.stack()[0][1]
        ).split('/')[:-1]
    )

    os.chdir(this_file_path)
    print(os.getcwd())
    return this_file_path



# ------------------ #

def get_all_hs_codes():
    hscode_Source_File_name = 'HS_Code_6digit_data.csv'
    file_loc = './../../GeneratedData/HSCodes'
    hscode_Source_File_path = os.path.join(
        file_loc,
        hscode_Source_File_name
    )
    df_hsc = pd.read_csv(hscode_Source_File_path, usecols=['hscode_6'])
    all_hs_codes = list(df_hsc['hscode_6'])
    return all_hs_codes


'''
Function ingests the expert provided file
Parses + Cleans
Output: Dataframe 
    { CountryOfOrigin, True <List of HSCodes>, False <List of HSCodes> }
'''
def pre_process_part_1():
    INP_FILE = './../../Data_v2/LEB/LEB_v1.xlsx'
    df = pd.read_excel(INP_FILE)

    '''
    Rename so that resultant columns have names :
    CountryOfOrigin
    True_1
    True_2
    False_1
    '''

    rename_columns = {
        'Country of Origin': 'CountryOfOrigin',
        'Banned HS Code (Export) - product or species maps to specific China or US code': 'True_1',
        'Could be banned HS code': 'True_2',
        'Whitelist HS Code (known Plantation Species)': 'False_1'
    }

    df = df.rename(columns=rename_columns)
    df = df[list(rename_columns.values())]

    # function to clean up the excel file data
    def cleanup_1(row, col):
        if row[col] is None:
            return None

        s = str(row[col])
        s = s.split(';')
        res = [_.strip() for _ in s]
        res = ';'.join(res)
        return res

    df['True_1'] = df.apply(cleanup_1, axis=1, args=('True_1',))
    df['True_2'] = df.apply(cleanup_1, axis=1, args=('True_2',))
    df['False_1'] = df.apply(cleanup_1, axis=1, args=('False_1',))

    # combine True_1 and True_2
    def combine(row, col_1, col_2):
        v1 = row[col_1]
        v2 = row[col_2]

        if v1 is None or (type(v1) != str and np.isnan(v1)) or v1 == 'nan':
            v1 = []
        else:
            v1 = v1.split(';')
        if v2 is None or (type(v2) != str and np.isnan(v2)) or v2 == 'nan':
            v2 = []
        else:
            v2 = v2.split(';')
        res = v1 + v2
        res = ';'.join(res)
        return res

    df['True'] = df.apply(combine, axis=1, args=('True_1', 'True_2'))
    return df


def get_6digit_codes(
        list_hsc,
        all_hs_codes
):
    res = []
    for _hsc in list_hsc:
        if len(str(_hsc)) == 6:
            res.append(int(_hsc))
            continue
        _hsc = str(_hsc)
        cmp_len = len(_hsc)
        for h in all_hs_codes:
            _h = str(h)[:cmp_len]
            if _h == _hsc:
                res.append(h)
    res = list(set(res))
    return res

'''
Convert the country names to 3 letter ISO codes
'''
def pre_process_part_2(df=None):
    global ISO_CODE_OBJ
    all_hs_codes = get_all_hs_codes()
    hs_code_dict = {_: [] for _ in all_hs_codes}

    country_list = df['CountryOfOrigin']
    country2iso_dict = {}
    for c in country_list:
        c = c.strip()
        iso_c = ISO_CODE_OBJ.get_iso_code(c)
        print(c, iso_c)
        country2iso_dict[c] = iso_c

    for i, row in df.iterrows():
        print(row['CountryOfOrigin'])
        _true = row['True']
        _false = row['False_1']
        _true = [_ for _ in _true.split(';')]
        _false = [_ for _ in _false.split(';') if _ != 'nan']

        true_hs6 = get_6digit_codes(_true, all_hs_codes)
        whitelist_hs6 = get_6digit_codes(_false, all_hs_codes)
        for t in true_hs6:
            if t not in hs_code_dict.keys():
                hs_code_dict[t] = []
            if t not in whitelist_hs6:
                hs_code_dict[t].append(
                    country2iso_dict[row['CountryOfOrigin']]
                )

    hs_code_dict = {
        k: ';'.join(v) for k, v in hs_code_dict.items()
    }

    op_df = pd.DataFrame(
        hs_code_dict.items(),
        columns =['hscode_6', 'CountryOfOrigin']
    )
    return op_df


def main():
    cur_path = get_cur_path()
    old_path = os.get_cwd()
    os.chdir(cur_path)

    df = pre_process_part_1()
    op_df = pre_process_part_2(df)
    print('----')
    print(op_df.head(10))

    file_loc = '../../GeneratedData/LEB'
    if not os.path.exists(file_loc):
        os.mkdir(file_loc)

    file_name = 'LEB_hscode_country.csv'
    OP_FILE = os.path.join(file_loc, file_name)
    op_df.to_csv(OP_FILE, index=False)
    os.chdir(old_path)
    return


