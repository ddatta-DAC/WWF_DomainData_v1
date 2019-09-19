import pandas as pd
import re
import os
import sys
import textacy

def get_raw_data():
    file_loc = './../../Data_v2/CommerciallyTraded'
    file_name = 'Commercial_Timber.xlsx'
    f_path = os.path.join(file_loc,file_name)
    usecols = [
        'Type',
        'Family',
        'Scientific name '
    ]
    df = pd.read_excel(f_path, header=2, encoding="ISO-8859-1",usecols=usecols)
    return df

def process_scientific_names(input_str):
    input_str = input_str.strip()
    input_str = input_str.replace('  ',' ')
    input_str = input_str.replace('\t', ' ')


    # Handle multiple cases
    # Sadly these have to be hardcoded
    # No requirement to know subspecies
    # Case 1 :
    # e.g. 'var.' xx y2 var. y2
    # Case 3
    # e.g. Cordia dichotoma (also as Cordia suaveolens)

    kw1 = ' var. '
    kw2 = '(& misspelt as'
    kw3 = '(also as'
    kw4 = '(as'
    kw5 = 'subsp.'
    res = []

    # handle subspecies
    if kw5 in input_str:
        pattern = re.compile('subsp\.(\s)*([A-z]|[a-z])*(\s)*\(as')
        res_tmp = re.sub(pattern, ';', input_str)
        if len(res_tmp) == len(input_str):
            res_tmp = input_str.replace('subsp.',';')
        res_tmp = res_tmp.strip(')')

        parts = res_tmp.split(';')
        parts = [ _.strip(' ') for _ in parts ]
        genus = None
        sp = None
        res = []
        for part in parts:
            tmp = part.split(' ')
            if len(tmp) == 1 :
                if tmp[0][0].isupper():
                    genus = tmp[0]
                else:
                    sp = tmp[0]
            else:
                genus = tmp[0]
                sp = tmp[1]

            scn = genus + ' ' + sp
            res.append(scn)
        res = ';'.join(res)
        return res


    if kw1 in input_str:
        pattern_1 = re.compile('(\s)*var\.(\s)*')
        p1_res = re.sub(pattern_1, ';', input_str)
        parts = p1_res.split(';')
        genus = None
        sp = None
        res = []
        for part in parts:
            tmp = part.split(' ')
            if len(tmp) == 2:
                genus = tmp[0]
                sp = tmp[1]
            scn = genus + ' ' + sp
            res.append(scn)
        res = ';'.join(res)
        return res
    elif kw2 in input_str:

        rep_str = '(& misspelt as'
        input_str = input_str.replace(rep_str,';')
        input_str = input_str.replace(')','')
        input_str =  input_str.strip()
        parts = input_str.split(';')
        parts = [ _.strip(' ') for _ in parts ]
        res = []
        sp = None
        for part in parts:
            tmp = part.split(' ')
            if len(tmp) == 2:
                genus = tmp[0]
                sp = tmp[1]
            else:
                genus = tmp[0]
            scn = genus + ' ' + sp
            res.append(scn)
        res = ';'.join(res)
        return res
    elif kw3 in input_str:
        rep_str = '(also as'
        input_str = input_str.replace(rep_str,';')
        input_str = input_str.replace(')','')
        input_str =  input_str.strip()
        parts = input_str.split(';')
        parts = [ _.strip() for _ in parts ]
        res = ';'.join(parts)
        return res
    elif kw4 in input_str:
        rep_str = kw4
        input_str = input_str.replace(rep_str,';')
        input_str = input_str.replace(')','')
        input_str = input_str.strip()
        parts = input_str.split(';')
        parts = [ _.strip() for _ in parts ]

        res = ';'.join(parts)
        return res
    else:
        return input_str

    return None

def parse_df(df):
    main_df = df
    main_df.reset_index(inplace=True)
    main_df = main_df.rename(
        columns = {
            "Scientific name ": "sc_name",
            "Type" : "type",
            "Family" : "family"
            }
    )
    # Assumption that only 3 max variations of scientific names exist
    columns = [
        'sc_name',
        'genus',
        'species',
        'type',
        'family'
    ]
    new_df = pd.DataFrame(columns=columns)
    # multiple scientific names
    # split them and create new entries
    for i, row in main_df.iterrows():
        sc_name = row["sc_name"]
        sc_name = sc_name.replace(' x ',' ')
        sc_name = sc_name.replace('\xa0', " ")
        sc_name = sc_name.strip()
        sc_name = textacy.preprocess.normalize_whitespace(sc_name)

        list_sc_name = process_scientific_names(sc_name)
        list_sc_name = list_sc_name.split(';')
        for _scn in list_sc_name:
            tmp = _scn.split(' ')
            row_dict = {
                'sc_name': _scn,
                'genus': tmp[0],
                'species': tmp[1],
                'num_sc_names': 0,
                'family': row['family'],
                'type': row['type']
            }

            new_df = new_df.append(row_dict, ignore_index=True)

    return new_df

def main():
    df = get_raw_data()
    df = parse_df(df)
    op_file_loc = './../GeneratedData/CommerciallyTraded'

    if not os.path.exists(op_file_loc):
        os.mkdir(op_file_loc)

    op_file_name = 'CommerciallyTraded.csv'
    op_file_path = os.path.join(
        op_file_loc,op_file_name
    )
    df.to_csv(op_file_path,index=False)
    return

main()

