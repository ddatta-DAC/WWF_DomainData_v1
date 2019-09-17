'''
Based on :

FINAL_LookupTables_US_ITC_HTS_Codes_2015-2017.xlsx
https://docs.google.com/spreadsheets/d/1jAx6VSln3-sCxI7R1TDtAVSZjzVsH36MpAq3yKa4e7o/edit#gid=1901895138

'''
from joblib import Parallel, delayed
import pandas as pd
import re
import textacy
import spacy
nlp = spacy.load('en')
import math
from src.preprocess_3.hs_codes.hs_code_cleanup import gen_kw_2
from src.preprocess_3.hs_codes.hs_code_cleanup import filter_hs_codes

'''
Files
1501_hts_delimited.txt
hts_2016_basic_delimited.csv
hts_2017_preliminary_csv.csv
hts_2018_basic_csv.csv
'''

def get_file_list_1():
    files = {
        '1501_hts_delimited.txt': 'hs_codes_file_1.csv',
        'hts_2016_basic_delimited.csv': 'hs_codes_file_2.csv',
        'hts_2017_preliminary_csv.csv': 'hs_codes_file_3.csv',
        'hts_2018_basic_csv.csv': 'hs_codes_file_4.csv',
    }
    return files

def get_file_list_2():
    files = [
        'hs_codes_file_1.csv',
        'hs_codes_file_2.csv',
        'hs_codes_file_3.csv',
        'hs_codes_file_4.csv'
    ]
    return files

# ---------------------------- #


def process_chg_file(input_file, op_file):
    parts = input_file.split('.')
    file_ext = parts[1]

    if file_ext == 'txt':
        df = pd.read_csv(input_file, sep='|', encoding="ISO-8859-1")
    else:
        df = pd.read_csv(input_file, encoding="ISO-8859-1")

    def clean_description(row):
        if row is None or row['Description'] is None:
            return None
        text = str(row['Description'])
        if len(text) == 0:
            return None
        pattern = re.compile(r'<[^>]+>')
        text = pattern.sub('', text)
        strip_chars = ['\t', ' ', '\t\n', ':', ',', ';']
        for s in strip_chars:
            text = text.strip(s)
        text = text.replace(',', ' && ')
        text = text.replace(' and ', ' && ')
        return text

    op_path = op_file
    df['Description'] = df.apply(clean_description, 1)
    delete_cols = [
        'Unit of Quantity',
        'General Rate of Duty',
        'Special Rate of Duty',
        'Column 2 Rate of Duty',
        'Col. 1 Rate',
        'Special Rate',
        'Col. 2 Rate',
        'Footnote / Comment',
        'Unnamed: 2'
    ]

    for d in delete_cols:
        try:
            del df[d]
        except:
            pass
    # in case of 'HTS No.', 'Stat Suffix'  - combine them into 'hs_code'
    # also rename Level as Indent

    try:
        df = df.rename(columns={'Level': 'Indent'})
    except:
        pass

    def combine_hscode(row):
        h1 = row['HTS No.']
        h2 = row['Stat Suffix']
        if math.isnan(h2) == False and (int(h2)) > 0:
            res = h1 + '.' + str(int(h2))
        else:
            res = h1
        return res

    df_col_names = list(df.columns)

    if ('HTS No.' in df_col_names) and ('Stat Suffix' in df_col_names):
        df['hs_code'] = df.apply(combine_hscode, axis=1)
        del df['HTS No.']
        del df['Stat Suffix']
    else:
        try:
            df = df.rename(columns={'HTS Number': 'hs_code'})
        except:
            pass

    print(df.columns)
    df = filter_hs_codes.filter_df(df,level =1 )
    df.to_csv(op_path)
    print('-----------------')
    return

# ----------------------------------- #

# Clean and parse from txt/csv to csv. Place files in csv with new names

def main_1():
    files = get_file_list_1()
    for inp, op in files.items():
        process_chg_file(inp, op)

# ----------------------------------- #

def main_2(file):

    def gen_aux_desc(df, idx):
        min_idx = 0
        neg_keyword = 'Other'
        res = None
        cur_row = df.loc[idx]
        indent_attr = 'Indent'
        try:
            cur_indent = cur_row['Indent']
        except:
            cur_indent = cur_row['Level']
            indent_attr = 'Level'

        cur_desc = cur_row['Description']
        is_neg_keyword = False

        if type(cur_desc) != str:
            cur_desc = ''

        if cur_desc == neg_keyword:
            is_neg_keyword = True

        def format_desc(s):
            return '[ ' + s + ' ]'

        if cur_indent == 0:
            res = format_desc(cur_desc)
        else:
            # go up and fetch
            cur_idx = idx - 1
            if is_neg_keyword:
                res_parts = []
                while cur_idx >= min_idx:
                    r = df.loc[cur_idx]
                    # Find entry in same level
                    if r[indent_attr] == cur_indent:
                        # add in what the thing is not
                        d3 = r['Description']
                        if type(d3) == str and len(d3) > 0:
                            t = '!' + format_desc(d3)
                            res_parts.append(t)

                    elif r[indent_attr] == cur_indent - 1:
                        fd = r['full_desc']
                        res = ' && '.join(res_parts)
                        res = format_desc(res)
                        res = fd + ' && ' + res
                        res = format_desc(res)
                        break

                    cur_idx -= 1

            else:
                while cur_idx >= min_idx:
                    r = df.loc[cur_idx]
                    dsc = r['full_desc']
                    if r[indent_attr] == cur_indent - 1:
                        res = dsc + ' && ' + format_desc(cur_desc)
                        break
                    else:
                        cur_idx -= 1
        return res

    def generate_desc(df):
        df.at[:, 'full_desc'] = None
        new_df = df.copy(deep=True)
        for i, row in df.iterrows():
            _desc = gen_aux_desc(new_df, i)
            new_df.at[i, 'full_desc'] = _desc
        return new_df




    df = pd.read_csv(file,index_col=0)
    df = generate_desc(df)
    try:
        del df['Unnamed: 0']
    except:
        pass
    print('main_2', file)
    print('-----------------')
    df.to_csv(file)

# ----------------------------------- #

def add_in_kws(file):

    df = pd.read_csv(file)
    df = gen_kw_2.add_in_kws(df)
    df.to_csv(file)
    print('add_in_kws', file)
    print('-----------------')
    return


# ----------------------------------- #

# Genrate genus, sc name, and common names if possible
def main_3(file):
    print( 'in main 3')

    def get_df(file):
        # file = '../../../Data_1/HS_code_descriptions/' + file
        df = pd.read_csv(file, index_col=0)
        return df

    def write_df(df, file):
        # file = '../../../Data_1/HS_code_descriptions/' + file
        df.to_csv(file)
        return

    def add_in_extras(df):

        df['sc_name'] = None
        df['genus'] = None
        df['common_name'] = None

        for i, row in df.iterrows():
            desc = row['Description']
            if type(desc)!= str :
                continue
            desc = desc.split('&&')

            genus_list = []
            sc_nm_list = []
            cn_list = []
            for d in desc:
                d = textacy.preprocess.normalize_whitespace(d)
                doc = textacy.Doc(d, lang='en')
                pattern = r'(<ADJ>|<NOUN>|<PROPN>){1,2} <NOUN|PROPN>{1}'
                res = textacy.extract.pos_regex_matches(doc, pattern)
                for r in res:
                    r1 = str(r)
                    z = r1.split(' ')
                    if r[0].pos_ == 'PROPN' and len(z) == 2:
                        if z[0][0].isupper() and z[1][0].islower():
                            if z[1] == 'spp':
                                genus_list.append(z[0])
                            else:
                                genus_list.append(z[0])
                                sc_nm_list.append(r1)
                    if len(z) == 3:
                        r1 = r1.lower()
                        cn_list.append(r1)
            scn = ';'.join(sc_nm_list)
            cn = ';'.join(cn_list)
            genus = ';'.join(genus_list)
            df.loc[i, 'sc_name'] = scn
            df.loc[i, 'genus'] = genus
            df.loc[i, 'common_name'] = cn
        return df

    def gen_aux_terms(df, idx):
        min_idx = 0
        cur_row = df.loc[idx]
        indent_attr = 'Indent'

        try:
            cur_indent = cur_row['Indent']
        except:
            cur_indent = cur_row['Level']
            indent_attr = 'Level'
        if cur_row['genus'] is None or type(cur_row['genus']) != str :
            cur_gn = None
        else :
            cur_gn = cur_row['genus']

        if cur_row['sc_name'] is None or type(cur_row['sc_name']) != str :
            cur_sp = None
        else :
            cur_sp = cur_row['sc_name']

        if cur_row['common_name'] is None or type(cur_row['common_name']) != str:
            cur_cn = None
        else:
            cur_cn = cur_row['common_name']

        def is_valid(item):
            if item is None or type(item)!= str :
                return False
            return False

        cur_idx = idx -1
        while cur_idx >= min_idx:

            r = df.loc[cur_idx]
            # Find entry in same level

            if r[indent_attr] == cur_indent - 1:
                _sp = r['sc_name']
                _gn = r['genus']
                _cn = r['common_name']
                # check if they are null or not
                if is_valid(_sp) :
                    if is_valid(cur_sp):
                        cur_sp += _sp
                    else :
                        cur_sp = _sp

                if is_valid(_gn) :
                    if is_valid(cur_gn):
                        cur_gn += _gn
                    else :
                        cur_gn = _gn

                if is_valid(_cn) :
                    if is_valid(cur_cn):
                        cur_cn += _cn
                    else :
                        cur_cn = _sp

                break
            cur_idx -= 1

        # write them out
        df.loc[idx,'genus'] = cur_gn
        df.loc[idx,'sc_name'] = cur_sp
        df.loc[idx,'common_name'] = cur_cn
        return

    df = get_df(file)
    df = add_in_extras(df)
    # Now comnbine the spc and genus lists, using indent
    for i, row in df.iterrows():
        gen_aux_terms(df, i)
    write_df(df, file)
    return

# ----------------------------------- #

# filter by requisite hs_codes

def filter_by_hscodes(file):
    df = pd.read_csv(file,index_col=0)
    new_df = filter_hs_codes.filter_df(df ,level=2)
    new_df.to_csv(file)
    return


# ----------------------------------- #


def aux_main(file):
    main_2(file)
    add_in_kws(file)
    main_3(file)
    filter_by_hscodes(file)

def main():
    main_1()
    Parallel(n_jobs=4)(delayed(aux_main)(file) for file in get_file_list_2())
    return



main()
