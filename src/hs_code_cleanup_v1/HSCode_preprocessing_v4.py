#!/usr/bin/env python
# coding: utf-8

# In[296]:


# !/usr/bin/env python
import pandas as pd
import os
import textacy
import spacy
import glob
import re
import inspect
import collections
import numpy as np
import multiprocessing as mp

nlp = spacy.load('en')


def get_cur_path():
    this_file_path = '/'.join(
        os.path.abspath(
            inspect.stack()[0][1]
        ).split('/')[:-1]
    )

    os.chdir(this_file_path)
    print(os.getcwd())
    return this_file_path


# from IPython.display import display, HTML
# import gen_kw_2
# import filter_hs_codes
# from src.hs_code_cleanup_v1 import gen_kw_2
# from src.hs_code_cleanup_v1 import filter_hs_codes


def clean_description(row, conjunction=' && '):
    if row is None or row['Description'] is None:
        return None
    text = str(row['Description'])

    if len(text) == 0:
        return None
    strip_chars = ['\t', ' ', '\t\n', ':', ',', ';']
    for s in strip_chars:
        text = text.strip(s)

    text = text.replace(',', conjunction)
    text = text.replace(' and ', conjunction)
    return text


def get_raw_file_names():
    loc = './../../Data_v2/HS_code_descriptions/raw_data/.ipynb_checkpoints'
    fl1 = sorted(glob.glob(os.path.join(loc, '**.csv')))
    fl2 = sorted(glob.glob(os.path.join(loc, '**.txt')))
    fl1.extend(fl2)
    return fl1


def get_df_list():
    file_list = get_raw_file_names()
    delete_list = [
        'Unit of Quantity',
        'General Rate of Duty', 'Special Rate of Duty', 'Column 2 Rate of Duty',
        'Quota Quantity', 'Additional Duties', 'Unnamed: 2',
        'Unit of Quantity', 'Col. 1 Rate', 'Special Rate', 'Col. 2 Rate', 'Footnote / Comment'
    ]
    step1_df_list = []

    for fp in file_list:

        ext = fp.split('.')[-1]

        if ext == 'txt':
            delimiter = '|'
        else:
            delimiter = ','

        df = pd.read_csv(fp, delimiter=delimiter)
        for dc in delete_list:
            try:
                del df[dc]
            except:
                pass
        try:
            df = df.rename(
                columns={
                    'HTS No.':
                        'HTS Number',
                    'Level': 'Indent'
                }
            )
        except:
            pass

        print(df.columns)

        step1_df_list.append(df)
        print('-----')
    return step1_df_list


'''
Remove spurious 0s
'''


def remove_spurious_zeros(row):
    h = row['HTS Number']
    if type(h) != str or h == 'nan':
        return None
    h = h.strip(' ')
    parts1 = h.split('.')
    parts = []
    idx = 0
    for p in parts1:
        if idx == 0 and len(p) < 4:
            p = p.zfill(4)
        elif len(p) == 1:
            p = str(p) + '0'
        idx += 1
        parts.append(p)

    _catch = '00'
    _flag = True

    while _flag:
        _last = parts.pop(-1)
        if _last != _catch:
            _flag = False
            parts.append(_last)

    if parts[-1] == '0' or parts[-1] == '00':
        parts.pop(-1)

    return '.'.join(parts)


def handle_stat_suffix(df, col='Stat Suffix', hscol='HTS Number'):
    df[col] = df[col].astype(str)

    def special_join(row, col='Stat Suffix', hscol='HTS Number'):

        if row[hscol] is None or type(row[hscol]) != str or row[col] is None or row[col] == 'nan' or row[col] == '0.0':
            return row[hscol]

        c_val = row[col]

        strip_chars = ["'", '"', ",", "`", "'"]
        for _ in strip_chars:
            row[hscol] = row[hscol].strip(_)

        for _ in strip_chars:
            c_val = c_val.strip(_)
        return row[hscol] + '.' + c_val.split('.')[0]

    df[hscol] = df.apply(special_join, axis=1)
    del df[col]
    return df


def step_1():
    list_DF = get_df_list()
    conjunction_symbol = ' && '
    list_DF_1 = []
    for df in list_DF:
        df['HTS Number'] = df['HTS Number'].astype(str)
        df['HTS Number'] = df.apply(remove_spurious_zeros, axis=1)

        # Handle 1 special case
        col = 'Stat Suffix'
        if col in list(df.columns):
            df = handle_stat_suffix(df, col='Stat Suffix', hscol='HTS Number')

        list_DF_1.append(df)

    return list_DF_1


def extrapolate_hs_code(ref_df, idx):
    indent_col = 'Indent'
    hts_col_name = 'HTS Number'
    row_indent = ref_df.at[idx, indent_col]

    # Case 1 :
    # Where "indent" = 0
    if row_indent == 0:
        cur_idx = idx + 1
        max_idx = len(ref_df) - 1
        while cur_idx < max_idx:
            indent = ref_df.at[cur_idx, indent_col]
            cur_hsc = ref_df.at[cur_idx, hts_col_name]

            # check validity
            is_valid = False
            try:
                if re.search('\d', cur_hsc): is_valid = True
            except:
                pass

            if indent > row_indent and is_valid:
                _indent = int(indent)
                res = '.'.join((cur_hsc.split('.'))[:-_indent])

                return res
            cur_idx += 1
    else:
        # Case 1 :
        # Where "indent" >= 1
        min_idx = 0
        cur_idx = idx - 1

        while cur_idx >= min_idx:
            indent = ref_df.at[cur_idx, indent_col]
            if indent <= row_indent:
                res = ref_df.at[cur_idx, hts_col_name]
                return res
            cur_idx -= 1
    return None


def fill_in_missing_code(df):
    hscode_col = 'HTS Number'
    new_DF = df.copy(deep=True)

    for i, row in df.iterrows():
        h = row[hscode_col]
        if type(h) != str:
            res = extrapolate_hs_code(new_DF, i)
            new_DF.at[i, hscode_col] = res
        else:
            if len(str(row[hscode_col])) < 4:
                new_DF.at[i, hscode_col] = str(row[hscode_col]).zfill(4)
    return new_DF


def scale_HSCode_6_digits(_input):
    if _input is None: return None
    return '.'.join((_input.split('.'))[:2])


'''
Collate the code descriptions : only 6 digits needed
1. Collect 6 digit codes
2. Join them up using  " && "
'''


def get_6digitcode_list(
        df,
        col='hscode_6'
):
    def check6(h):
        return len(h.replace('.', '')) == 6

    list_HScodes = list(
        set(sorted(list(df[col])))
    )
    list_HScodes = [_item for _item in list_HScodes if check6(_item)]
    return sorted(list_HScodes)


def step_2_aux(df):
    df['Description'] = df.apply(
        clean_description,
        axis=1,
        args=(" && ",)
    )

    new_DF = fill_in_missing_code(df)
    new_DF_1 = complete_descriptions(new_DF)
    new_DF_2 = pd.DataFrame(new_DF_1, copy=True)
    return new_DF_2


def step_2(list_DF):
    res = []
    pool = mp.Pool(len(list_DF))
    results = [pool.apply_async(step_2_aux, args=(df,)) for df in list_DF]
    pool.close()
    pool.join()
    output = [p.get() for p in results]
    return output


def extrapolate_hs_code(ref_df, idx):
    indent_col = 'Indent'
    hts_col_name = 'HTS Number'
    row_indent = ref_df.at[idx, indent_col]

    # Case 1 :
    # Where "indent" = 0
    if row_indent == 0:
        cur_idx = idx + 1
        max_idx = len(ref_df) - 1
        while cur_idx < max_idx:
            indent = ref_df.at[cur_idx, indent_col]
            cur_hsc = ref_df.at[cur_idx, hts_col_name]

            # check validity
            is_valid = False
            try:
                if re.search('\d', cur_hsc): is_valid = True
            except:
                pass

            if indent > row_indent and is_valid:
                _indent = int(indent)
                res = '.'.join((cur_hsc.split('.'))[:-_indent])

                return res
            cur_idx += 1
    else:
        # Case 1 :
        # Where "indent" >= 1
        min_idx = 0
        cur_idx = idx - 1

        while cur_idx >= min_idx:
            indent = ref_df.at[cur_idx, indent_col]
            if indent <= row_indent:
                res = ref_df.at[cur_idx, hts_col_name]
                return res
            cur_idx -= 1
    return None


def fill_in_missing_code(df):
    hscode_col = 'HTS Number'
    new_DF = df.copy(deep=True)

    for i, row in df.iterrows():
        h = row[hscode_col]
        if type(h) != str:
            res = extrapolate_hs_code(new_DF, i)
            new_DF.at[i, hscode_col] = res
        else:
            if len(str(row[hscode_col])) < 4:
                new_DF.at[i, hscode_col] = str(row[hscode_col]).zfill(4)
    return new_DF


'''
Function to complete the HS Code descriptions
Uses auxillary function
Logic :
1. First complete the descriptions based on indent and hs code length
2. Aggregate to 6 level using more granular codes
'''


def aux_complete_desc(ref_df, idx):
    indent_col = 'Indent'
    desc_col_name = 'Description'
    hscode_col = 'HTS Number'

    cur_hsc = ref_df.at[idx, hscode_col]
    cur_hsc_4 = str(cur_hsc)[:4]

    row_indent = ref_df.at[idx, indent_col]
    cur_desc = ref_df.at[idx, desc_col_name]

    min_idx = 0
    cur_idx = idx - 1
    while cur_idx >= min_idx:
        indent = ref_df.at[cur_idx, indent_col]
        hsc = ref_df.at[cur_idx, hscode_col]

        # Check validity of using this row for using to fill description
        is_valid = True
        if indent >= row_indent:
            is_valid = False
        if cur_hsc_4 != hsc[:4]:
            is_valid = False

        cand = ref_df.at[cur_idx, desc_col_name]
        if cand is None or len(cand) == 0: is_valid = False
        if is_valid is True:
            res = cand + ' && ' + cur_desc
            return res

        cur_idx -= 1
        if indent == 0:
            break

    return cur_desc


def complete_descriptions(df):
    hscode_col = 'HTS Number'
    indent_col = 'Indent'
    desc_col_name = 'Description'

    # ------ Simple cleanup ----- #
    # Strip unwanted characters
    # --------------------------- #
    def custom_strip(input):
        input = input.strip(';')
        input = input.strip(':')
        input = input.strip('.')
        input = input.strip(' ')
        input = input.replace('\t', ' ')
        input = input.strip()
        return input

    df[desc_col_name] = df[desc_col_name].apply(custom_strip)
    res_df = pd.DataFrame(df, copy=True)

    # df is used for indexing only
    for i, row in df.iterrows():
        new_desc = aux_complete_desc(
            res_df,
            i
        )
        res_df.loc[i, desc_col_name] = new_desc

    return res_df


# # Work with assumption only 6 digits needed.
def scale_HSCode_6_digits(_input):
    if _input is None: return None
    return '.'.join((_input.split('.'))[:2])


'''
Collate the code descriptions : only 6 digits needed
1. Collect 6 digit codes
2. Join them up using  " && "
'''


def get_6digitcode_list(
        df,
        col='hscode_6'
):
    def check6(h):
        return len(h.replace('.', '')) == 6

    list_HScodes = list(set(sorted(list(df[col]))))
    list_HScodes = [_item for _item in list_HScodes if check6(_item)]
    return sorted(list_HScodes)


def collate_description_6digit(df, list_HSCode6, conjunction_symbol=' && '):
    hs_col = 'hscode_6'
    desc_col = 'Description'

    result_df = pd.DataFrame(columns=[hs_col, desc_col])
    for hs_item in list_HSCode6:
        tmp = list(df.loc[df[hs_col] == hs_item][desc_col])
        if len(tmp) == 0:
            continue
        res = conjunction_symbol.join(tmp)
        _dict = {
            hs_col: hs_item,
            desc_col: res
        }

        result_df = result_df.append(_dict, ignore_index=True)
    return result_df


'''
Keep descriptions of the HS codes needed, discard the rest
The target HS codes have been provided on a per country bases
Locate them in <project_root>/Data_v2/Target_HSCodes
This function should return the first 4 digits of targeted codes 
4 digits so that we can include more
'''


def get_target_hscodes2():
    # loc = './Target_HSCodes'
    loc = './../../Data_v2/Target_HSCodes'
    file_paths = sorted(glob.glob(os.path.join(loc, '*.txt')))
    list_target_HSCodes2 = set()

    for _fp in file_paths:
        tmp_df = pd.read_csv(_fp, header=None)
        tmp_df[0] = tmp_df[0].astype(str)
        tmp_list = [int(str(_)[:2]) for _ in list(tmp_df[0])]
        list_target_HSCodes2 = list_target_HSCodes2.union(tmp_list)
    return list_target_HSCodes2


def filter_out_target_HSCodes(df):
    df = pd.DataFrame(df, copy=True)
    '''
    Description
    Extract the target HS codes at max 4 digit resolution
    '''
    list_target_HSCodes_max2 = get_target_hscodes2()

    # Remove_dots
    def remove_dots(_input):
        return str(_input).replace('.', '')

    df['hscode_6'] = df['hscode_6'].apply(remove_dots)

    '''
    Extract the relevant HS Codes
    Check 2, 4 or 6 digit match
    '''

    def extract(
            row,
            target_list
    ):

        hsc = str(row['hscode_6'])
        candidate_lens = [2]
        for K in candidate_lens:
            # first K digits
            first_K = int(str(hsc)[:K])
            if first_K in target_list:
                return hsc
        return None

    df['hscode_6'] = df.apply(
        extract,
        axis=1,
        args=(list_target_HSCodes_max2,)
    )
    df = df.dropna(subset=['hscode_6'])

    return df


'''
## Code to parse out keywords
### 1. Unigrams, Bigrams 
### 2. Plant names
'''


def process_chunk(chunk, ngrams_lens=[1, 2, 3]):
    exclude_pos = [
        'SYM',
        'NUM',
        '$',
        'IN',
        'CONJ',
        'PUNCT',
        'CC',
        'PART',
        'INTJ',
        'JJR',
        'JJS'
    ]

    # Remove genus/species
    chunk = chunk.replace('spp.', '')
    chunk = textacy.preprocess.normalize_whitespace(chunk)
    pattern = re.compile(r'\(?<.*>*<.*>(\s)*\)?')
    chunk = pattern.sub('', chunk)

    # create textacy doc
    sent = textacy.preprocess.normalize_whitespace(chunk)
    doc = textacy.Doc(sent, lang='en')
    res = []

    for nl in ngrams_lens:
        ngrams_genobj = textacy.extract.ngrams(
            doc,
            n=nl,
            exclude_pos=exclude_pos
        )
        ngs = []
        for _ in ngrams_genobj:
            ngs.append(str(_))

        '''
        Process the ngrams
        Remove extra spaces
        Remove space with a hyphen
        '''
        for item in ngs:
            item = item.strip(' ')
            item = item.replace('-', ' ')
            item = textacy.preprocess.normalize_whitespace(item)
            res.append(item)
    res = ';'.join(list(set(res)))
    return res


def get_sc_names(text_chunk):
    if text_chunk is None or type(text_chunk) != str:
        return None
    res_list = []
    patterns = [
        '\<\S+\>[A-Z][a-z]+\<\S+\> \<\S+\>[a-z]+\<\S+\>',
        '\<\S+\>[A-Z][a-z]+ [a-z]+\<\S+\>',
        '\<\S+\>[A-Z][a-z]+\s*\<\S+\> [a-z]+\.',
        '\<\S+\>[A-Z][a-z]+\<\S+\> [a-z]+\.',
        '\<\S+\> [A-Z][a-z]+\<\S+\> [a-z]+\.',
        '\<\S+\> [A-Z][a-z]+\ <\S+\>  [a-z]+\.',
        '\<\S+\>[A-Z][a-z]+\ <\S+\>spp',
    ]

    for pattern in patterns:
        search_res = re.findall(pattern, text_chunk)
        if len(search_res) > 0:
            for grp in range(len(search_res)):
                _res = search_res[grp]
                _res = ''.join(_res)
                _pattern = re.compile(r'<[^>]+>')
                text = _pattern.sub('', _res)
                text = text.strip()
                res_list.append(text)
    res = None
    if len(res_list) > 0:
        res = ';'.join(set(res_list))
    return res


def extract_keywords(row, conjunction_symbol):
    desc_col = 'Description'
    desc = row[desc_col]
    res = []
    text_chunks = desc.split(conjunction_symbol)
    for chunk in text_chunks:
        chunk_res = process_chunk(chunk)
        if chunk_res is not None:
            res.append(chunk_res)
    res = [_.lower() for _ in res]
    res = ';'.join(res)
    res = [_ for _ in res.split(';') if len(_) > 0]
    res = ';'.join(set(sorted(res)))
    return res


def generate_keywords(ref_df, conjunction_symbol):
    df = pd.DataFrame(ref_df, copy=True)
    df['keywords'] = None
    df['keywords'] = df.apply(extract_keywords, axis=1, args=(conjunction_symbol,))
    return df


def extract_sc_names(row, conjunction_symbol):
    desc_col = 'Description'
    desc = row[desc_col]
    res = []
    text_chunks = desc.split(conjunction_symbol)
    for chunk in text_chunks:
        chunk_res = get_sc_names(chunk)
        if chunk_res is not None:
            res.append(chunk_res)

    res = ';'.join(sorted(res))
    res = ';'.join(set(res.split(';')))
    return res


def generate_sc_names(ref_df, conjunction_symbol):
    df = pd.DataFrame(ref_df, copy=True)
    df['sc_names'] = None
    df['sc_names'] = df.apply(extract_sc_names, axis=1, args=(conjunction_symbol,))
    return df


# In[ ]:


def main_aux():
    conjunction_symbol = " && "
    col = 'HTS Number'
    list_DF_1 = step_1()
    list_DF_2 = step_2(list_DF_1)

    list_DF_3 = []
    list_HSCode6 = []

    for df in list_DF_2:
        df['hscode_6'] = df[col].apply(scale_HSCode_6_digits)
        try:
            del df[col]
        except:
            pass
        list_DF_3.append(
            pd.DataFrame(df, copy=True)
        )
        list_HSCode6.extend(
            get_6digitcode_list(df)
        )

    list_HSCode6 = list(set(list_HSCode6))
    list_HSCode6.sort()
    pool = mp.Pool(len(list_DF_3))

    results = [pool.apply_async(
        collate_description_6digit,
        args=(df, list_HSCode6, conjunction_symbol,)
    ) for df in list_DF_3
    ]
    pool.close()
    pool.join()

    list_DF_4 = [r.get() for r in results]
    # ========================
    # Join the dataframes
    # ========================
    master_df = None
    for df in list_DF_4:
        if master_df is None:
            master_df = df
        else:
            master_df = master_df.append(df, ignore_index=True)

    master_df['text'] = master_df.groupby(['hscode_6'])['Description'].transform(lambda x: ' && '.join(x))
    DF_5 = master_df.reset_index()
    del DF_5['Description']
    DF_5 = DF_5.rename(columns={'text': 'Description'})
    DF_6 = filter_out_target_HSCodes(DF_5)
    DF_7 = generate_keywords(DF_6, conjunction_symbol)
    DF_8 = generate_sc_names(DF_7, conjunction_symbol)

    op_loc = './../../GeneratedData/HSCodes'
    if not os.path.exists(op_loc):
        os.mkdir(op_loc)

    op_file = 'HS_Code_6digit_data.csv'
    op_path = os.path.join(op_loc, op_file)
    DF_8.to_csv(op_path, index=False)


def main():
    old_path = os.getcwd()
    cur_path = get_cur_path()
    os.chdir(cur_path)
    main_aux()
    os.chdir(old_path)

    return

# main()