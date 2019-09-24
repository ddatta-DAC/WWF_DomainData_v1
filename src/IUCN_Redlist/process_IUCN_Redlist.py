import pandas as pd
import os
import sys
import re
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


def get_raw_data():
    file_loc = './../../Data_v2/IUCN_RedList'
    file_name = 'RedList_CommonNames_and_status.csv'
    file_path = os.path.join(file_loc,file_name)
    usecols = [
        'genus',
        'species',
        'Common Name',
        'main_common_name',
        'Status code'
    ]

    df = pd.read_csv(
        file_path,
        usecols=usecols,
        index_col=None
    )
    return df

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def parse(df):

    def clean_common_names(_input):
        if _input is None or type(_input)!= str:
            return None
        # Remove stuff between ( and )
        p1 = re.compile('\(.*\)')
        _input = re.sub(p1,'', _input)
        # Remove patterns like 'Species code: xx'
        p2 = re.compile('Species (c|C)ode(\s)*:*(\s)*([a-z]|[A-Z]){2}')
        _input = re.sub(p2, '', _input)

        _input = _input.strip(' ')
        _input = _input.strip(',')
        parts_input = _input.split(',')
        res = []
        for part in parts_input:
            part = part.lower()
            part = part.strip('"')
            part = part.strip(' ')
            part = part.strip('.')
            if is_ascii(part):
                res.append(part)

        return ';'.join(res)

    def to_lower(_input):
        if type(_input)!=str:
            return None
        return _input.lower()

    def join_all_common_names(row,cols):
        res = []
        for col in cols :
            if type(row[col]) != str:
                continue
            r = row[col].split(';')
            res.extend(r)
        res = ';'.join(res)
        return res

    df['Common Name'] = df['Common Name'].apply(clean_common_names)
    df['main_common_name'] = df['main_common_name'].apply(to_lower)
    # ----
    # Collate the common names into a single column
    # ----
    df['common_names'] = df.apply(
        join_all_common_names,
        axis=1,
        args= ( ['Common Name','main_common_name'],)
    )
    try:
        del df['main_common_name']
        del df['Common Name']
    except:
        pass
    df = df.rename(columns={'Status code':'iucn_status_code'})

    # some species names have multiple parts, stretch them out into multiple rows
    new_df = pd.DataFrame(columns= list(df.columns))
    for i,row  in df.iterrows():
        gn = row['genus']
        sp = row['species']
        if ';' not in sp:
            _dict = {
                'genus': row['genus'],
                'species': row['species'],
                'iucn_status_code': row['iucn_status_code'],
                'common_names': row['common_names']
            }
            new_df = new_df.append(_dict, ignore_index=True)
        else:
            sp_parts = sp.split(';')
            for _sp in sp_parts:
                _dict = {
                    'genus' : gn,
                    'species': _sp,
                    'iucn_status_code' : row['iucn_status_code'],
                    'common_names' : row['common_names']
                }

                new_df = new_df.append(_dict, ignore_index=True)

    return new_df


def main():
    old_path = os.getcwd()
    cur_path = get_cur_path()
    os.chdir(cur_path)

    df = get_raw_data()
    df = parse(df)
    op_loc = './../../GeneratedData/IUCN_RedList'
    if not os.path.exists(op_loc):
        os.mkdir(op_loc)
    op_file = 'IUCN_RedList.csv'
    op_file_path = os.path.join(
        op_loc,
        op_file
    )
    df.to_csv(
        op_file_path,
        index=False
    )
    os.chdir(old_path)
    return


