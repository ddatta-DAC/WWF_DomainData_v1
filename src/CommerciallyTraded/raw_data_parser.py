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

    print(df.columns)
    return df

def process_scientific_names(input_str):
    input_str = input_str.strip()
    # Handle multiple cases
    # Sadly these have to be hardcoded
    # No requirement to know subspecies
    # Case 1 :
    # 'var.' xx y2 var. y2
    # Case 3
    # Cordia dichotoma (also as Cordia suaveolens)

    def remove_subspecies(_sp_str):
         pattern = re.compile('subsp\.[a-z]?(\s)*')
         res = re.sub(pattern_1, ';', _sp_str)
         return res

    kw1 = ' var. '
    kw2 = '(& misspelt as'
    kw3 = '(also as'
    kw4 = '(as'

    if kw1 in input_str:
        pattern_1 = re.compile('.*(\s)var\.(\s)*')
        p1_res = re.sub(pattern_1, ';', input_str)
        parts = p1_res.split(';')
        genus = None
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
        res = []
        sp = None
        for part in parts:
            tmp = part.split(' ')
            if len(tmp) == 2:
                genus = tmp[0]
                sp = tmp[0]
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
        input_str =  input_str.strip()
        parts = input_str.split(';')
        parts = [ _.strip() for _ in parts ]

        res = ';'.join(parts)
        return res
    else:
        return input_str

    return None
def parse_df(df):
    main_df = df
    print(main_df.columns)

    main_df.reset_index(inplace=True)
    main_df['genus'] = None
    main_df['species'] = None
    main_df['sub_species'] = None
    main_df = main_df.rename(
        columns={"Scientific name ": "sc_name"}
    )

    # Assumption that only 3 max variations of scientific names exist
    columns = [
        'sc_name_1',
        'sc_name_2',
        'sc_name_3',
        'type',
        'family'
    ]
    new_df = pd.DataFrame(columns=columns)

    var_kw = 'var.'
    var_kws = ['misspelt', 'also as', 'as']
    sub_sp_kw = 'subsp.'

    for i, row in main_df.iterrows():
        sc_name = row["sc_name"]
        print('>>', sc_name)
        sc_name = sc_name.replace('\xa0', " ")
        sc_name = sc_name.strip()
        sc_name = textacy.preprocess.normalize_whitespace(sc_name)

        row_dict = {
            'sc_name_1': None,
            'sc_name_2': None,
            'sc_name_3': None,
            'num_sc_names': 0,
            'family': row['Family'],
            'type': row['Type']
        }

        if sc_name is not None and type(sc_name) == str:
            parts = sc_name.split(' ')
            if len(parts) == 2:
                # single sc_name , no subspecies
                row_dict['sc_name_1'] = ' '.join([parts[0], parts[1]])
                row_dict['num_sc_names'] = 1

            elif sub_sp_kw in sc_name:
                # single sc_name , subspecies present
                parts_1 = sc_name.split(sub_sp_kw)
                parts_2 = parts_1[0].split(' ')
                g = parts_2[0].strip()
                sp = parts_2[1].strip()
                ssp = parts_1[1].strip()
                row_dict['sc_name_1'] = ' '.join([g, sp, ssp])
                row_dict['num_sc_names'] = 1

            elif var_kw in sc_name:
                # 2 sc_name , no subspecies
                parts = sc_name.split(var_kw)
                parts_1 = parts[0].split(' ')
                parts_2 = parts[1]
                genus = parts_1[0].strip()
                sp1 = parts_1[1].strip()
                sp2 = parts_2[1].strip()

                row_dict['sc_name_1'] = ' '.join([genus, sp1])
                row_dict['sc_name_1'] = ' '.join([genus, sp2])
                row_dict['num_sc_names'] = 2

            elif '(' in sc_name:
                # 2 possible sc names
                # split on "(" , since something present in ()
                parts = sc_name.split('(')
                parts[1] = parts[1].strip(')')
                part_1 = parts[0]
                parts_1 = part_1.split(' ')
                gn = parts_1[0]
                sp = parts_1[1]
                row_dict['sc_name_1'] = ' '.join([gn, sp])
                # part 2 has some keywords and some other words
                # filter them out
                part_2 = parts[1]
                tmp = []
                doc = textacy.Doc(part_2, lang='en')
                for t in doc:
                    exclude_pos = ['VERB', 'CCONJ', 'ADV', 'ADP', 'PUNCT', 'SYM', 'INTJ', 'PART', 'ADJ']
                    if t.pos_ not in exclude_pos:
                        tmp.append(str(t))

                if tmp[0].isupper():
                    gn2 = tmp[0]
                    sp2 = tmp[1]
                else:
                    gn2 = gn
                    sp2 = tmp[0]
                row_dict['sc_name_2'] = ' '.join([gn2, sp2])
                row_dict['num_sc_names'] = 2

            elif ' x ' in sc_name:
                sc_name = sc_name.replace('x', '')
                sc_name = textacy.preprocess.normalize_whitespace(sc_name)
                parts = sc_name.split(' ')
                genus = parts[0].strip()
                sp = parts[1].strip()
                row_dict['sc_name_1'] = ' '.join([genus, sp])
                row_dict['num_sc_names'] = 1

        print(row_dict)
        new_df = new_df.append(row_dict, ignore_index=True)

    return


df = get_raw_data()
df = parse_df(df)