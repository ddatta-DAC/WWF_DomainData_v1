import pandas as pd
import os
import re
from iso3166 import countries
from collections import defaultdict


def get_raw_data():
    file_loc = './../../Data_v2/ForestProductKeywords'
    file_name = 'ForestProductKeywords.xlsx'
    file_path = os.path.join(
        file_loc,
        file_name
    )
    df = pd.read_excel(file_path, header=None, encoding="utf-8")
    df = df.dropna(subset=[1])
    df = df.dropna(how='all')
    df = df.rename(columns={
        0: 'common_name',
        1: 'scientific_names',
        2: 'region',
        3: 'other_common_names'
    })
    return df


'''
# Returns all the regions and the common names
# Clumping is ok
Primary Key is scientific name 
'''


def get_country_ISO3(string_cn):

    hardcoded_dict = {
        'Vietnam': 'VNM',
        'UK': 'GBR',
        'Venezuela': 'VEN',
        'Burkina-Faso': 'BFA',
        'Bolivia': 'BOL',
        'Dem. Rep. of the Congo': 'COD',
        'Côte d’Ivoire': 'CIV',
        'Laos': 'LAO',
        'Tanzania': 'TZA'
    }
    res = None
    if string_cn in hardcoded_dict.keys():
        res = hardcoded_dict[string_cn]
    return res


def parse_row(row):

    txt = row['region']
    if type(txt) != str:
        res1 = None
    else:
        txt = txt.replace('\xa0', "")
        txt = txt.replace('"', '')
        txt = txt.split('\n')
        res1 = []
        for t in txt:
            if t != '':
                try:
                    obj_c = countries.get(t)
                    t = obj_c.alpha3
                    res1.append(t)
                except:
                    t = get_country_ISO3(t)
                    if t is not None: res1.append(t)

    res2 = None
    main_common_name = row['common_name']
    if main_common_name is not None and type(main_common_name) == str:
        main_common_name = main_common_name.lower()
        main_common_name = main_common_name.replace('\n','')
        main_common_name = main_common_name.strip(' ')
        main_common_name = main_common_name.strip('\n')
        main_common_name = main_common_name.strip(' ')
        main_common_name = main_common_name.strip('"')
        main_common_name = main_common_name.replace('"','')

        if ',' in main_common_name:
            main_common_name = ' '.join(main_common_name.split(',')[::-1])
        res2 = [main_common_name]

        txt = row['other_common_names']
        try:
            txt = txt.replace('\xa0', '')
            txt = txt.replace('"', '')
            txt = txt.replace(',\n', ';')
            txt = txt.repalce('\n', ' ')
            txt = txt.split(' ')
            for t in txt:
                if t != '':
                    res2.append(t.lower())
        except:
            pass

        res2 = list(set(res2))
    return res1, res2


def parse_sp_name(text):
    text = text.replace('\xa0', '')
    tmp = text.split('\n')
    parts = []

    for p in tmp:
        if p != '':
            parts.append(p)

    res = []
    for p in parts:
        # Check 'Syn' present or () present
        parts1 = p.split('Syn')
        for item1 in parts1:
            item1 = item1.replace(')', '')
            item1 = item1.replace('(', '')
            item1 = item1.strip('.')
            item1 = item1.strip()
            parts2 = item1.split(' ')
            r = ' '.join(parts2[0:2])

            if r != '':
                res.append(r)
    res = ';'.join(res)
    return res

def process(df):

    df_0 = pd.DataFrame(columns=['genus', 'species', 'common_names', 'regions'])

    for i, row in df.iterrows():
        sc_names = parse_sp_name(row['scientific_names'])
        regions, common_names = parse_row(row)

        if len(sc_names) == 0:
            continue
        if regions is not None and len(regions) > 0:
            regions = ';'.join(regions)
        else:
            regions = None

        if common_names is not None and len(common_names) > 0:
            common_names = ';'.join(common_names)
        else:
            common_names = None

        list_sc_names = sc_names.split(';')
        for sc_name in list_sc_names:
            parts = sc_name.split(' ')
            if len(parts) == 1 : continue

            genus = parts[0]
            species = parts[1]
            if species == 'spp':
                species = 'spp.'

            insert_dict = {
                'genus': genus,
                'species' : species,
                'regions': regions,
                'common_names': common_names
                }
            df_0 = df_0.append(insert_dict, ignore_index=True)


    # Deduplicate the outputs, since same scientific name can have multiple rows
    df_1 = pd.DataFrame(df_0,copy=True)

    def join_genus_sp(row):
        sn = ' '.join([row['genus'],row['species']])
        return sn

    # df_1['scientific_name'] = df_1.apply(join_genus_sp , axis=1)
    df_1 = df_1.fillna('')
    res  = df_1.groupby(
        ['genus','species'],
        as_index=False
    ).agg(
            {'common_names': ';'.join, 'regions': ';'.join}
    )
    print(res)
    output_df = pd.DataFrame(
       res,copy=True
    )
    #
    # for g in groupby_obj:
    #     g_df = g.reset_index()
    #     print(g_df)

    return output_df





def main():
    df = get_raw_data()
    df = process(df)
    op_file_loc = './../../GeneratedData/ForestProductKeywords'
    if not os.path.exists(op_file_loc):
        os.mkdir(op_file_loc)
    op_file_name = 'ForestProductKeywords.csv'
    op_file_path = os.path.join(
        op_file_loc,op_file_name
    )
    df.to_csv(op_file_path,index=False)
    return


