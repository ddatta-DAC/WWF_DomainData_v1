import pandas as pd
import numpy as np
import os
import sys

'''
Domain knowledge
# Remove_families :
'Leguminosae','Orchidaceae','Cactaceae',
'Cyatheaceae', 'Euphorbiaceae','Primulaceae','Thymelaeaceae'

Also : all plants of a genus belongs to same family

CITES :
listing in ['I','II','III']

IUCN :
0 : 'LR/nt', 'LC', 'LR/lc'
1 : Rest

'''


def get_scn(row):
    return ' '.join([row['genus'], row['species']])


def fix_generic_species(_input):
    if _input == 'spp.':
        return _input
    elif _input == 'spp':
        return 'spp.'
    else:
        return _input


'''
Treat the ('genus','species') as primary keys
'''


def check_has_duplicates(ref_df):
    df = pd.DataFrame(ref_df, copy=True)

    df['genus'] = df['genus'].apply(str.capitalize)
    df['species'] = df['species'].apply(str.lower)
    # fix up spp and spp. : have spp. only
    df['species'] = df['species'].apply(fix_generic_species)

    df['sc_name'] = df.apply(get_scn, axis=1)

    dd_df = df.drop_duplicates(subset=['sc_name'])
    del df['sc_name']
    res = len(df) != len(dd_df)
    return res


def deduplicate(df):
    # Check duplicates
    if (check_has_duplicates(df)):
        _fdict = {}
        _columns = list(df.columns)
        _columns.remove('genus')
        _columns.remove('species')
        df = df.fillna('')
        for c in _columns:
            _fdict[c] = ';'.join
        res = df.groupby(
            ['genus', 'species'],
            as_index=False
        ).agg(
            _fdict
        )
        print(res.head(10))
        return res

    return df


def get_data_sources():
    sources = ['IUCN_RedList', 'CITES', 'ForestProductKeywords', 'CommerciallyTraded', 'WWF_HighRisk']
    file_loc = './../../GeneratedData'

    df_dict = {}
    for source in sources:
        f_path = os.path.join(
            file_loc,
            source,
            source + '.csv'
        )
        df = pd.read_csv(f_path, index_col=None)
        print('source', source, ' | ', df.columns)
        df = deduplicate(df)
        df_dict[source] = df
        print(len(df))
    return df_dict


# create genus to family mapping
def get_genus_family_map(_df):
    genus_family = {}

    for i, row in _df.iterrows():
        if row['genus'] not in genus_family.keys():
            genus_family[row['genus']] = row['family']

    return genus_family


# Find all scientific names
# Genus spp. and Genus species


def filter_out_unwanted_families(df):
    exclude_families = ['Leguminosae', 'Orchidaceae', 'Cactaceae', 'Cyatheaceae', 'Euphorbiaceae', 'Primulaceae',
                        'Thymelaeaceae']
    print(len(df))
    df = df.loc[~df['family'].isin(exclude_families)]
    print(len(df))
    return df


'''
Add in the common names
Take care of spp. case as well
'''


def add_common_names(ref_df, df_dict):
    # common names present in 3 sources :
    # IUCN_RedList, ForestProductKeywords, WWF_HighRisk
    df = pd.DataFrame(ref_df, copy=True)
    target_dfs = [df_dict['IUCN_RedList'], df_dict['WWF_HighRisk'], df_dict['ForestProductKeywords']]
    all_scn = list(ref_df['sc_name'])

    for i, row in ref_df.iterrows():
        cn_list = []
        scn = row['sc_name']

        gflag = 'spp.' in scn

        for tdf in target_dfs:
            idx_list = tdf.loc[tdf['sc_name'] == scn].index.to_list()
            if len(idx_list) == 0: continue
            idx = idx_list[0]
            cn = tdf.at[idx, 'common_names']
            if cn is not None and type(cn) == str:
                cn_list.extend(cn.split(';'))
        cn_list = [_ for _ in cn_list if len(_) > 1]
        cn_list = set(cn_list)
        cn = ';'.join(cn_list)

        # spp. means add common names to all members of the genus
        if gflag:
            genus = row['genus']
            indices = ref_df.loc[ref_df['genus'] == genus].index.to_list()
            for j in indices:
                prev = ref_df.at[j, 'common_names']
                if prev is None or type(prev) != str:
                    df.at[j, 'common_names'] = cn
                else:
                    df.at[j, 'common_names'] = prev + ';' + cn
        else:
            df.at[i, 'common_names'] = cn

    return df


def add_region(ref_df, df_dict):
    # common names present in 3 sources :
    # CITES, ForestProductKeywords, WWF_HighRisk
    df = pd.DataFrame(ref_df, copy=True)
    target_dfs = [df_dict['CITES'], df_dict['WWF_HighRisk'], df_dict['ForestProductKeywords']]
    all_scn = list(ref_df['sc_name'])

    for i, row in ref_df.iterrows():
        reg_list = []
        scn = row['sc_name']
        gflag = 'spp.' in scn

        for tdf in target_dfs:
            idx_list = tdf.loc[tdf['sc_name'] == scn].index.to_list()
            if len(idx_list) == 0: continue
            idx = idx_list[0]
            reg = tdf.at[idx, 'regions']
            if reg is not None and type(reg) == str:
                reg_list.extend(reg.split(';'))
        reg_list = [_ for _ in reg_list if len(_) > 1]
        reg_list = set(reg_list)
        reg = ';'.join(reg_list)

        # spp. means add common names to all members of the genus
        if gflag:
            genus = row['genus']
            indices = ref_df.loc[ref_df['genus'] == genus].index.to_list()
            for j in indices:
                prev = ref_df.at[j, 'regions']
                if prev is None or type(prev) != str:
                    df.at[j, 'regions'] = reg
                else:
                    df.at[j, 'regions'] = prev + ';' + reg
        else:
            df.at[i, 'regions'] = reg

    return df


'''
Add in the flags
1. CITES
2. IUCN 

3. WWF_highRisk

IUCN :
0 : 'LR/nt', 'LC', 'LR/lc'
1 : Rest
'''


def add_cites_flag(ref_df, data_df):
    target_listing = ['I', 'II', 'III']

    ref_df = ref_df.loc[ref_df['cites_listing'].isin(target_listing)]

    for i, row in ref_df.iterrows():
        scn = row['sc_name']
        idx_list = data_df.loc[data_df['sc_name'] == scn].index.to_list()
        if len(idx_list) == 0: continue

        gflag = 'spp.' in scn
        if gflag:
            genus = row['genus']
            idx_list = data_df.loc[data_df['genus'] == genus].index.to_list()
            for j in idx_list:
                data_df.at[j, 'cites_flag'] = 1
        else:
            idx = idx_list[0]
            data_df.at[idx, 'cites_flag'] = 1
    return data_df


def add_iucn_flag(ref_df, data_df):
    nottarget_listing = ['LR/nt', 'LC', 'LR/lc']

    ref_df = ref_df.loc[~ref_df['iucn_status_code'].isin(nottarget_listing)]

    for i, row in ref_df.iterrows():
        scn = row['sc_name']
        idx_list = data_df.loc[data_df['sc_name'] == scn].index.to_list()
        if len(idx_list) == 0: continue

        gflag = 'spp.' in scn
        if gflag:
            genus = row['genus']
            idx_list = data_df.loc[data_df['genus'] == genus].index.to_list()
            for j in idx_list:
                data_df.at[j, 'iucn_flag'] = 1
        else:
            idx = idx_list[0]
            data_df.at[idx, 'iucn_flag'] = 1
    return data_df


def add_wwf_high_risk_flag(ref_df, data_df):
    for i, row in ref_df.iterrows():
        scn = row['sc_name']
        idx_list = data_df.loc[data_df['sc_name'] == scn].index.to_list()
        if len(idx_list) == 0: continue

        gflag = 'spp.' in scn
        if gflag:
            genus = row['genus']
            idx_list = data_df.loc[data_df['genus'] == genus].index.to_list()
            for j in idx_list:
                data_df.at[j, 'wwf_high_risk_flag'] = 1
        else:
            idx = idx_list[0]
            data_df.at[idx, 'wwf_high_risk_flag'] = 1
    return data_df


def main():
    df_dict = get_data_sources()
    # This is hardcoded for now
    attributes = [
        'sc_name',
        'genus',
        'species',
        'family',
        'cites_flag',
        'iucn_status_code',
        'common_names',
        'regions',
        'wwf_high_risk_flag'
    ]

    master_df = pd.DataFrame(columns=attributes)
    all_scn = []

    for source, _df in df_dict.items():
        _df['sc_name'] = _df.apply(get_scn, axis=1)
        all_scn.extend(list(_df['sc_name']))

    all_scn = sorted(set(all_scn))

    genus_family_map = get_genus_family_map(df_dict['CommerciallyTraded'])

    for scn in all_scn:
        parts = scn.split(' ')
        gn = parts[0]
        sp = parts[1]
        family = None
        if gn in genus_family_map.keys():
            family = (genus_family_map[gn]).capitalize()

        _dict = {
            'sc_name': scn,
            'genus': gn,
            'species': sp,
            'family': family,
            'cites_flag': 0,
            'common_names': None,
            'regions': None,
            'iucn_flag': 0,
            'wwf_high_risk_flag': 0
        }

        master_df = master_df.append(
            _dict,
            ignore_index=True
        )

    master_df = filter_out_unwanted_families(master_df)
    master_df_1 = add_common_names(master_df, df_dict)
    master_df_1.to_csv('tmp1.csv', index=False)

    master_df_2 = add_region(master_df_1, df_dict)
    master_df_2.to_csv('tmp2.csv', index=False)

    ref_df = df_dict['CITES']
    master_df_3 = add_cites_flag(ref_df, master_df_2)
    ref_df = df_dict['IUCN_RedList']
    master_df_4 = add_iucn_flag(ref_df, master_df_3)
    ref_df = df_dict['WWF_HighRisk']
    master_df_5 = add_wwf_high_risk_flag(ref_df, master_df_4)

    op_loc = './../../GeneratedData/Collated'

    if not os.path.exists(op_loc):
        os.mkdir(op_loc)
    op_f_name = 'Complete_Domain_Data.csv'
    op_file_path = os.path.join(op_loc, op_f_name)

    master_df_5.to_csv(op_file_path, index=None)

