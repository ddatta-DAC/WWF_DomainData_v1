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

'''
Add in the common names
Take care of spp. case as well
'''
def add_common_names(ref_df, df_dict):
    # common names present in 3 sources :
    # IUCN_RedList, ForestProductKeywords, WWF_HighRisk
    df = pd.DataFrame(ref_df,copy=True)
    target_dfs = [df_dict['IUCN_RedList'],df_dict['WWF_HighRisk'],df_dict['ForestProductKeywords']]
    all_scn = list(ref_df['sc_name'])

    for i,row  in df.iterrows():
        scn = row['sc_name']
        gflag = 'spp.' in scn
        for tdf in target_dfs:
            idx = ref_df.loc[ref_df[scn]].index[0]
            print(idx)
            pass

    return

def add_region(ref_df, df_dict):
    # common names present in 3 sources :
    # CITES, ForestProductKeywords, WWF_HighRisk
    df = pd.DataFrame(ref_df, copy=True)
    target_dfs = [df_dict['CITES'],df_dict['WWF_HighRisk'],df_dict['ForestProductKeywords']]
    all_scn = list(ref_df['sc_name'])

    for i, row in df.iterrows():
        scn = row['sc_name']
        gflag = 'spp.' in scn
        for tdf in target_dfs:

            pass


    return

def filter_out_unwanted_families(df):
    exclude_families = ['Leguminosae','Orchidaceae','Cactaceae','Cyatheaceae', 'Euphorbiaceae','Primulaceae','Thymelaeaceae']
    print(len(df))
    df = df.loc[~df['family'].isin(exclude_families)]
    print(len(df))
    return df


def process():
    df_dict = get_data_sources()
    # This is hardcoded for now
    attributes = [
        'sc_name'
        'genus',
        'species',
        'family',
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
    print(len(all_scn))

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
            'iucn_status_code': None,
            'common_names': None,
            'regions': None,
            'wwf_high_risk_flag': None
        }

        master_df = master_df.append(
            _dict,
            ignore_index=True
        )
    master_df = filter_out_unwanted_families(master_df)
    # Add in common names
    master_df.to_csv('tmp.csv')
    master_df_1 = add_common_names(master_df, df_dict)
    master_df_2 = add_region(master_df_1, df_dict)

    return master_df_2


def main():
    process()
    return


main()
