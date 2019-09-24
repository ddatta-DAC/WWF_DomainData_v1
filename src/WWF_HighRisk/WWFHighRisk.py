import pandas as pd
import textacy
import spacy
import os
import inspect
import sys

nlp = spacy.load('en')
from iso3166 import countries

def get_cur_path():
    this_file_path = '/'.join(
        os.path.abspath(
            inspect.stack()[0][1]
        ).split('/')[:-1]
    )

    os.chdir(this_file_path)
    print(os.getcwd())
    return this_file_path

# Intermediate file manually edited. Donot lose that !!!!!
# 'WWFHighRisk_intermediate.xlsx'

# In[18]:


def get_df():

    loc = './../../Data_v2/WWF_HighRisk'
    file = 'WWF_HighRiskCountryProfiles_Species.xlsx'
    file_path = os.path.join(loc, file)
    df = pd.read_excel(file_path)
    return df


def process_country_row(sent):
    c_list = sent.replace(",", " , ")
    c_list = c_list.replace('also via', ',')
    c_list = c_list.replace('and via', ',')
    c_list = c_list.replace('all via', ',')
    c_list = c_list.replace(' all', ',')
    c_list = c_list.replace(' also', ',')
    c_list = c_list.replace(' and', ',')
    c_list = c_list.replace(' & ', ',')
    c_list = c_list.replace(' also ', ',')
    c_list = c_list.replace(' via ', ',')
    c_list = c_list.replace('-', ' ')
    c_list = textacy.preprocess.normalize_whitespace(c_list)
    c_list = c_list.split('(Note')[0]
    c_list = c_list.split(',')

    res_list = []
    for item in c_list:
        item = item.strip()
        if len(item) < 20 and len(item) > 0:
            res_list.append(item)

    return res_list


'''
#  -------------------------------------- #
#  Replace the country names!
#  Manually curated List 
#  Using iso31366 package
#  from iso3166 import countries
#  print(countries.get('us'))
#  for c in countries:
#        print (c)
#  -------------------------------------- #
'''


def parse_countries(df):
    # Hardcoded for clean up
    ref_corr_country_names = {
        'Bolivia': 'Bolivia, Plurinational State of',
        'Russia': 'Russian Federation',
        'Lao DR': "Lao People's Democratic Republic",
        'DRC': 'Congo, Democratic Republic of the',
        'Vietnam': "Viet Nam"
    }

    replace_dict = {}
    list_countries = list(df['Country of Origin Risk'])

    # ---------- #
    # Create a list of all countries , to clean up
    # ---------- #
    set_countries = []
    for c_list in list_countries:
        if type(c_list) != str:
            continue
        res_list = process_country_row(c_list)
        set_countries.extend(res_list)

    set_countries = list(set(set_countries))
    for c in set_countries:
        if c not in ref_corr_country_names.keys():
            res = countries.get(c)
        else:
            c_name = ref_corr_country_names[c]
            res = countries.get(c_name)

        if res is not None:
            replace_dict[c] = res.alpha3
        else:
            print("error")

    # Place the country names back in the Data Frame
    # using replace_dict
    for i, row in df.iterrows():
        sent = row['Country of Origin Risk']
        p1 = None
        p2 = None

        if type(sent) == str:
            # split on via
            sent = sent.replace(",", " , ")
            parts = sent.split(' via ')
            if len(parts) > 1:
                p1 = process_country_row(parts[0])
                p2 = process_country_row(parts[1])
            else:
                p1 = process_country_row(parts[0])

            origins = None
            conduit = None
            if p1 is not None:
                origins = []

            for p in p1:
                if p in ref_corr_country_names.keys():
                    p = ref_corr_country_names[p]
                origins.append(str(countries.get(p).alpha3))

            origins = ';'.join(origins)
            if p2 is not None:
                conduit = []
                for p in p2:
                    if p in ref_corr_country_names.keys():
                        p = ref_corr_country_names[p]
                    conduit.append(countries.get(p).alpha3)
                conduit = ';'.join(conduit)
        df.loc[i, 'origin'] = origins
        df.loc[i, 'conduit'] = conduit

    return df


def main_aux():
    # Filter out Columns needed
    df = get_df()
    df_1 = parse_countries(df)
    df_1 = df_1[
        ['Taxonomic Genus', 'Species, if applicable', 'Common Names', 'origin', 'conduit']
    ]
    df_2 = df_1.rename(columns={
        'Taxonomic Genus': 'genus',
        'Species, if applicable': 'species',
        'Common Names': 'common_name'
    })

    df_2.to_excel('WWFHighRisk_intermediate.xlsx', index=False)

    '''
    NOTE:
    WWFHighRisk_intermediate.xlsx was saved and edited :
    WWFHighRisk_intermediate_edited.xlsx
    Reason :
    Incorrect and inconsistent formatting provided by data provider, along with irrelevant text
    '''

    df_3 = pd.read_excel(
        'WWFHighRisk_intermediate_edited.xlsx',
        index_col=None
    )

    for i, row in df_3.iterrows():
        cn = row['common_name']
        if type(cn) != str:
            continue
        cn = cn.split(',')
        res = []
        for c in cn:
            c = c.strip()
            c = c.lower()
            c = textacy.preprocess.normalize_whitespace(c)
            res.append(c)
        res = ';'.join(res)
        df_3.loc[i, 'common_name'] = res

    # df_3['regions'] = None

    def set_regions(row):
        s1 = row['origin']
        s2 = row['conduit']
        if type(s1) == str and type(s2) == str:
            s1 = s1.split(';')
            s2 = s2.split(';')
        elif type(s1) == str:
            s1 = s1.split(';')
            s2 = []
        elif type(s2) == str:
            s2 = s2.split(';')
            s1 = []
        else:
            s1 = []
            s2 = []
        res = list(s1)
        res.extend(s2)
        res = ';'.join(res)
        return res

    df_3['regions'] = df_3.apply(set_regions, axis=1)
    df_3 = df_3.rename(columns={'common_name':'common_names'})

    try:
        del df_3['origin']
        del df_3['conduit']
    except:
        pass
    df_3['genus'] = df_3['genus'].apply(str.strip,args=(' ',))
    print(os.getcwd())
    op_loc = './../../GeneratedData/WWF_HighRisk'
    if not os.path.exists(op_loc):
        os.mkdir(op_loc)

    op_file = 'WWF_HighRisk.csv'
    op_file_path = os.path.join(op_loc, op_file)
    df_3.to_csv(op_file_path,index=False)
    return

def main():
    old_path = os.getcwd()
    cur_path = get_cur_path()
    os.chdir(cur_path)
    main_aux()
    os.chdir(old_path)
