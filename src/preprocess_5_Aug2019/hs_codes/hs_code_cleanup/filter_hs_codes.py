import pandas as pd
from pprint import pprint

def get_boundary_pairs( hs_code_list ,level = 1 ):

    if level == 1 :
        f_list = [ 44 , 92 , 93 , 94 ,95 ]
    elif level  == 2 :
        f_list = [
            44, 940161,940169 , 94019015, 94019040, 9401905081 ,
            940330, 940340 ,940350, 940360 , 94039070, 9201,
            9202, 920590, 92060020, 920790, 92099920,
            9209994040, 950420, 95069915
        ]

    boundary_pairs = []
    l1 = []

    for code in f_list :
        start = str(code)
        stop = str(code+1)
        pts = [3,5,7]
        # place in . in code to make it match
        def _fmt(code_str):
            res = ''
            for j in range(len(code_str)):
                res += code_str[j]
                if j in pts:
                    res += '.'
            res = res.strip('.')
            # if len(res)<4:
            #     res += '01'

            return res
        start = _fmt(start)
        l1.append(start)
    l1 = (sorted(l1))

    exit_on = None
    for i in l1:
        switch = 0
        res_list  = []
        for h in hs_code_list:
            string_start = h[0:len(i)]

            if i == string_start:
                switch = 1
                res_list.append(h)
            elif switch == 1 and i != string_start:
                exit_on = h
                break
        boundary_pairs.append((res_list[0],exit_on))
    return  boundary_pairs



def filter_df(df , level ):
    hs_code_list = set(list(df['hs_code']))
    cleaned_hs = []
    for h in hs_code_list :
        if type(h)== str :
            cleaned_hs.append(h)

    hs_code_list = sorted(cleaned_hs)
    bp = get_boundary_pairs(hs_code_list , level)
    new_df = pd.DataFrame(columns = list(df.columns))
    for item in bp:
        start = item[0]
        stop = item[1]
        start_idx = (df.index[df['hs_code'] == start].tolist())[0]
        stop_idx = (df.index[df['hs_code'] == stop].tolist())[0]
        tmp_df = df.iloc[start_idx:stop_idx]
        try:
            del tmp_df['Unnamed: 0']
        except:
            pass
        new_df = new_df.append(tmp_df,ignore_index=True)

    try:
        del new_df['Unnamed: 0']
    except:
        pass


    return new_df

