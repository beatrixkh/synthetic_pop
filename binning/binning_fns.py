import numpy as np, pandas as pd, math, os, pickle

# base fns ---------------------------------------------------------------------

def nCk(n,k):
    num = math.factorial(n)
    denom = math.factorial(n-k) * math.factorial(k)
    return num / denom

# globals ----------------------------------------------------------------------
all_races = ['racaian', 'racasn', 'racblk', 'racnhpi',
       'racsor', 'racwht']

relp_label = {0:'head',
              1:'spouse',
              2:'biolog_child',
              3:'adopted_child',
              4:'stepchild',
              5:'sibling',
              6:'parent',
              7:'grandchild',
              8:'parent_in_law',
              9:'child_in_law',
              10:'other_relative',
              11:'roomer_boarder',
              12:'house_roommate',
              13:'unmarried_part',
              14:'foster_child',
              15:'other_nonrel',
              16:'inst_gq',
              17:'noninst_gq'
             }

relp_map = {'head':'head',
            'spouse':'spouse',
           'biolog_child':'child',
           'stepchild':'child',
           'sibling':'other',
           'parent':'other',
           'grandchild':'grandchild',
           'parent_in_law':'other',
           'child_in_law':'other',
           'other_relative':'other',
           'roomer_boarder':'other',
           'house_roommate':'other',
           'unmmarried_part':'unmarried_part',
           'foster_child':'foster_child',
           'other_nonrel':'other',
           'inst_gq':'inst_gq',
           'noninst_gq':'noninst_gq'}

bin_dict = {('race','race_detail'):2**6-1, #most granular race
            ('race','multiracial3'):nCk(6,1) + nCk(6,2) + 1,
            ('race','multiracial4'):nCk(6,1) + nCk(6,2) + nCk(6,3) + 1,
            ('age','age'):116, #most granular age
            ('age','a1'):len(np.arange(85).tolist() + ['85-89', '90-94', '95-99', '100+']),
            ('age','a2'):len(np.arange(90).tolist() + ['90-94', '95-99', '100+']),
            ('age','a3'):len(np.arange(90).tolist() + ['90+']),
            ('relp','relationship'):18, #most granular relp
            ('relp','relp_reduced'):set(relp_map.values()).__len__(),
            ('sex','sex_id'):2, #most granular sex
            ('ethnicity','hispanic'):2} #most granular ethnicity

fips_codes = {1: 'al', 2: 'ak', 5: 'ar', 4: 'az', 6: 'ca', 8: 'co', 9: 'ct', 
10: 'de', 11: 'dc', 12: 'fl', 13: 'ga', 15: 'hi', 16: 'id', 17: 'il', 18: 'in', 
19: 'ia', 20: 'ks', 21: 'ky', 22: 'la', 23: 'me', 24: 'md', 25: 'ma', 26: 'mi',
 27: 'mn', 28: 'ms', 29: 'mo', 30: 'mt', 31: 'ne', 32: 'nv', 33: 'nh', 34: 'nj', 
 35: 'nm', 36: 'ny', 37: 'nc', 38: 'nd', 39: 'oh', 40: 'ok', 41: 'or', 42: 'pa', 
 44: 'ri', 45: 'sc', 46: 'sd', 47: 'tn', 48: 'tx', 49: 'ut', 50: 'vt', 51: 'va', 
 53: 'wa', 54: 'wv', 55: 'wi', 56: 'wy', 72: 'pr'}

# data prep --------------------------------------------------------------------

def add_race_vars(df, include_bins = True):
    
    # add a column with # of races
    df['racsum'] = df[all_races].sum(axis=1)
    
    # collapse race dummy vars
    df['race7'] = 'multi'
    for race in all_races:
        df.loc[(df[race]==1) & (df.racsum==1),'race7'] = race

    # create 63-race col
    df['race_detail'] = ['racaian{}_racasn{}_racblk{}_racnhpi{}_racsor{}_racwht{}'.format(a,b,c,d,e,f) 
    for (a,b,c,d,e,f)in zip(df.racaian,
        df.racasn, df.racblk, df.racnhpi, df.racsor, df.racwht)]
    
#     if include_bins:
#         df['multiracial3'] = [i if j<3 else 'three_plus' for (i,j) in zip(df.race_detail, df.racsum)]
#         df['multiracial4'] = [i if j<4 else 'four_plus' for (i,j) in zip(df.race_detail, df.racsum)]

    return df

def add_bin_vars(df):
    # add sub categories
    df['a1'] = [i if i < 85 else (i - i%5) if i < 100 else 100 for i in df.age]
    df['a2'] = [i if i < 90 else (i- i%5) if i < 100 else 100 for i in df.age]
    df['a3'] = [i if i < 90 else 90 for i in df.age]
    
    df['multiracial3'] = [i if j<3 else 'three_plus' for (i,j) in zip(df.race_detail,df.racsum)]
    df['multiracial4'] = [i if j<4 else 'three_plus' for (i,j) in zip(df.race_detail,df.racsum)]
    
    df['relp_reduced'] = df.relationship.map(relp_label).map(relp_map)
    
    return df

# binning fns ------------------------------------------------------------------

def count_bins_per_geoid(age = 'age', race = 'race_detail', relp = 'relationship', sex = 'sex_id', ethnicity = 'hispanic'):
    """
    for a given binning scenario, calculate max possible bins per geoid
    """
    n_sex = bin_dict[('sex', sex)]
    n_ethnicity = bin_dict[('ethnicity', ethnicity)]
        
    n_age = bin_dict[('age', age)]
    n_race = bin_dict[('race', race)]
    n_relp = bin_dict[('relp', relp)]
    
    return n_sex * n_age * n_relp * n_ethnicity * n_race

def find_n_hi(df, ns, **kwargs):
    """
    for a given n, find n-hi.
    if using binning, need to input the total number of bins, n_bins
    """    
    location_cols = ['state', 'county', 'tract', 'block']
    
    # vars on which to key
    geoids = kwargs.get('geoids', location_cols + ['geoid'])
    sex = kwargs.get('sex', 'sex_id')
    age = kwargs.get('age', 'age')
    relp = kwargs.get('relp', 'relationship')
    ethnicity = kwargs.get('ethnicity', 'hispanic')
    race = kwargs.get('race', 'race_detail') #6 categories, have to be at least one race

    # get total bins possible
    n_geoid = df[geoids].drop_duplicates().shape[0]
    n_bins = n_geoid * count_bins_per_geoid(age = age, race = race, relp = relp,
                                            sex = sex, ethnicity = ethnicity)    
    # get total bins geq n
    key_cols = geoids + [sex, age, relp, ethnicity, race]
    df = df.groupby(key_cols).sum().reset_index()
    
    n_his = pd.DataFrame(data = {'n':[np.inf], 'n_hi':[n_bins]})
    for n in ns: 
        filled = df[df.pweight > n].shape[0]
        n_his.loc[n_his.shape[0]] = n, n_bins - filled
    
    # subtract
    return n_his

def calc_n_hi_all_scenarios(df, ns = [0,1,2,3,4,5,6,7,8,16,32,64,128,256,512]):

    # make sure just one tract
    assert(df[['state','county','tract']].drop_duplicates().shape[0]==1), 'Oops; function expects one tract at a time'

    state = df.iloc[0].state
    county = df.iloc[0].county
    tract = df.iloc[0].tract

    state_name = fips_codes[state]

    # format df
    df = add_race_vars(df)
    df = add_bin_vars(df)
    
    # calculate n_hi for each n
    n_his = pd.DataFrame()
    for age in ['age','a1','a2','a3']:
        for race in ['race_detail','multiracial3','multiracial4']:
            for relp in ['relationship','relp_reduced']:
                his = find_n_hi(df, ns = ns, age = age, race = race, relp = relp)
                his[['state_name','state','county','tract','age_var','race_var',
                'relp_var']] = pd.DataFrame([[state_name, state, county, tract, 
                  age, race, relp]], index = his.index)
                n_his = n_his.append(his)
                
    return n_his

def pull_zero_pop_tracts(state_name):
    """ for a given state, pull a dataframe containing all tracts with pop==0
    """
    zero_pop_path =  '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/zero_pop_tracts/state_{}_geoids.csv'.format(state_name)
    zero_pop = pd.read_csv(zero_pop_path)
    zero_pop = zero_pop[['STATE','COUNTY','TRACT']].drop_duplicates()
    
    return [(c,t) for (c,t) in zip(zero_pop.COUNTY,zero_pop.TRACT)]


def save_n_his(df, state_name, state, county):
    
    state_dir = '/ihme/scratch/users/beatrixh/synthetic_pop/binning_results/n_his/{}/'.format(state_name)
    save_path = state_dir + 'state{}_county{}_nhis.csv'.format(state, county)
    
    if not os.path.exists(state_dir):
        os.mkdir(state_dir)
    
    df.to_csv(save_path, index = False)
    print('saved state {} (fips {}), county {}'.format(state_name, state, county))

# load pickle objs -------------------------------------------------------------

def load_obj(name ):
    path = '/ihme/scratch/users/beatrixh/fips_dicts/'
    with open(path + name + '.pkl', 'rb') as f:
        return pickle.load(f)

def load_county_dicts(states):
    d = {}
    for state in states:
        path = '{}_county_dict'.format(state)
        new = load_obj(path)
        d = {**d, **new}
    return d

def load_tract_dicts(states):
    d = {}
    for state in states:
        path = '{}_tract_dict'.format(state)
        new = load_obj(path)
        d = {**d, **new}
    return d 