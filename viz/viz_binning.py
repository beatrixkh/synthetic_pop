import numpy as np, pandas as pd, math, os

# globals ----------------------------------------------------------------------

all_races = ['racaian', 'racasn', 'racblk', 'racnhpi',
       'racsor', 'racwht']

## count bins ------------------------------------------------------------------

def count_bins(df, **kwargs):
    """
    for a given set of vars, count how many unique bins there ought to be in a df
    """
    location_cols = ['state', 'county', 'tract', 'block']
    
    n_geoids = df[location_cols].drop_duplicates().shape[0]
    n_sex = kwargs.get('n_sex', 2)
    n_age = kwargs.get('n_age', 116)
    n_relp = kwargs.get('n_relp', 18)
    n_ethnicity = kwargs.get('n_ethnicity', 2)
    n_race = kwargs.get('n_race', 2**6 - 1) #6 categories, have to be at least one race
    
    return n_geoids * n_sex * n_age * n_relp * n_ethnicity * n_race


def find_n_hi(df, n, **kwargs):
    """
    for a given n, find n-hi.
    if using binning, need to input the total number of bins, n_bins
    """
    n_bins = kwargs.get('n_bins', count_bins(df))
    
    location_cols = ['state', 'county', 'tract', 'block']
    
    # vars on which to key
    geoids = kwargs.get('geoids', location_cols + ['geoid'])
    sex = kwargs.get('sex', 'sex_id')
    age = kwargs.get('age', 'age')
    relp = kwargs.get('relp', 'relationship')
    ethnicity = kwargs.get('ethnicity', 'hispanic')
    race = kwargs.get('race', 'race_detail') #6 categories, have to be at least one race

    key_cols = geoids + [sex, age, relp, ethnicity, race]
    df = df.groupby(key_cols).sum().reset_index()
    
    filled = df[df.pweight > n].shape[0]
    
    return n_bins - filled


## load and process data -------------------------------------------------------

# create relp map

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


def agg_data(df, pweight_var = 'pweight', **kwargs):

    geoid_vars = kwargs.get('geoid_vars', ['state', 'county', 'tract', 'block', 'geoid'])
    age_var = kwargs.get('age_var', 'age')
    sex_var = kwargs.get('sex_var', 'sex_id')
    relp_var = kwargs.get('relp_var', 'relationship')
    hisp_var = kwargs.get('hisp_var', 'hispanic')
    race_var = kwargs.get('race_var', 'race_detail')

    groupby = geoid_vars + [age_var, sex_var, relp_var, hisp_var, race_var]

    df = df[groupby + [pweight_var]].groupby(groupby).sum().reset_index()

    return df


def nCk(n,k):
    num = math.factorial(n)
    denom = math.factorial(n-k) * math.factorial(k)
    return num / denom

def add_race_vars(df):
    
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

    return df


def read_dir(state, br = None):
    in_dir = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/{}/'.format(state)
    files = os.listdir(in_dir)
    
    df = pd.DataFrame()
    i = 0
    for file in files:
        df_0 = pd.read_csv(in_dir + file)
        df = df.append(df_0)
        
        i += 1
        if br!=None:
            if i == br:
                break
                
    
    # tabulate data
    key_cols = ['state', 'county', 'tract', 'block', 'geoid', 'age', 'sex_id',
       'relationship', 'hispanic', 'racaian', 'racasn', 'racblk', 'racnhpi',
       'racsor', 'racwht']
    df = df.groupby(key_cols).sum().reset_index()
    
    return df