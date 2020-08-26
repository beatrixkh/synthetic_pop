import pandas as pd
import numpy as np
from datetime import date
# import tests

"""Goal: pull in ACS data. Output, for a unique state (WA)  x race combination, (black alone, black + asian, aian + nhpi + white, etc.),
a df in long format, with, for each [sex] x [single-year-age] x [state] x [year], the count of corresponding individuals residing in
the location
"""

import pandas as pd, numpy as np, os

all_races = ['racaian', 'racasn', 'racblk', 'racnhpi', 'racsor', 'racwht']

def load_data(state, years = np.arange(1996,2013).tolist() + [2017]):
    assert(len(state)==2), "state must be 2-char abbreviation"
    state = state.upper()
    
    # convert to list
    if type(years)==int:
        years = [years]
    elif type(years)!=list:
        years = years.tolist()
    
    for year in years:
        assert(year in np.arange(1996,2013).tolist() + [2017]), "years must be from 1996-2012 or 2017; else use load_incoming_data()"
    
    df = pd.DataFrame()
    for year in years:
        IN_DIR = "/home/j/DATA/USA/AMERICAN_COMMUNITY_SURVEY/"
        personurl = 'USA_ACS_' + str(year) + '_PERSONS_' + str(state) + '_Y2012M02D01.CSV'
        path = IN_DIR + str(year) + '/' + personurl
        if os.path.isfile(path):
            new_df = pd.read_csv(path) 
            new_df["year"] = str(year)   
            new_df.columns = new_df.columns.map(str.lower)
            df = df.append(new_df)
    df["st"] = state
    df_cols = ["serialno", "st", "year", "pwgtp", "agep", "sex","racnum","racaian","racasn","racblk",
                "racnh","racpi","racsor","racwht","hisp"]
    return df.filter(items=df_cols)

def load_incoming_data(state, years = np.arange(2012,2017).tolist()):
    assert(len(state)==2), "state must be 2-char abbreviation"
    state = state.lower()
    
    # convert to list
    if type(years)==int:
        years = [years]
    elif type(years)!=list:
        years = years.tolist()
    
    for year in years:
        assert(year in np.arange(2012,2017)), "years must be from 2012-2016; else use load_data()"
    
    df = pd.DataFrame()
    for year in years:
        # IN_DIR = "/home/j/DATA/Incoming Data/USA/AMERICAN_COMMUNITY_SURVEY"
        IN_DIR = '/ihme/scratch/users/beatrixh/acs_census_2012_2017/'
        personurl = 'ss' + str(year)[2:] + 'p' + state + '.csv'
        
        new_df = pd.read_csv(IN_DIR + '/' + str(year) + '/' + personurl)
        new_df["year"] = str(year)
        new_df.columns = new_df.columns.map(str.lower)
        df = df.append(new_df)
    df_cols = ["serialno", "st","year", "pwgtp", "agep", "sex","racnum","racaian","racasn","racblk",
                    "racnh","racpi","racsor","racwht","hisp","relp"]
    return df.filter(items=df_cols)

def clean_acs(input_df, races, years, include_vars = ['hispanic','relationship']):
    """input: raw ACS data for a state & year(s)
       output: df with counts of people (using person weights) per state/sex/age/ethnicity/race bin, averaged over all years (each year 
               weighted equally)
    """
    #todos
    print("need to add a check that makes sure aren't pulling all of 'relp' var, pre, and post-2012 data")
    
    #create copy of df
    df = input_df.copy(deep=True)
    
    #combine native hawaiian and pacific islander
    df['racnhpi'] = df[['racnh','racpi']].max(axis=1)
    df.drop(columns=['racnh','racpi'], inplace=True)
    
    #TEST sum of races == racnum
    all_races = ['racaian', 'racasn', 'racblk', 'racnhpi', 'racsor', 'racwht']
    df['race_count'] = df[all_races].sum(axis=1)
    assert(df[df.race_count!=df.racnum].shape[0]==0), "races in each row dont' sum to row total"
#     assert(type(races)==list), "Oops; even if only one race, type(races) needs to be list"
    
    #subset to races of interest
    if type(races)!=list:
        races = [races]

    if not races==['all']:
        if races==['multiracial']:
            df = df[df.race_count > 1]
            print("including all multiracial combinations")
        else:
            df = df[df.race_count==len(races)]
            for i in races:
                df = df[df[i] > 0]
    
    #TEST: people exist in this category
    
    #remap vars
    df["hispanic"] = df['hisp'].map({1: 0,
                                     2: 1,
                                     3: 1,
                                     4: 1,
                                     5: 1,
                                     6: 1,
                                     7: 1,
                                     8: 1,
                                     9: 1,
                                     10: 1,
                                     11: 1,
                                     12: 1,
                                     13: 1,
                                     14: 1,
                                     15: 1,
                                     16: 1,
                                     17: 1,
                                     18: 1,
                                     19: 1,
                                     20: 1,
                                     21: 1,
                                     22: 1,
                                     23: 1,
                                     24: 1})
    
    df["state"] = df["st"].map({1: "AL",
                             2: "AK",
                             4: "AR",
                             5: "AZ",
                             6: "CA",
                             8: "CO",
                             9: "CT",
                             10: "DE",
                             11: "DC",
                             12: "FL",
                             13: "GA",
                             15: "HI",
                             16: "ID",
                             17: "IL",
                             18: "IN",
                             19: "IA",
                             20: "KS",
                             21: "KY",
                             22: "LA",
                             23: "ME",
                             24: "MD",
                             25: "MA",
                             26: "MI",
                             27: "MN",
                             28: "MS",
                             29: "MO",
                             30: "MT",
                             31: "NE",
                             32: "NV",
                             33: "NH",
                             34: "NJ",
                             35: "NM",
                             36: "NY",
                             37: "NC",
                             38: "ND",
                             39: "OH",
                             40: "OK",
                             41: "OR",
                             42: "PA",
                             44: "RI",
                             45: "SC",
                             46: "SD",
                             47: "TN",
                             48: "TX",
                             49: "UT",
                             50: "VT",
                             51: "VA",
                             53: "WA",
                             54: "WV",
                             55: "WI",
                             56: "WY",
                             72: "PR"})
 
    df.rename(columns={"agep": "age",
                       "pwgtp": "weight",
                       "sex": "sex_id",
                       "relp":"relationship"
                      }, inplace=True)
    df.drop(["hisp","st","racnum"], axis=1, inplace=True)
    
    #weight population distr. from each year equally
    df["pop_year_weight"] = df.groupby('year')['weight'].transform('sum') / df.weight.sum()
    df["weight"] = df.weight * df.pop_year_weight
    df.drop(columns=['pop_year_weight'], inplace=True)
    
    #get population counts per sex/age/race/ethnicity/hhgq
    if include_vars==None:
        order_vars = ['state','sex_id','age'] + all_races
    else:
        order_vars = ['state','sex_id','age'] + include_vars + all_races
    df['pop_count'] = df.groupby(order_vars)['weight'].transform('sum')
    df.drop(columns=['weight','serialno','year'], inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[order_vars + ['race_count','pop_count']]
    
    df['pop_count'] = np.round(df.pop_count)
    df = df.sort_values(df.columns.tolist())
    
    return df


def format_acs(input_df):
    ''' merge on age groups and calculate proportion of age group each age comprises
    '''
    df = input_df.copy(deep=True)

    # remove everyone in gq
    df['hhgq'] = ['gq' if (relp in [16, 17]) else 'hh' for relp in df.relationship]

    # add on age bin columns corresponding to decennial data
    age_starts = [0,5,10,15,18,20,21,22,25,30,35,40,45,50,55,60,62,65,67,70,75,80,85,120]
    df['age_bins'] = pd.cut(df.age, bins = age_starts, right=False, include_lowest=False, retbins=False)
    df['age_start'] = [i.left for i in df.age_bins]
    df['age_end'] = [i.right-1 for i in df.age_bins]

    # add race with 7 bins
    df['race7'] = 'multi'
    for col in all_races:
        df.loc[(df[col]==1) & (df.race_count==1),'race7'] = col

    # # calculate proportion of age bin each age comprises
    # group_vars = ['state','race7','sex_id','age_start','age_end','hispanic','hhgq']
    # df['pop_denom'] = df.groupby(group_vars)['pop_count'].transform('sum')
    # df['pop_proportion'] = df['pop_count'] / df['pop_denom']

    #clean up
    # df.drop(columns=['age_bins','pop_denom'], inplace=True)
    df.drop(columns=['age_bins'], inplace=True)

    #check
    #     df.groupby(['sex_id','age_start','age_end','hispanic','hhgq']).sum().pop_proportion.unique()

    return df