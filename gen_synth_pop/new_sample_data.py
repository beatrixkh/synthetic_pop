import pandas as pd, numpy as np
import warnings

def generate_single_year_df(input_df, all_race_age_distr):
    ''' takes in a df with geoid/sex_id/age-bins/race-bins
        outputs df with geoid/sex_id/single-year-ages/detailed-race/ethnicity/relationship
    '''
    # subset to rows of interest / rows with positive population
    input_df = input_df[input_df.pop_count>0]

    # add vals
    all_race_age_distr = add_id_var(all_race_age_distr)
    
    df = pd.DataFrame()
    for i, row in input_df.iterrows():
        new_rows = sample_data(row, all_race_age_distr)
        df = df.append(new_rows)
    
    #split id columns into individual columns
    all_races = ['racaian', 'racasn', 'racblk', 'racnhpi', 'racsor', 'racwht']
    new_cols = all_races + ['age', 'relationship']
    df[new_cols] = df.ids.str.split('_', expand=True).iloc[:,:-1]
    
    #convert from strings to ints
    for col in all_races:
        df[col] = df[col].str[-1:].astype(int)
    df['relationship'] = df['relationship'].str[12:].astype(int)
    df['age'] = df['age'].str[3:].astype(int)
    
    
    df = df.drop(columns=['ids'])
    #create person-weight var
    df['pweight'] = 1
    
    # test
    assert(input_df.pop_count.sum()==df.shape[0]), 'Total population mismatch'
    assert(input_df.geoid.nunique()==df.geoid.nunique()), 'Geoid mismatch'
    
    return df

def add_id_var(input_df, id_vars = ['age','relationship']):
    df = input_df.copy(deep=True)
    
    ## standardize age and relp length, if present
    if 'age' in id_vars:
        df.age = [('00' + str(i))[-3:] for i in df.age]
    if 'relationship' in id_vars:
        df.relationship = [('0' + str(i))[-2:] for i in df.relationship]

    all_races = ['racaian', 'racasn', 'racblk', 'racnhpi', 'racsor', 'racwht']
    all_vars = all_races + id_vars
    df['ids'] = ''
    for v in all_vars:
        df['ids'] = df['ids'] + v + df[v].astype(str) + '_'

    if 'age' in id_vars:
        df.age = [int(i) for i in df.age]
    if 'relationship' in id_vars:
        df.relationship = [int(i) for i in df.relationship]

    return df


def sample_data(row, person_df):
    """
    Input: a row of data from the underlying age-sex-race-ethnicity-hhgq structure
    Ouput: a row of data with specific age-sex-race-ethnicity-relps
            assigned to each individ from person_df distribution
    """   
    # clean indices
    person_df.reset_index(drop=True, inplace=True)

    # add hhgq var
    person_df['hhgq'] = ['gq' if relp in [16,17] else 'hh' for relp in person_df.relationship]

    # get specific vals
    size = np.int(row.pop_count)
    sex = row.sex_id
    age_bin = row.age_start
    race = row.race
    hisp = row.hispanic
    hhgq = row.hhgq
    
    #debugging
    # print(row)
    
    # map vals to selection vectors
    sex_req = person_df.sex_id==sex
    age_req = person_df.age_start==age_bin
    eth_req = person_df.hispanic==hisp
    hhgq_req = person_df.hhgq==hhgq
    if race=='multi':
        # if multiracial, we'll included all versions of multiracial
        race_req = person_df.race_count > 1
    else:
        #otherwise, subset to the specific one-race-alone
        race_req = (person_df[race]==1) & (person_df.race_count==1)
    
    tracker = 0
    # get corresponding vals & probabilities
    vals = person_df[sex_req & eth_req & race_req & hhgq_req & age_req].ids.tolist()
    probs = person_df[sex_req & eth_req & race_req & hhgq_req & age_req].pop_count.tolist()
    
    # if person_df doesn't have enough detail to be able to assign vals/probs, stop matching on as many vars:  
    if vals==[]: #stop matching on sex
        vals = person_df[eth_req & race_req & hhgq_req & age_req].ids.tolist()
        probs = person_df[eth_req & race_req & hhgq_req & age_req].pop_count.tolist() 
        tracker += 1
        
        if vals==[]: #stop matching on ethnicity
            vals = person_df[race_req & hhgq_req & age_req].ids.tolist()
            probs = person_df[race_req & hhgq_req & age_req].pop_count.tolist()     
            tracker += 1
            
            if vals==[]: #stop matching on race
                vals = person_df[hhgq_req & age_req].ids.tolist()
                probs = person_df[hhgq_req & age_req].pop_count.tolist()
                tracker += 1

                #reassign race
                all_races = ['racaian', 'racasn', 'racblk', 'racnhpi', 'racsor', 'racwht']
                def reassign_race(old_ids):
                    non_race_ids = old_ids[49:]
                    choose = np.random.choice(person_df[race_req].index) #todo: for multiracial, weight this properly
                    race_assignments = person_df[all_races].iloc[choose].tolist()
                    race_encoding = [i + str(j) for (i,j) in zip(all_races,race_assignments)]
                    race_encoding = '_'.join(race_encoding)

                    return race_encoding + non_race_ids
                vals = [reassign_race(val) for val in vals]                

                if (vals==[]) & (hhgq!='hh'): #stop matching on hhgq
                    tracker += 1
                    #get baseline vals
                    vals = person_df[age_req].ids.tolist()
                    probs = person_df[age_req].pop_count.tolist()
                    #reassign race
                    vals = [reassign_race(val) for val in vals]
                    
                    #reassign relps as needed
                    def reassign_gq(old_id):
                        if hhgq=='inst':
                            assign = '16_'
                        elif hhgq=='noninst':
                            assign = '17_'
                        else:
                            warnings.warn("hhgq other than hh, inst, noninst!")
                        return old_id[:-3] + assign
                    vals = [reassign_gq(val) for val in vals]

                elif (vals==[]) & (hhgq=='hh'):
                    raise Exception("For this state and age, you don't have a detailed enough dist")
        
    # debugging
    # print("tracker + {}".format(tracker))
    
    #normalize probs
    probs = [i/sum(probs) for i in probs]
    
    ids = np.random.choice(a=vals, p=probs, size=size)
    
    return pd.DataFrame({'geoid':row.geoid, 'sex_id': sex, 'hispanic' : hisp, 'hhgq': hhgq, 'ids': ids})    