from new_process_decennial import *

## set globals -----------------------------------------------------------------
gq_tract_sex_age_dict = {'PCT0210001': 'total_gq',
                         'PCT0210002': 'male_gq',
                         'PCT0210003': 'male_0_18',
                         'PCT0210004': 'male_inst_0_18',
                         'PCT0210023': 'male_noninst_0_18',
                         'PCT0210035': 'male_18_64',
                         'PCT0210036': 'male_inst_18_64',
                         'PCT0210055': 'male_noninst_18_64',
                         'PCT0210067': 'male_65_120',
                         'PCT0210068': 'male_inst_65_120',
                         'PCT0210087': 'male_noninst_65_120',
                         'PCT0210099': 'female_gq',
                         'PCT0210100': 'female_0_18',
                         'PCT0210101': 'female_inst_0_18',
                         'PCT0210120': 'female_noninst_0_18',
                         'PCT0210132': 'female_18_64',
                         'PCT0210133': 'female_inst_18_64',
                         'PCT0210152': 'female_noninst_18_64',
                         'PCT0210164': 'female_65_120',
                         'PCT0210165': 'female_inst_65_120',
                         'PCT0210184': 'female_noninst_65_120'
                        }

gq_block_race_dict = {
    'white_gq':'P029A026',
    'white_inst':'P029A027',
    'white_noninst':'P029A028',
    'black_gq':'P029B026',
    'black_inst':'P029B027',
    'black_noninst':'P029B028',
    'aian_gq':'P029C026',
    'aian_inst':'P029C027',
    'aian_noninst':'P029C028',
    'asian_gq':'P029D026',
    'asian_inst':'P029D027',
    'asian_noninst':'P029D028',
    'nhpi_gq':'P029E026',
    'nhpi_inst':'P029E027',
    'nhpi_noninst':'P029E028',
    'otherrace_gq':'P029F026',
    'otherrace_inst':'P029F027',
    'ohterrace_noninst':'P029F028',
    'multi_gq':'P029G026',
    'multi_inst':'P029G027',
    'multi_noninst':'P029G028',
}

## block race
gq_tract_sex_age_vars = list(gq_tract_sex_age_dict.keys())
gq_block_race_vars = list(gq_block_race_dict.values())

# ------------------------------------------------------------------------------

def assign_gq(df, gq_race_age_df):
  assert(df.pop_count.max()==1), 'oops; is this tabbed data? convert to person-data first'

  pop_total = df.pop_count.sum()

  # subset to rows with sims in gq
  gq = gq_race_age_df[(gq_race_age_df.pop_count > 0)]

  # make sure df index is ordered
  df = df.reset_index(drop=True)

  # add hhgq col if nonexistant
  if 'hhgq' not in df.columns:
      df['hhgq'] = 'hh' 

  for typ in ['inst','noninst']:
    gq_df = gq[gq.gq_type==typ].reset_index(drop=True)
    if gq_df.shape[0]>0:
      for i, row in gq_df.iterrows():
          # grab settings
          race = row.race
          sex = row.sex_id
          age_start = row.age_start
          age_end = row.age_end
          g = row.geoid
          n = int(row.pop_count)

          # filter base df
          potential_sims = df[(df.race == race) & 
          (df.sex_id == sex) &
          (df.age_start >= age_start) & 
          (df.age_end <= age_end) & 
          (df.geoid==g)].index.tolist()

          # choose randomly
          gq_sims = np.random.choice(potential_sims, size = n, replace = False)

          # assign gq status
          df.loc[df.index.isin(gq_sims),'hhgq'] = typ

  assert(df.pop_count.sum()==pop_total), "Oops; you've lost or gained sims"

  return df

def pull_block_race(state, county, tract, path):
    gq_race_df = pd.read_csv(input_dir + path, usecols = location_cols + gq_block_race_vars)
    gq_race_df = gq_race_df[(gq_race_df.BLOCK.notna()) & (gq_race_df.BLOCK!=' ')]

    gq_race_df = gq_race_df[(gq_race_df.STATE==float(state)) & (gq_race_df.COUNTY==float(county))]
    if tract!=None:
        gq_race_df = gq_race_df[(gq_race_df.TRACT==float(tract))]

    gq_race_df = gq_race_df.melt(id_vars = location_cols, value_vars = gq_block_race_vars)

    decennial_race_dict = {'A':'white','B':'black','C':'aian','D':'asian','E':'nhpi','F':'otherrace','G':'multi'}
    decennial_gq_dict = {'26':'gq_all','27':'inst','28':'noninst'}

    gq_race_df['gq_type'] = gq_race_df.variable.str[-2:].map(decennial_gq_dict)
    gq_race_df['race'] = gq_race_df.variable.str[4:5].map(decennial_race_dict)

    gq_race_df = add_geoid(gq_race_df)
    gq_race_df['tract_geoid'] = gq_race_df.geoid.str[:11]
    gq_race_df = gq_race_df.rename(columns={'value':'pop_count'})

    return gq_race_df

def pull_sex_age_tract(state, county, tract, path):
    gq_sex_age_df = pd.read_csv(input_dir + path, usecols = location_cols + ['SUMLEV'] + gq_tract_sex_age_vars)
    gq_sex_age_df = gq_sex_age_df[(gq_sex_age_df.SUMLEV==140) &
                                  (gq_sex_age_df.TRACT.notna()) & 
                                  (gq_sex_age_df.BLKGRP.isna()) & 
                                  (gq_sex_age_df.BLOCK.isna())]

    gq_sex_age_df = gq_sex_age_df[(gq_sex_age_df.STATE==float(state)) &
                                  (gq_sex_age_df.COUNTY==float(county))]
    if tract!=None:
        gq_sex_age_df = gq_sex_age_df[(gq_sex_age_df.TRACT==float(tract))]

    gq_sex_age_df = gq_sex_age_df.melt(id_vars = ['STATE','COUNTY','TRACT'], value_vars = gq_tract_sex_age_vars)

    gq_sex_age_df['sex_id'] = gq_sex_age_df.variable.map(gq_tract_sex_age_dict).str[0]
    gq_sex_age_df[gq_sex_age_df.sex_id!='t']
    gq_sex_age_df['sex_id'] = [1 if i=='m' else 2 for i in gq_sex_age_df.sex_id]

    gq_sex_age_df['age_sex_inst_key'] = gq_sex_age_df.variable.map(gq_tract_sex_age_dict)
    gq_sex_age_df['age_start'] = [i.split('_')[-2] for i in gq_sex_age_df.age_sex_inst_key]
    gq_sex_age_df['age_end'] = [i.split('_')[-1] for i in gq_sex_age_df.age_sex_inst_key]
    gq_sex_age_df['gq_type'] = [i.split('_')[1] for i in gq_sex_age_df.age_sex_inst_key]

    gq_sex_age_df = gq_sex_age_df[~(gq_sex_age_df.age_start.isin(['total','male','female']))]
    gq_sex_age_df = gq_sex_age_df[(gq_sex_age_df.gq_type.isin(['inst','noninst']))]

    gq_sex_age_df['sex_age'] = ['m_' + str(i) if j==1 else 'f_' + str(i) for (i,j) in zip(gq_sex_age_df.age_start,gq_sex_age_df.sex_id)]

    gq_sex_age_df = gq_sex_age_df.drop(columns=['age_sex_inst_key','variable'])
    gq_sex_age_df = gq_sex_age_df.rename(columns={'value':'pop_count'})
    
    gq_sex_age_df = add_geoid(gq_sex_age_df)
    
    return gq_sex_age_df