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
        gq_race_df = gq_race_df[(gq_race_df.TRACT.astype(float)==float(tract))]

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
        gq_sex_age_df = gq_sex_age_df[(gq_sex_age_df.TRACT.astype(float)==float(tract))]

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
    gq_sex_age_df.pop_count = gq_sex_age_df.pop_count.astype(int)

    gq_sex_age_df = add_geoid(gq_sex_age_df)
    
    return gq_sex_age_df

def find_gq(df, gq_race_df, gq_sex_age_df):

  census_blocks = gq_race_df.geoid.unique().tolist()
  sex_ages = gq_sex_age_df.sex_age.unique().tolist()
  races = gq_race_df.race.unique().tolist()
  gq_types = ['inst','noninst']

  tract_geoids = gq_sex_age_df.geoid.unique().tolist()

  gq_model = ConcreteModel()
  gq_model.x = Var(races, sex_ages, census_blocks, gq_types, within=NonNegativeIntegers)


  # require race distribution correct
  gq_model.correct_race_hist = ConstraintList()
  for k in census_blocks:
    for i in races:
      for l in gq_types:
        count = gq_race_df[(gq_race_df.geoid==k) & (gq_race_df.race==i) & (gq_race_df.gq_type==l)].pop_count.values[0]
        gq_model.correct_race_hist.add(
          sum(gq_model.x[i,j,k,l] for j in sex_ages) == count
          )

  # require sex/age distribution correct
  gq_model.correct_sex_age_hist = ConstraintList()
  for tract_geoid in tract_geoids:
    geoids = gq_race_df[(gq_race_df.tract_geoid==tract_geoid)].geoid.unique().tolist()
    for j in sex_ages:
      for l in gq_types:
        count = gq_sex_age_df[(gq_sex_age_df.geoid==tract_geoid) & (gq_sex_age_df.sex_age==j) &  (gq_sex_age_df.gq_type==l)].pop_count.values[0]
        gq_model.correct_sex_age_hist.add(
          sum(gq_model.x[i,j,k,l] for i in races for k in geoids) == count
          )


  # for each block/race/sex/age, require that gq <= gq + non-gq
  ceiling_df = df.copy()
  ceiling_df['sex2_age3'] = [0 if age_end < 18 else 18 if age_end < 65 else 65 for age_end in ceiling_df.age_end]
  ceiling_df['sex2_age3'] = ['f_' + str(i) if j==2 else 'm_' + str(i) for (i,j) in zip(ceiling_df.sex2_age3,ceiling_df.sex_id)]
  ceiling_df = ceiling_df[['geoid','sex2_age3','race','pop_count']].groupby(['geoid','sex2_age3','race']).sum().reset_index()

  gq_model.pop_count_ceiling = ConstraintList()
  for i in races:
    for j in sex_ages:
        for k in census_blocks:
          ceil = ceiling_df[(ceiling_df.race==i) & (ceiling_df.sex2_age3==j) & (ceiling_df.geoid==k)].pop_count.values[0]
          gq_model.pop_count_ceiling.add(
            sum(gq_model.x[i,j,k,l] for l in gq_types) <= ceil
            )

  #solve
  gq_model.obj = Objective(expr = 0)
  opt = SolverFactory('cbc')  # installed from conda https://anaconda.org/conda-forge/coincbc
  gq_results = opt.solve(gq_model)

  #pull results into an array
  X = np.zeros((len(races), len(sex_ages), len(census_blocks), len(gq_types)))
  for i in range(len(races)):
    for j in range(len(sex_ages)):
      for k in range(len(census_blocks)):
        for l in range(len(gq_types)):
          X[i,j,k,l] = value(gq_model.x[races[i],sex_ages[j],census_blocks[k],gq_types[l]])
  
  # move into a 2D dataframe
  gq_race_age_df = pd.DataFrame()
  for k in census_blocks:
    for l in gq_types:
      X_0 = np.zeros((len(races), len(sex_ages)))
      for i in range(len(races)):
        for j in range(len(sex_ages)):
          X_0[i,j] = value(gq_model.x[races[i],sex_ages[j],k,l])
      df_0 = pd.DataFrame(X_0, index = races, columns = sex_ages).reset_index().melt(id_vars = 'index', value_vars = sex_ages)
      df_0['geoid'] = k
      df_0['gq_type'] = l
      gq_race_age_df = gq_race_age_df.append(df_0)

  #format
  gq_race_age_df['sex_id'] = [1 if i[0]=='m' else 2 for i in gq_race_age_df.variable.str.split('_')]
  gq_race_age_df['age_start'] = [np.int(i[1]) for i in gq_race_age_df.variable.str.split('_')]
  gq_race_age_df['age_end'] = [17 if i==0 else 64 if i==18 else 115 for i in gq_race_age_df.age_start]

  gq_race_age_df = gq_race_age_df.rename(columns={'value':'pop_count','index':'race'})

  return gq_race_age_df