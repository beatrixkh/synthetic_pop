import pandas as pd, numpy as np

## set globals -----------------------------------------------------------------

input_dir = '/ihme/scratch/users/beatrixh/'
location_cols = ['STATE', 'COUNTY', 'TRACT', 'BLKGRP', 'BLOCK']

race_alone_totals = ['P003000' + str(i) for i in range(1,9)]

sex_by_age = ['P012000' + str(i) for i in range(1,9)] + ['P01200' + str(i) for i in range(10,50)] #all races
sex_by_age_white_alone =['P012A00' + str(i) if i<10 else 'P012A0' + str(i) for i in range(1,50)]
sex_by_age_black_alone = ['P012B00' + str(i) if i<10 else 'P012B0' + str(i) for i in range(1,50)]
sex_by_age_aian_alone = ['P012C00' + str(i) if i<10 else 'P012C0' + str(i) for i in range(1,50)]
sex_by_age_asian_alone = ['P012D00' + str(i) if i<10 else 'P012D0' + str(i) for i in range(1,50)]
sex_by_age_nhpi_alone = ['P012E00' + str(i) if i<10 else 'P012E0' + str(i) for i in range(1,50)]
sex_by_age_otherrace_alone = ['P012F00' + str(i) if i<10 else 'P012F0' + str(i) for i in range(1,50)]
sex_by_age_mixed_race = ['P012G00' + str(i) if i<10 else 'P012G0' + str(i) for i in range(1,50)]

sex_by_age_detailed_all = [sex_by_age_white_alone, sex_by_age_black_alone, sex_by_age_aian_alone, sex_by_age_asian_alone,sex_by_age_nhpi_alone,sex_by_age_otherrace_alone,sex_by_age_mixed_race]
sex_by_age_detailed_all = [item for sublist in sex_by_age_detailed_all for item in sublist]


def pull_geo_race_age_sex(path, state, county, tract = None):
	"""
	for a given location, pull in age-sex-race structure from 2010 decennial
	"""
	# read in data and subset to one partition/sumlev
	df = pd.read_csv(input_dir + path, usecols = location_cols + sex_by_age_detailed_all)
	df = df[(df.BLOCK.notna()) & (df.BLOCK!=' ')]

	# subset to loc of interest and cast to long
	df = df[(df.STATE==float(state)) & (df.COUNTY==float(county))]
	if tract!=None:
		df = df[df.TRACT.astype(float)==float(tract)]
	df = df.melt(id_vars = location_cols, value_vars = sex_by_age_detailed_all)

	# remove both-sex all-age
	df = df[~(df.variable.str[-2:].isin(['01']))]

	# add age, sex, and geoid cols
	df = label_census_age_sex(df)
	df = add_geoid(df)

	# add race
	decennial_race_dict = {'A':'white','B':'black','C':'aian','D':'asian','E':'nhpi','F':'otherrace','G':'multi'}
	df['race'] = df.variable.str[4:5]
	df['race'] = df.race.map(decennial_race_dict)

	# remove sex-specific all-age
	df = df[~(df.variable.str[-2:].isin(['01','02','26']))] # the label_census_age_sex required 02 and 26

	# clean
	df = df.rename(columns={'value':'pop_count'})
	df = df.drop(columns=['var_key','variable'])

	df['sex_age'] = ['m_' + str(i) if j==1 else 'f_' + str(i) for (i,j) in zip(df.age_start,df.sex_id)]

	return df
	

def add_geoid(input_df):
	""" input shape: has ['STATE','COUNTY','TRACT','BLOCK'] with dtypes [int,float,float,float]
	"""

	df = input_df.copy(deep=True)

	lower_cols = ['state', 'county', 'tract', 'block']
	for col in lower_cols:
		if col in df.columns.tolist():
			if col.upper() not in df.columns.tolist():
				df.rename(columns={col:col.upper()}, inplace = True)
	            
	df['geoid'] = [('0' + str(i))[-2:] for i in df.STATE]
	
	if 'COUNTY' in df.columns:
		df['COUNTY'] = df.COUNTY.astype(float).astype(int).astype(str)
		df['COUNTY'] = ['00' + i for i in df.COUNTY]
		df['COUNTY'] = df.COUNTY.str[-3:]
		df['geoid'] = df['geoid'] + df.COUNTY.astype(str)

	if 'TRACT' in df.columns:
		df['TRACT'] = df.TRACT.astype(float).astype(int).astype(str)
		df['TRACT'] = ['00000' + i for i in df.TRACT]
		df['TRACT'] = df.TRACT.str[-6:]
		df['geoid'] = df['geoid'] + df.TRACT.astype(str)
		assert(np.unique([len(i) for i in df.geoid]).tolist()==[11]), "some of the geoids aren't len 15"
	    
	# df['BLKGRP'] = df.BLKGRP.astype(int).astype(str)

	if 'BLOCK' in df.columns:
		df['BLOCK'] = df.BLOCK.astype(float).astype(int).astype(str)
		df['BLOCK'] = ['000' + i for i in df.BLOCK]
		df['BLOCK'] = df.BLOCK.str[-4:]
		df['geoid'] = df['geoid'] + df.BLOCK.astype(str)
		assert(np.unique([len(i) for i in df.geoid]).tolist()==[15]), "some of the geoids aren't len 15"

	return df

def label_census_age_sex(input_df):
	""" This is for the 5-ish year age bins and vars of the form 'P012[A/B/.../G]0'
	"""
	df = input_df.copy(deep=True)
	df['var_key'] = df.variable.str[:4] + df.variable.str[5:]
	if df.var_key.nunique()!=48:
		print(df.var_key.unique())
		raise Exception("Oops; variable col doesn't contain expected vals")
		
	df['var_key'] = df.var_key.str[-2:].astype(int)


	# map census var names into something readable
	age_start = [0,5,10,15,18,20,21,22,25,30,35,40,45,50,55,60,62,65,67,70,75,80,85]
	age_end = [i-1 for i in age_start[1:]] + [115]

	age_start = [0] + age_start
	age_end = [115] + age_end

	age_start_dict = {}
	age_end_dict = {}
	sex_dict = {}

	counter = 0
	for i in np.arange(2,26):
	    age_start_dict[i] = age_start[counter]
	    age_end_dict[i] = age_end[counter]
	    sex_dict[i] = 1
	    counter += 1
	    
	counter = 0
	for i in np.arange(26,50):
	    age_start_dict[i] = age_start[counter]
	    age_end_dict[i] = age_end[counter]
	    sex_dict[i] = 2
	    counter += 1

	df['age_start'] = df.var_key.map(age_start_dict)
	df['age_end'] = df.var_key.map(age_end_dict)
	df['sex_id'] = df.var_key.map(sex_dict)
	return df


def label_census_races(input_df):
	"""
	add dummy variables for 7 race categories, the sex-by-age race vars
	"""
	df = input_df.copy()

	## create dict for converting labels
	# race_labels = ['racwht','racblk','racaian','racasn','racnhpi','racsor','multiracial']
	# census_var_race_dict = {census_var:race for (census_var,race) in zip(['A','B','C','D','E','F','G'],race_labels)}
	decennial_race_dict = {'A':'white','B':'black','C':'aian','D':'asian','E':'nhpi','F':'otherrace','G':'multi'}
	# create column identifying races
	df['race_category'] = df.variable.str[4:5]

	# map to readable labels
	df['race_category'] = df.race_category.map(census_var_race_dict)

	# convert to dummy vars
	# df = pd.get_dummies(df, columns=['race_category'], prefix = '', prefix_sep = '')

	return df


def expand_tabbed_to_person(df, pweight):
	# starting total
	n = int(df[pweight].sum())

	# subset to rows with sims
	df = df.loc[df[pweight] > 0]

	# convert to lists
	df[pweight] = [[1] * int(i) for i in df[pweight]]

	# expand to person data
	df = df.explode(pweight)

	assert(df[pweight].sum()==n), "Oops; you've gained or lost sims"

	return df