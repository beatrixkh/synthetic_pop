import pandas as pd, numpy as np, pickle

# globals
files_dir = '/ihme/scratch/users/beatrixh/decennial_census_2010'

def main(state):
	## read in file
	state = state.lower()
	path = files_dir + '/{}_decennial_2010/{}2010ur1_all_vars.CSV'.format(state, state)
	df = pd.read_csv(path, usecols = ['STATE','COUNTY','TRACT'])

	## grab complete nonredundant set of state-county-tracts
	df = df[(df.TRACT.notna()) & (df.COUNTY.notna()) & (df.STATE.notna())]
	df = df[(df.TRACT!=' ') & (df.COUNTY!=' ') & (df.STATE!=' ')].drop_duplicates()

	## format cols
	df.STATE = [('0' + str(i))[-2:] for i in df.STATE]
	df.COUNTY = [('00' + str(int(i)))[-3:] for i in df.COUNTY]
	df.TRACT = [('00000' + str(int(i)))[-6:] for i in df.TRACT]

	county_dict = {state : [county for county in df.COUNTY.unique()]}
	tract_dict = {(state,county):[tract for tract in df[df.COUNTY==county].TRACT.unique()] for county in df.COUNTY.unique()}

	def save_dict(obj, filename):
		path = '/ihme/scratch/users/beatrixh/fips_dicts/'
		with open(path + filename + '.pkl', 'wb') as f:
			pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

	# def load_obj(name ):
	# 	path = '/ihme/scratch/users/beatrixh/fips_dicts/'
	# 	 with open(path + name + '.pkl', 'rb') as f:
	# 		return pickle.load(f)

	county_dict_name = '{}_county_dict'.format(state)
	tract_dict_name = '{}_tract_dict'.format(state)

	save_dict(county_dict, county_dict_name)
	save_dict(tract_dict, tract_dict_name)

# if __name__=="__main__":
# 	import sys
# 	import argparse
# 	import cProfile
# 	parser = argparse.ArgumentParser()
# 	parser.add_argument('state', help='', type='str')
# 	main(args.state)

states = ['nc']

for state in states:
	print(state)
	main(state)