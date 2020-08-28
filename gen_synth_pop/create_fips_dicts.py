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

# states = ['al', 'ak', 'ar', 'az', 'ca', 'co', 'ct', 'de', 'dc', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'pr']

# states = ['sc', 'or', 
# states = ['pa', 'wy', 'ri', 'tn', 'ut', 'pr', 
# states = ['pr', 'vt', 'nc', 
# states = ['ok', 
# states = ['tx', 
states = ['va', 'sd', 'wi', 'oh', 'wv', 'nd']
# fips_dict = {'al': 1, 'ak': 2, 'ar': 4, 'az': 5, 'ca': 6, 'co': 8, 'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}

for state in states:
	print(state)
	main(state)