import os, pickle
import pandas as pd, numpy as np

## set globals -----------------------------------------------------------------

location_cols = ['STATE', 'COUNTY', 'TRACT', 'BLKGRP', 'BLOCK']

underlying_dir = '/ihme/scratch/users/beatrixh/underlying_pop/best/'
outputs_dir = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/'

fips_dict = {'al': 1, 'ak': 2, 'ar': 4, 'az': 5, 'ca': 6, 'co': 8, 
'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 
'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 
'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 
'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 
'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 
'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 
'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}

states = list(fips_dict.keys())

## get locs to rerun for final jobs --------------------------------------------

def determine_final_reruns(state):    
	outputs = find_missings(state,'final')
	if outputs.shape[0] == 0:
		return pd.DataFrame()
	outputs['state'] = fips_dict[state]
	outputs['args'] = outputs.state.astype(str) + ' ' + outputs.county + ' ' + outputs.tract
	return outputs

## get locs to rerun for underlying structure ----------------------------------

def determine_reruns(state):
	# pull in locations missing output files
	outputs = find_missings(state, 'underlying')
	outputs['TRACT'] = outputs.tract.astype(float)
	outputs['COUNTY'] = outputs.county.astype(float)

	# read in raw data
	path = '/ihme/scratch/users/beatrixh/decennial_census_2010/{}_decennial_2010/{}2010ur1_all_vars.CSV'.format(state,state)
	usecols = location_cols + ['P0010001']
	df = pd.read_csv(path, usecols=usecols)
	df = df[df.BLOCK.notna()]

	# find tracts for which we don't have output files, which DO have pop > 0
	counts = outputs.merge(df, how = 'left', on = ['COUNTY','TRACT'])
	rerun = counts.loc[counts.P0010001 > 1,['county','tract']].drop_duplicates()

	# format for output
	rerun['state'] = fips_dict[state]
	rerun['args'] = rerun.state.astype(str) + ' ' + rerun.county + ' ' + rerun.tract

	return rerun

## eval fns --------------------------------------------------------------------

def tally_underlying_locs(state):
	"""
	determine all state/county/tracts, for a given state, with an output file
	from the gen_underlying_str script
	"""
	state_underlying_str_path = '/ihme/scratch/users/beatrixh/underlying_pop/best/{}'.format(state)

	if not os.path.exists(state_underlying_str_path):
		return pd.DataFrame()
	output = pd.DataFrame([i.split('_') for i in os.listdir(state_underlying_str_path)], columns = ['state','county','tract'])

	output.state = fips_dict[state]
	output.county = [i[-3:] for i in output.county]
	output.tract = [i[5:-4] for i in output.tract]

	output = output.sort_values(by = output.columns.tolist())
	output.reset_index(inplace = True, drop = True)

	return output

def tally_final_locs(state):
	"""
	determine all state/county/tracts, for a given state, with an output file
	from the gen_single_yr estimates script
	"""
	state_underlying_str_path = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/{}'.format(state)

	if not os.path.exists(state_underlying_str_path):
		return pd.DataFrame()
	output = pd.DataFrame([i.split('_') for i in os.listdir(state_underlying_str_path)], columns = ['state','county','tract'])

	output.state = fips_dict[state]
	output.county = [i[-3:] for i in output.county]
	output.tract = [i[5:-4] for i in output.tract]

	output = output.sort_values(by = output.columns.tolist())
	output.reset_index(inplace = True, drop = True)

	return output


def tally_target(state):
	"""
	output a df of all state/tract/counties in a given state
	"""
	if type(state)!=list:
		state = [state]
	county_dict = load_county_dicts(state)
	tract_dict = load_tract_dicts(state)

	target = pd.DataFrame(columns = ['state','county','tract'])
	r = 0
	for key in tract_dict.keys():
		for v in tract_dict[key]:
			target.loc[r] = fips_dict[key[0]], key[1], v
			r +=1

	target = target.sort_values(by = target.columns.tolist())

	return target.reset_index(drop = True)

def find_missings(state, which = 'underlying'):
	"""
	for either the underlying_str script or single_yr script:
	determine which tracts are missing for a given state
	"""

	#pull data
	if which=='underlying':
		output = tally_underlying_locs(state)
		target = tally_target(state)
	elif which=='final':
		output = tally_final_locs(state)
		target = tally_underlying_locs(state)
	else:
		raise Exception("'which' must be in ['underlying','final']")

	#make sure data nonempty
	if output.shape[0]==0:
		print("no outputs for {}, {}".format(state, which))
		return pd.DataFrame()
	if target.shape[0]==0:
		print("no inputs for {}, {}".format(state, which))

	#find targets not hit   
	missings = np.unique([county for county in target.county if county not in output.county])
	which_tracts = pd.DataFrame(columns = ['county','tract'])
	r = 0
	for county in missings:
		m = [i for i in target[target.county==county].tract.tolist() if i not in output[output.county==county].tract.tolist()]
		for tract in m:
			which_tracts.loc[r] = county, tract
			r +=1
	return which_tracts

## import fns ------------------------------------------------------------------

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

