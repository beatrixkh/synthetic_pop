import pandas as pd, numpy as np, pickle
from new_process_decennial import add_geoid

# globals
files_dir = '/ihme/scratch/users/beatrixh/decennial_census_2010'

def main(state):
	"""
	Input: state as two-letter abbrev
	Output: dataframe containing all blocks associated with pop==0 tracts in the input state
	"""

	df = find_pop_zero_tracts(state)

	save_files(df, state)

def find_pop_zero_tracts(state):   
	## read in file
	state = state.lower()
	path = files_dir + '/{}_decennial_2010/{}2010ur1_all_vars.CSV'.format(state, state)
	df = pd.read_csv(path, usecols = ['STATE','COUNTY','TRACT','BLOCK','P0010001'])

	## select the block-level partition of the data
	df = df[df.BLOCK.notna() & (df.BLOCK!=' ')]

	## format cols
	df.STATE = [('0' + str(i))[-2:] for i in df.STATE]
	df.COUNTY = [('00' + str(int(i)))[-3:] for i in df.COUNTY]
	df.TRACT = [('00000' + str(int(i)))[-6:] for i in df.TRACT]
	df.BLOCK = [('000' + str(int(i)))[-4:] for i in df.BLOCK]

	## df = add_geoid(df) # sometimes results in an int, i believe

	tract_level = df.groupby(['STATE','COUNTY','TRACT']).sum().reset_index()

	pop_zero_tracts = tract_level[tract_level.P0010001==0]
	pop_zero_tracts = pop_zero_tracts.merge(df, on = ['STATE','COUNTY','TRACT'], how = 'left')

	assert(pop_zero_tracts.P0010001_y.sum()==0), 'ERROR: grabbed non-zero population blocks'

	return pop_zero_tracts[['STATE','COUNTY','TRACT','BLOCK']]

def save_files(df, state):

	path = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/zero_pop_tracts/state_{}_geoids.csv'.format(state)
	df.to_csv(path, index = False)

if __name__=="__main__":
	import sys
	import argparse
	import cProfile
	parser = argparse.ArgumentParser()
	parser.add_argument("state", help="", type=str)
	args = parser.parse_args()
	main(args.state)