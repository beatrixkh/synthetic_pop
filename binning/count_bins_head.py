from binning_fns import *

fips_dict ={'al': 1, 'ak': 2, 'ar': 5, 'az': 4, 'ca': 6, 'co': 8, 'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}

def main(state_name, county):

	state = fips_dict[state_name]

	county_dict = load_county_dicts([state_name])
	tract_dict = load_tract_dicts([state_name])

	tracts = tract_dict[(state_name,county)]

	print(f'number of tracts: {len(tracts)}')

	zero_pop_tracts = pull_zero_pop_tracts(state_name)

	n_his = pd.DataFrame()
	for tract in tracts:
		if (int(county), int(tract)) not in zero_pop_tracts:
			path = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/{}/state{}_county{}_tract{}.csv'.format(state_name, state, county, tract)
			df = pd.read_csv(path)
			sub = calc_n_hi_all_scenarios(df)

			n_his = n_his.append(sub)

	save_n_his(n_his, state_name, state, county)

if __name__=="__main__":
	import sys
	import argparse
	import cProfile
	parser = argparse.ArgumentParser()
	parser.add_argument("state_name", help="", type=str)
	parser.add_argument("county", help="", type=str)
	args = parser.parse_args()
	main(args.state_name, args.county)
