from binning_fns import *


# read in data -----------------------------------------------------------------

## TODO: make this generic
path = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/co/state8_county001_tract007801.csv'
df = pd.read_csv(path)

# format data ------------------------------------------------------------------
df = add_race_vars(df)
df = add_bin_vars(df)

# checks -----------------------------------------------------------------------

def check_count_bins():
	"""
	make sure baseline n_bins correctly counted for each binning scenario
	"""

	def correct_scalars_used(age = 'age', relp = 'relationship', race = 'race_detail'):
			#this is total possible bins
			target = int(count_bins_per_geoid())

			#grab correction factors
			age_scalar = bin_dict[('age', 'age')] / bin_dict[('age', age)]
			relp_scalar = bin_dict[('relp', 'relationship')] / bin_dict[('relp', relp)]    
			race_scalar = bin_dict[('race','race_detail')] / bin_dict[('race',race)]

			#calculate specific binning scenario
			test = count_bins_per_geoid(age = age, relp = relp, race = race)

			#this should be zero
			return target - int(test * age_scalar * relp_scalar * race_scalar)


	test = 0
	for age in ['age','a1','a2','a3']:
		for race in ['race_detail','multiracial3','multiracial4']:
			for relp in ['relationship','relp_reduced']:
			test += check_count_bins(age, relp, race)

	assert(test==0), 'ERROR: Something in the count_bins_per_geoid() is miscalculating'