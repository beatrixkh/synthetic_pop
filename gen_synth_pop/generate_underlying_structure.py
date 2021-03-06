import os
import datetime

import numpy as np, pandas as pd
from pyomo.environ import *

from new_process_decennial import *
from add_ethnicity import *
from add_gq import *

def main(state, county, tract):

	fips_codes = {1: 'AL', 2: 'AK', 5: 'AR', 4: 'AZ', 6: 'CA', 
	8: 'CO', 9: 'CT', 10: 'DE', 11: 'DC', 12: 'FL', 13: 'GA', 
	15: 'HI', 16: 'ID', 17: 'IL', 18: 'IN', 19: 'IA', 20: 'KS', 
	21: 'KY', 22: 'LA', 23: 'ME', 24: 'MD', 25: 'MA', 26: 'MI', 
	27: 'MN', 28: 'MS', 29: 'MO', 30: 'MT', 31: 'NE', 32: 'NV', 
	33: 'NH', 34: 'NJ', 35: 'NM', 36: 'NY', 37: 'NC', 38: 'ND', 
	39: 'OH', 40: 'OK', 41: 'OR', 42: 'PA', 44: 'RI', 45: 'SC', 
	46: 'SD', 47: 'TN', 48: 'TX', 49: 'UT', 50: 'VT', 51: 'VA', 
	53: 'WA', 54: 'WV', 55: 'WI', 56: 'WY', 72: 'PR'}

	state_name = fips_codes[int(state)].lower()
	path = 'decennial_census_2010/{}_decennial_2010/{}2010ur1_all_vars.CSV'.format(state_name, state_name)

	#pull underlying geo/race/age/sex structure
	df = pull_geo_race_age_sex(path, state, county, tract)

	#grab some constants to check against
	male_check, fem_check = df[df.sex_id==1].pop_count.sum(), df[df.sex_id==2].pop_count.sum()

	#make sure positive int pop
	if male_check + fem_check == 0:
		print("Location {} {} {} has population zero".format(state, county, tract))
	
	else:

		## add ethnicity -----------------------------------------------------------

		#pull joint race/ethnicity distribution
		race_ethnicity_props_df = pull_race_ethnicity(state, county, tract, path)

		#pull joint ethnicity/age/sex distribution
		hispanic_age_sex =  pull_age_sex_ethnicity(state, county, tract, path)

		# optimize to get joint race-ethnicity-age-sex
		hispanic_age_sex_race = assign_hispanic(df, race_ethnicity_props_df, hispanic_age_sex)

		## subtract hispanic out from (hispanic+non-hispanic)
		df_with_ethnicity = subtract_hispanic_from_all(df, hispanic_age_sex_race)

		## add gq structure --------------------------------------------------------

		# pull gq-race distribution
		gq_race_df = pull_block_race(state, county, tract, path)

		# pull gq-sex/age distribution
		gq_sex_age_df = pull_sex_age_tract(state, county, tract, path)

		# optimize to get joint race-sex/age-gq distribution
		gq_race_age_df = find_gq(df, gq_race_df, gq_sex_age_df)

		# assign gq to extant structure
		df_final = assign_gq(df_with_ethnicity, gq_race_age_df)

		## checks ------------------------------------------------------------------

		assert(int(df_final[df_final.sex_id==1].pop_count.sum())==male_check), "male pop count didn't stay constant"
		assert(int(df_final[df_final.sex_id==2].pop_count.sum())==fem_check), "female pop count didn't stay constant"

		## format and save ---------------------------------------------------------
		race_label_map = {'multi':'multi','white':'racwht','asian':'racasn','black':'racblk','otherrace':'racsor','aian':'racaian','nhpi':'racnhpi'}
		df_final["race"] = df_final.race.map(race_label_map)

		# save to csv
		# first try to save to best dir
		best_dir = '/ihme/scratch/users/beatrixh/underlying_pop/best/'
		state_dir = best_dir  + state_name
		for d in [best_dir, state_dir]:
		    if not os.path.exists(d):
		        os.mkdir(d)
		if tract==None:
			save_name = '/state{}_county{}.csv'.format(state,county)
		else:
			save_name = '/state{}_county{}_tract{}.csv'.format(state,county,tract)

		# if this file doesn't already exist in best dir, save. otherwise, save to date-specific dir
		if not os.path.exists(state_dir + save_name):
			df_final.to_csv(state_dir + save_name, index=False)
		else:
			today = datetime.date.today()
			today_dir = '/ihme/scratch/users/beatrixh/underlying_pop/' + str(today) + '/'
			state_dir = today_dir + '/' + state_name
			for d in [today_dir, state_dir]:
				os.mkdir(d)
			df_final.to_csv(state_dir + save_name, index = False)    


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



def subtract_hispanic_from_all(df, hispanic_age_sex_race):
	df = df.merge(hispanic_age_sex_race, how = 'left',on = ['race', 'sex_age','geoid'])
	df['non_hispanic_pop_count'] = df.pop_count - df.hispanic_pop_count

	non_hispanic_df = df.drop(columns=['pop_count','hispanic_pop_count'])
	non_hispanic_df = non_hispanic_df.rename(columns = {'non_hispanic_pop_count':'pop_count'})
	non_hispanic_df['hispanic'] = 0

	hispanic_df = df.drop(columns=['pop_count','non_hispanic_pop_count'])
	hispanic_df = hispanic_df.rename(columns = {'hispanic_pop_count':'pop_count'})
	hispanic_df['hispanic'] = 1

	df_final = non_hispanic_df.append(hispanic_df)
	df_final = expand_tabbed_to_person(df_final, 'pop_count')

	return df_final.reset_index(drop=True)


def assign_hispanic(df, race_ethnicity_props_df, hispanic_age_sex):
    
    # define opt fn
    def run_opt(k):
        model = ConcreteModel()
        model.x = Var(races_hispanic, sex_ages, within=NonNegativeIntegers)

        # require that for each ethnicity, sex/ages sum correctly
        model.correct_race_hist = ConstraintList()

        for i in races_hispanic:
            count = race_ethnicity_props_df[(race_ethnicity_props_df.geoid==k) & (race_ethnicity_props_df.race_ethnicity==i)].pop_count.values[0]
            model.correct_race_hist.add(
                sum(model.x[i,j] for j in sex_ages) == count
                )

        # require that for each block and sex/age, race/ethnicity sum correctly
        model.correct_sex_age_hist = ConstraintList()

        for j in sex_ages:
            count = hispanic_age_sex[(hispanic_age_sex.geoid==k) & (hispanic_age_sex.sex_ages==j)].pop_count.values[0]
            model.correct_sex_age_hist.add(
                sum(model.x[i,j] for i in races_hispanic) == count
                )


        # require that for each block, sex/age, (hispanic) <= (hispanic + non-hispanic)
        model.pop_count_ceiling = ConstraintList()
        for j in sex_ages:
            for i in races_hispanic:
                ceil = df[(df.race==i[:-9]) & (df.sex_age==j) & (df.geoid==k)].pop_count.values[0]
                model.pop_count_ceiling.add(
                    model.x[i,j] <= ceil
                )

        model.obj = Objective(expr = 0)
        opt = SolverFactory('cbc')  # installed from conda https://anaconda.org/conda-forge/coincbc
        results = opt.solve(model)
        return model
    
    # get vars to loop thru
    census_blocks = df.geoid.unique()
    races_hispanic = hispanic_race_labels[1:]
    races = df.race.unique().tolist()
    sex_ages = hispanic_age_sex.sex_ages.unique().tolist()
    
    # initialize df
    hispanic_age_sex_race = pd.DataFrame()
    
    #solve for each block
    for k in census_blocks:
        X_0 = np.zeros((len(races_hispanic), len(sex_ages)))
        if race_ethnicity_props_df[race_ethnicity_props_df.geoid==k].pop_count.sum() > 0:
            model = run_opt(k)
            for i in range(len(races_hispanic)):
                for j in range(len(sex_ages)):
                    X_0[i,j] = value(model.x[races_hispanic[i],sex_ages[j]])

        df_0 = pd.DataFrame(X_0, index = races_hispanic, columns = sex_ages).reset_index().melt(id_vars = 'index', value_vars = sex_ages)
        df_0['geoid'] = k
        hispanic_age_sex_race = hispanic_age_sex_race.append(df_0)
        
    
    relabel = {'index':'race','variable':'sex_age','value':'hispanic_pop_count'}
    hispanic_age_sex_race.rename(columns=relabel, inplace=True)
    hispanic_age_sex_race['race'] = hispanic_age_sex_race.race.str[:-9]

    return hispanic_age_sex_race


if __name__=="__main__":
	import sys
	import argparse
	import cProfile
	parser = argparse.ArgumentParser()
	parser.add_argument("state", help="", type=int)
	parser.add_argument("county", help="", type=str)
	parser.add_argument("tract", help="", type=str)
	args = parser.parse_args()
	main(args.state, args.county, args.tract)


# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N test_ms28_161_950300 -l fthread=1 -l m_mem_free=10G -q all.q -l h_rt=02:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/pyomo_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/generate_underlying_structure.py 28 161 950300