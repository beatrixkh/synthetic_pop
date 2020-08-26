from new_process_decennial import *

## set globals
hispanic_totals_by_race = ['P00500' + str(i) for i in range(10,18)]
sex_by_age_hispanic = ['P012H0' + ('0' + str(i))[-2:] for i in range(2,50)]
hispanic_race_labels = ['all_hispanic','white_hispanic','black_hispanic','aian_hispanic','asian_hispanic','nhpi_hispanic','otherrace_hispanic','multi_hispanic']


def pull_race_ethnicity(state, county, tract, path):
	state, county = float(state), float(county)
	
	if tract!=None:
		tract = float(tract)   

	## pull race-ethnicity counts
	race_ethnicity_props_df = pd.read_csv(input_dir + path, usecols = location_cols + hispanic_totals_by_race)

	## subset to one partition of data
	race_ethnicity_props_df = race_ethnicity_props_df[(race_ethnicity_props_df.BLOCK.notna()) & (race_ethnicity_props_df.BLOCK!=' ')]

	## subset to loc of interest
	race_ethnicity_props_df = race_ethnicity_props_df[(race_ethnicity_props_df.STATE==state) &
												(race_ethnicity_props_df.COUNTY==county)]
	if tract!=None:
		race_ethnicity_props_df = race_ethnicity_props_df[(race_ethnicity_props_df.TRACT==tract)]

	## cast to long
	race_ethnicity_props_df = race_ethnicity_props_df.melt(id_vars = location_cols,
		value_vars = hispanic_totals_by_race, value_name= 'pop_count', var_name = 'race_ethnicity')

	## rename race-ethnicity count columns to something intelligible
	hispanic_race_label_dict = {var:label for (var,label) in zip(hispanic_totals_by_race,hispanic_race_labels)}
	race_ethnicity_props_df['race_ethnicity'] = race_ethnicity_props_df.race_ethnicity.map(hispanic_race_label_dict)

	## format
	race_ethnicity_props_df = add_geoid(race_ethnicity_props_df)
	race_ethnicity_props_df = race_ethnicity_props_df.sort_values(by=['geoid','race_ethnicity'])

	return race_ethnicity_props_df

def pull_age_sex_ethnicity(state, county, tract, path):
	state, county = float(state), float(county)
	
	if tract!=None:
		tract = float(tract)      

	## pull age/sex ethnicity counts, subset to one partition of the data
	hispanic_age_sex = pd.read_csv(input_dir + path, usecols = location_cols + sex_by_age_hispanic)
	hispanic_age_sex = hispanic_age_sex[(hispanic_age_sex.BLOCK.notna()) & (hispanic_age_sex.BLOCK!=' ')]

	## subset to loc of interest
	hispanic_age_sex = hispanic_age_sex[(hispanic_age_sex.STATE==state) & (hispanic_age_sex.COUNTY==county)]
	if tract!=None:
		hispanic_age_sex = hispanic_age_sex[(hispanic_age_sex.TRACT==tract)]

	## cast to long, add age and sex cols
	hispanic_age_sex = hispanic_age_sex.melt(id_vars = location_cols, value_vars = sex_by_age_hispanic, value_name = 'pop_count')
	hispanic_age_sex = label_census_age_sex(hispanic_age_sex)

	# drop the male and female all-age vars
	hispanic_age_sex = hispanic_age_sex[~(hispanic_age_sex.variable.str[-2:].isin(['02','26']))]

	# format
	hispanic_age_sex = add_geoid(hispanic_age_sex)
	hispanic_age_sex['sex_ages'] = ['m_' + str(i) if j==1 else 'f_' + str(i) for (i,j) in zip(hispanic_age_sex.age_start,hispanic_age_sex.sex_id)]
	hispanic_age_sex = hispanic_age_sex.sort_values(by=['geoid','sex_id','age_start'])

	return hispanic_age_sex