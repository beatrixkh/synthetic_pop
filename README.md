## Synthetic Pop ##
Creates synthetic population files for the 2010 US population

## Overview ##
**1. gen_synth_pop/swarm_underlying_structure.py** 

Launches (1) gen_synth_pop/generate_underlying_structure.py and 
(2) gen_synth_pop/new_sample_single_year_age_distribution.py. For each person in 
the 2010 Decennial Census, these scripts create a row. These people are saved in 
csv batches to the dir /ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/{state}/, 
with filename  "state{i}_county{j}_tract{k}.csv", where i, j, and k correspond 
to FIPS geoids.


**2. gen_synth_pop/swarm_pop_zero.py** 

Launches gen_synth_pop/identify_pop_zero_tracts/identify_pop_zero_tracts.py. For 
each tract containing zero people in the 2010 Decennial census, this creates a 
csv saved to the dir /ihme/scratch/users/beatrixh/synthetic_pop/pyomo/zero_pop_tracts 
with filename "state_{state}_geoids.csv". This file has columns: ['STATE',
'COUNTY','TRACT','BLOCK'], and a row for each block in the state with population 
zero, containing the corresponding FIPS geoid.


**3. gen_synth_pop/synth_pop_diagnostics.py**

Runs a series of checks on the file outputs.

## Script Logic ##
### 1. generate_underlying_structure.py ###
	
	ARGUMENTS: state (int), county (string), tract (string)
	
	INPUTS: 2010 Decennial Census tables P12A, P12B, P12C, P12D, P12E, P12F, 
	P12G P5, P12H, PCT21, P21A, P29B, P29C, P29D, P29E, P29F, P29G
	
	LOGIC:

        * pulls in joint geo/race/age/sex distribution

        * pulls in joint race/ethnicity distribution

        * pulls in joint ehtnicity/age/sex distribution

        * optimizes to get joint race/ethnicity/age/sex distribution

        * subtracts out {hispanic} from {hispanic + non-hispanic}

        * pulls in group-quarters (gq)/race distribution

        * pulls in gq/sex/age distribution

        * optimizes to get joint race/sex/age/gq distribution

        * assigns gq status to established structure

	OUTPUTS: a csv with one row for each unique 
	2-sex/23-age/7-race/2-ethnicity/2-gq/block-level geoid value, and the count 
	of such individuals from the 2010 Decennial census

### 2. new_sample_single_year_age_distribution.py ###
	
	ARGUMENTS: state (int), county (string), tract (string)
	
	INPUTS: the 2-sex/23-age/7-race/2-ethnicity/2-gq/block-level geoid output by 
	generate_underlying_structure.py; ACS community surveys from 2012-2017
	
	LOGIC: For each row of the underlying structure table, sample an individual 
	from the state-level population distribution. More specifically, given a row
	with a sex, 5ish-year age bin, race category, ethnicity, group quarters 
	status,	and census block geoid obtain list of individuals in the state who 
	match on:
    
        *  [sex, age, race, ethnicity, and gq status].

            * If nonexistant, try: [age, race, ethnicity, and gq status]

                * If nonexistant, try: [age, race, and gq status]

                    * If nonexistant, try: [age, and gq status]

                        * If nonexistant, try: [age]

	With this list, for every individual in the corresponding row of the 
	underlying population structure table, sample an individual from the 
	state-level ACS data, and assign to the individual from the underlying 
	population structure the sampled:

        * Specific age (Moving from 5-year bins to single-year)

        * Specific race (Moving from 7 categories to 63; note the only change 
        	here is to give multiracial individuals a specific racial mix)

        * Specific relationship to head of household (Moving from 2 values to 
        	18; provided a good prior for this value)

	OUTPUTS: a csv saved to the dir 
	ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/{state}, to the file 
	"state{i}_county{j}_tract{k}.csv" with columns ['state','county','tract',
	'block','geoid','age','sex_id','relationship','hispanic','racaian','racasn',
	'racblk','racnhpi','racsor','racwht','pweight'], and a row for each 
	individual in the specified tract.


### 3. identify_pop_zero_tracts.py ###

	ARGUMENTS: state (string)

	INPUTS: 2010 Decennial Census table P1

	LOGIC: Selects all blocks in the given state for which the "total population" (var P0010001) equals 0

	OUTPUTS: For each tract containing zero people in the 2010 Decennial census, 
	this creates a csv saved to the dir 
	/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/zero_pop_tracts with 
	filename "state_{state}_geoids.csv". This file has columns: ['STATE',
	'COUNTY','TRACT','BLOCK'], and a row for each block in the state with 
	population zero, containing the corresponding FIPS geoid.


### 4. swarm_underlying_structure.py ###

### 5. swarm_pop_zero.py ###

### 6. synth_pop_diagnostics.py ###

## Todos ##
1. "swarm_underlying_structure.py" ought to be renamed to "swarm_pop_synth.py"
2. "new_sample_data.py" ought to be renamed to "sample_data.py"
3. "new_process_decennial.py" ought to be renamed to "process_decennial.py"
4. "new_process_acs.py" ought to be renamed to "process_acs.py"
5. "new_sample_single_year_age_distribution.py" ought to be renamed to 
"sample_single_year_age_distribution.py"
6. Regarding the function gen_synth_pop.new_sample_data.sample_data:
	- Create diagnostic function to record the frequency with which the 
	different levels of the 'if' hierarchy are used. I.e., how frequently 
	do we match only on age and gq (expected low)
	- Restructure 'if' hierarchy such that if can't match on [age, race, 
	ethnicity, and gq status], try matching on [sex, race, ethnicity, and 
	gq status] before trying [race, ethnicity, and gq status].

## Acknowledgements ##

This research was conducted with support and resources provided by the Cornell Institute of Social and Economic Research (CISER) at Cornell University, via the 2010 decennial census files they formatted and have available for download at https://archive.ciser.cornell.edu/explore/download-centers/census-2010-sf1
