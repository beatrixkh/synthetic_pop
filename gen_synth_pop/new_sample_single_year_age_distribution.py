import pandas as pd, numpy as np
import datetime
import line_profiler

from new_process_acs import *
from new_process_decennial import *
from new_sample_data import *

def main(state, county, tract):
    print("loading fips dict")
    state_codes = {1: 'AL', 2: 'AK', 5: 'AR', 4: 'AZ', 6: 'CA', 
    8: 'CO', 9: 'CT', 10: 'DE', 11: 'DC', 12: 'FL', 13: 'GA', 
    15: 'HI', 16: 'ID', 17: 'IL', 18: 'IN', 19: 'IA', 20: 'KS', 
    21: 'KY', 22: 'LA', 23: 'ME', 24: 'MD', 25: 'MA', 26: 'MI', 
    27: 'MN', 28: 'MS', 29: 'MO', 30: 'MT', 31: 'NE', 32: 'NV', 
    33: 'NH', 34: 'NJ', 35: 'NM', 36: 'NY', 37: 'NC', 38: 'ND', 
    39: 'OH', 40: 'OK', 41: 'OR', 42: 'PA', 44: 'RI', 45: 'SC', 
    46: 'SD', 47: 'TN', 48: 'TX', 49: 'UT', 50: 'VT', 51: 'VA', 
    53: 'WA', 54: 'WV', 55: 'WI', 56: 'WY', 72: 'PR'}

    state_name = state_codes[int(state)].lower()

    data_dir = '/ihme/scratch/users/beatrixh/underlying_pop/best/{}/'.format(state_name)
    filename = 'state{}_county{}_tract{}.csv'.format(state,county,tract)

    read_path = data_dir + filename

    # pull in block/5 year age/sex/7-bucket race/ethnicity/hhgq structure
    # read_path = '/share/temp/sgeoutput/beatrixh/de_test_8_21.csv'
    print("reading input")
    input_df = pd.read_csv(read_path)

    # convert geoid back to str
    input_df = add_geoid(input_df)
    input_df.geoid = input_df.geoid.astype(str)

    # make sure geoids are all block-level, correctly formatted
    assert(np.unique([len(i) for i in input_df.geoid]).sum()==15), "Some of the geoids aren't length 15"

    # grab some invariants to check against
    print("grabbing invars")
    male_total_check = input_df[input_df.sex_id==1].pop_count.sum()
    fem_total_check = input_df[input_df.sex_id==2].pop_count.sum()
    unique_vals_invar1 = input_df.geoid.nunique() * input_df.sex_id.nunique() * input_df.hispanic.nunique() * input_df.hhgq.nunique()

    # get joint distribution from ACS
    acs_data = load_incoming_data(state_name)
    all_race_age_distr = clean_acs(acs_data, races = 'all', years = acs_data.year.unique().tolist())
    all_race_age_distr = format_acs(all_race_age_distr)

    print("gen data")
    # sample detail from ACS to fill in specifqstatic age/race/relp
    single_years_df = generate_single_year_df(input_df, all_race_age_distr)

    ## checks ------------------------------------------------------------------

    assert(single_years_df[single_years_df.sex_id==1].pweight.sum()==male_total_check), "male total didn't stay constant"
    assert(single_years_df[single_years_df.sex_id==2].pweight.sum()==fem_total_check), "fem total didn't stay constant"

    check_unique_vals_invar1 = single_years_df.geoid.nunique() * single_years_df.sex_id.nunique() * single_years_df.hispanic.nunique() * single_years_df.hhgq.nunique()
    assert(check_unique_vals_invar1==unique_vals_invar1), "nunique vals of some var (geoid, sex, hispanic, hhgq) is off"


    ## format and save ---------------------------------------------------------

    # add back in location_colss
    single_years_df['state'] = single_years_df.geoid.str[:2]
    single_years_df['county'] = single_years_df.geoid.str[2:5]
    single_years_df['tract'] = single_years_df.geoid.str[5:11]
    single_years_df['block'] = single_years_df.geoid.str[11:]

    #final order
    final_cols = ['state','county','tract','block','geoid','age','sex_id','relationship','hispanic', 
    'racaian', 'racasn', 'racblk', 'racnhpi', 'racsor', 'racwht', 'pweight']
    output = single_years_df.filter(items=final_cols)

    print("saving")
    # save to csv
    # first try to save to best dir
    best_dir = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/best/'
    state_dir = best_dir  + state_name
    for d in [best_dir, state_dir]:
        if not os.path.exists(d):
            os.mkdir(d)
    save_name = '/state{}_county{}_tract{}.csv'.format(state,county,tract)

    # if this file doesn't already exist in best dir, save. otherwise, save to date-specific dir
    if not os.path.exists(state_dir + save_name):
        output.to_csv(state_dir + save_name, index=False)
    else:
        today = datetime.date.today()
        today_dir = '/ihme/scratch/users/beatrixh/synthetic_pop/pyomo/' + str(today) + '/'
        state_dir = today_dir + '/' + state_name
        for d in [today_dir, state_dir]:
            os.mkdir(d)
        output.to_csv(state_dir + save_name, index = False)    


if __name__=="__main__":
    import sys
    import argparse
    import cProfile
    print("loading args")
    parser = argparse.ArgumentParser()
    parser.add_argument("state", help="", type=int)
    parser.add_argument("county", help="", type=str)
    parser.add_argument("tract", help="", type=str)
    args = parser.parse_args()
    print("running main")
    main(args.state, args.county, args.tract)

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N mem15G -l fthread=1 -l m_mem_free=15G -q all.q -l h_rt=01:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/basic_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/new_sample_single_year_age_distribution.py 6 001 400100