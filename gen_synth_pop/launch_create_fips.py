# -----------------------------------------------------------------------------
# Project: Add binning before DAS, see how effects results (number of near-zeros)
# Purpose: qsub parallelized jobs
# -----------------------------------------------------------------------------

# ---IMPORT LIBRARIES----------------------------------------------------------
import errno
import logging
import os
import numpy as np

logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s')

# ---SETTINGS------------------------------------------------------------------
threads = 1
mem = 10
j_access = ''
queue_type = 'all.q'
max_runtime = '00:20:00'
shell = "/ihme/code/beatrixh/microsim_2020/scripts/basic_shell.sh"
script = "/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/create_fips_dicts.py"
project = '-P proj_cost_effect'
output_dir = "/share/temp/sgeoutput/beatrixh/output"
error_dir = "/share/temp/sgeoutput/beatrixh/errors"
sge_output_dir = "-o {} -e {}".format(output_dir, error_dir)


# ---ARGUMENTS----------------------------------------------------------------
states = ['al', 'ak', 'ar', 'az', 'ca', 'co', 'ct', 'de', 'dc', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'pr']

states = ['nc']

# ---LAUNCH JOBS----------------------------------------------------------------
for state in states:
		# create qsub
		job_name ="fips_dict_{}".format(state)
		sys_sub = "qsub {} {} -N {} -l fthread={} -l m_mem_free={}G -l \
		archive={} -q {} -l h_rt={} -V".format(
			project,
			sge_output_dir,
			job_name,
			threads,
			mem,
			j_access,
			queue_type,
			max_runtime)
		# print(sys_sub)
		logging.info(sys_sub + " " + shell + " " + script + " " + state)
		os.popen(sys_sub + " " + shell + " " + script + " " + state)

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N launch_create_dicts -l fthread=1 -l m_mem_free=1G -q all.q -l h_rt=00:10:00 -V /ihme/code/beatrixh/microsim_2020/scripts/basic_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/launch_create_fips.py