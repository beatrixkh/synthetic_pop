from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.executors.base import ExecutorParameters

import getpass
user = 'beatrixh'

def main():
	# states = ['nm', 'ks', 'wi', 'nv', 'ma', 'mn', 'ca', 'pa', 'hi', 'mo', 'dc', 
	# 'fl', 'ar', 'al', 'wv', 'ga', 'oh', 'nj', 'ia', 'nh', 'in', 'nd', 'mt', 
	# 'wy', 'ky', 'sd', 'mi', 'va', 'sc', 'tx', 'tn', 'id', 'la', 'co', 'ny', 'ct', 
	# 'md', 'az', 'vt', 'ne', 'pr', 'ms', 'de', 'ut', 'ri', 'me', 'ak', 'or', 'ok',
	#  'il']
	# states = ['ks', 'wi', 'nv', 'ma', 'mn', 'ca', 'pa', 'hi', 'mo', 'dc', 
	# 'fl', 'ar', 'al', 'wv', 'ga', 'oh', 'nj', 'ia', 'nh', 'in', 'nd', 'mt', 
	# 'wy', 'ky', 'sd', 'mi', 'va', 'sc', 'tx', 'tn', 'id', 'la', 'co', 'ny', 'ct', 
	# 'md', 'az', 'vt', 'ne', 'pr', 'ms', 'de', 'ut', 'ri', 'me', 'ak', 'or', 'ok',
	#  'il']
	# states = ['pa']
	# # states = ['nm']
	# races = ["racwht","racaian","racasn","racblk","racnhpi","racsor","multiracial"]

	# mojbom = SynthPopJobSwarm(states, races)
	# mojbom.find_nrow_per_state_race_jobs()
	# status = mojbom.workflow_count.run()

	# if status == 0:
	# 	mojbom.generate_synth_pop()
	# 	mojbom.workflow_gen.run()
	# else:
	# 	print("count workflow failed")

	states = [6]
	counties = ['001']
	tracts = ['427100', '427200', '427300', '427600', '427700', '427800',
       '427900', '428000', '428100', '428200', '428301', '428302',
       '428400', '428500', '428600', '428700', '990000', '420100',
       '420200', '420300', '420400', '420500', '420600', '421100',
       '421200', '421300', '421400', '421500', '421600', '421700',
       '421800', '421900', '422000', '422100', '422200', '422300',
       '422400', '422500', '422600', '422700', '422800', '422900',
       '423000', '423100', '423200', '423300', '423400', '423500',
       '423601', '423602', '423700', '423800', '423901', '423902',
       '424001', '424002', '441100', '441200', '441301', '441302',
       '441401', '441402', '441503', '441521', '441522', '441523',
       '441524', '441601', '441602', '441700', '441800', '441921',
       '441923', '441924', '441925', '441926', '441927', '442000',
       '442100', '442200', '442301', '442302', '442400', '442500',
       '442601', '442602', '442700', '442800', '442900', '443001',
       '443002', '443102', '443103', '443104', '443105', '443200',
       '443301', '443321', '443322', '444302', '440100', '444100',
       '444200', '444301', '444400', '444500', '444601', '444602',
       '440200', '440301', '440304', '440305', '440306', '440307',
       '440308', '440331', '440332', '440333', '440334', '440335',
       '440336', '441501', '433700', '433800', '433900', '434000',
       '430101', '430102', '430200', '430300', '430400', '430500',
       '430600', '430700', '430800', '430900', '431000', '431100',
       '431200', '432800', '435103', '435200', '435500', '435601',
       '435602', '435700', '436300', '435300', '436401', '436402',
       '435102', '435104', '435400', '436200', '436500', '436601',
       '436602', '436700', '436800', '436900', '437000', '437101',
       '437102', '437200', '437300', '437400', '437500', '437600',
       '437701', '437702', '437800', '437900', '438000', '438100',
       '438201', '438203', '438204', '438300', '438400', '432100',
       '432200', '432300', '432400', '432501', '432502', '432600',
       '432700', '433000', '433102', '433103', '433104', '433200',
       '433300', '433400', '433500', '433600', '435800', '435900',
       '436000', '436100', '450101', '450102', '450200', '450300',
       '450400', '450501', '450502', '450750', '450751', '450752',
       '450601', '451101', '451102', '451201', '451202', '451300',
       '451401', '451403', '451404', '451501', '451503', '451504',
       '451505', '451506', '451601', '451602', '451701', '451703',
       '451704', '450602', '450603', '450604', '450605', '450606',
       '450607', '450701', '450741', '450742', '450743', '450744',
       '450745', '450746', '425101', '425102', '425103', '425104',
       '400100', '400200', '400300', '400400', '400500', '400600',
       '400700', '400800', '400900', '401000', '401100', '401200',
       '401300', '401400', '401500', '401600', '401700', '401800',
       '402200', '402400', '402500', '402600', '402700', '402800',
       '402900', '403000', '403100', '403300', '403400', '403501',
       '403502', '403600', '403701', '403702', '403800', '403900',
       '404000', '404101', '404102', '404200', '404300', '404400',
       '404501', '404502', '404600', '404700', '404800', '404900',
       '405000', '405100', '405200', '405301', '405302', '405401',
       '405402', '405500', '405600', '405700', '405800', '405901',
       '405902', '406000', '406100', '406201', '406202', '406300',
       '406400', '406500', '406601', '406602', '406700', '406800',
       '406900', '407000', '407101', '407102', '407200', '407300',
       '407400', '407500', '407600', '407700', '407800', '407900',
       '408000', '408100', '408200', '408300', '408400', '408500',
       '408600', '408700', '408800', '408900', '409000', '409100',
       '409200', '409300', '409400', '409500', '409600', '409700',
       '409800', '409900', '410000', '410100', '410200', '410300',
       '410400', '410500', '981900', '982000', '983200', '426100',
       '426200']

   	mojbom = nameThisLater(states, counties, tracts)
	mojbom.find_nrow_per_state_race_jobs()
	status = mojbom.workflow_count.run()



class nameThisLater(object):
	def __init__(self, states, counties, tracts):
		self.states = states
		self.counties = counties
		self.tracts = tracts

		self.workflow_pyomo = Workflow(workflow_args='first_pass_ca_county1', name = 'gen_underlying_structure_ca_county1', project = 'proj_cost_effect',
			stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
			stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
			working_dir='/homes/{}'.format(user),
			seconds_until_timeout=2000)

	def gen_underlying_structure(self):
		for state in self.states:
			for county in self.counties: #county_dict[state]:
				for tract in self.tracts: #state_county_tract_dict[(state,county)]:
					task = PythonTask(script = '/ihme/code/beatrixh/microsim_2020/census_2020/binning/test_update/generate_underlying_structure.py',
						args=[state,county,tract],
						num_cores = 1,
						m_mem_free = 20,
						max_attempts = 5,
						max_runtime_seconds = 2000,
						queue = 'all.q')
					self.workflow_pyomo.add_task(task)

# class SynthPopJobSwarm(object):
# 	def __init__(self, states, races, row_starts = [], nrows_per = 10_000):
# 		self.states = states
# 		self.races = races
# 		self.row_starts = row_starts
# 		self.nrows_per = nrows_per

# 		self.workflow_count = Workflow(workflow_args='pa_long_count_state_race_sizes', name = 'setup_for_synth_pop', project = 'proj_cost_effect',
# 			stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
# 			stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
# 			working_dir='/homes/{}'.format(user),
# 			seconds_until_timeout=600)
# 		self.workflow_gen = Workflow(workflow_args='full_synth_pop_gen', name = 'synth_pop', project = 'proj_cost_effect',
# 			stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
# 			stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
# 			working_dir='/homes/{}'.format(user),
# 			seconds_until_timeout=3600)
# 		# self.row_calculation = {}

# 	def find_nrow_per_state_race_jobs(self):
# 	# for each state-race, i want these jobs to output a list of row_starts
# 		for state in self.states:
# 			for race in self.races:
# 				task = PythonTask(script= '/ihme/code/beatrixh/microsim_2020/census_2020/binning/count_nrows_per_state_race.py',
# 					args=[state, race],
# 					name='pa_long_test_find_nrows_{}_{}'.format(state,race), #oops left the 'nm_' here
# 					num_cores = 1,
# 					m_mem_free = 10,
# 					max_attempts = 5,
# 					max_runtime_seconds = 600,
# 					queue = 'long.q')
# 				self.workflow_count.add_task(task)
# 				# self.row_calculation[task.name] = task



# 	def generate_synth_pop(self):
# 		for state in self.states:
# 			for race in self.races:
# 				row_path = '/ihme/temp/sgeoutput/beatrixh/jobmon_args/nrows_{}_{}.csv'.format(state,race)
# 				self.row_starts = pd.read_csv(row_path).starts
# 				for row in self.row_starts:
# 					task = PythonTask(script= '/ihme/code/beatrixh/microsim_2020/census_2020/binning/sample_single_year_age_distribution.py',
# 						args=[state, race, row],
# 						name='gen_synth_pop_{}_{}_{}_{}'.format(state, race, row_start, row_start + self.nrows_per),
# 						num_cores = 1,
# 						m_mem_free = 10,
# 						max_attempts = 5,
# 						max_runtime_seconds = 1200,
# 						queue = 'all.q')
# 					# task.add_upstream(
# 					# 	self.row_calculation['find_nrows_{}_{}'.format(state,race)])
# 					self.workflow_gen.add_task(task)


if __name__=="__main__":
	main()




# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_synth_pop -l fthread=1 -l m_mem_free=10G -l archive= -q all.q -l h_rt=00:30:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/binning/swarm_synth_pop.py