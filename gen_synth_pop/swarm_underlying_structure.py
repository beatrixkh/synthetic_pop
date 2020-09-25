from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.executors.base import ExecutorParameters

import pickle
from synth_pop_diagnostics import *
# import shutil
import getpass
import os
user = 'beatrixh'

fips_dict = {'al': 1, 'ak': 2, 'ar': 5, 'az': 4, 'ca': 6, 'co': 8, 'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}

def main():
       states = ['ia']

       mojbom = UnderlyingStructureJobSwarm(states)
       # mojbom.gen_underlying_structure()
       # mojbom.sample_person_data()
       # status = mojbom.wflow.run()

       rejam = rerunFull(states)
       # rejam.add_underlying_str_jobs()
       rejam.sample_person_data()
       status = rejam.wflow.run()


       # status = mojbom.wflow_single.run()
       print(status)


class rerunFull(object):

       def __init__(self, states):
              self.states = states
              reruns = pd.DataFrame()
              for state in self.states:
                     reruns = reruns.append(determine_reruns(state))
              self.args_underlying = reruns.args.tolist()

              reruns_single_yr = pd.DataFrame()
              for state in self.states:
                     reruns_single_yr = reruns_single_yr.append(determine_final_reruns(state))
              self.args_single_yr = reruns_single_yr.args.tolist()
              self.underlying_str_jobs = {}
              self.person_jobs = {}


              self.wflow = Workflow(workflow_args='r_sin_ia_{}_04'.format(states[0][:2]), 
                     name = 'gen_underlying_structure', 
                     project = 'proj_cost_effect',
                     stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     working_dir='/homes/{}'.format(user),
                     seconds_until_timeout=len(self.states) * 36000)

       def add_underlying_str_jobs(self):

              interpreter = '/ihme/code/beatrixh/miniconda/envs/pyomo/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/generate_underlying_structure.py'
              for arg in self.args_underlying:
                     print("tract: {}".format(arg))
                     cmd = interpreter + ' ' + script + ' ' + arg
                     label = arg.replace(' ','_')
                     task = BashTask(cmd,
                            name = 'gen_{}'.format(label),
                            num_cores = 1,
                            m_mem_free = 8,
                            max_attempts = 3,
                            max_runtime_seconds = 3600,
                            resource_scales = {'m_mem_free' : 0.3, 
                            'max_runtime_seconds' : 2.0},
                            queue = 'all.q')
                     self.wflow.add_task(task)
                     print("added {}".format(task.name))
                     self.underlying_str_jobs[task.name] = task


       def sample_person_data(self):

              interpreter = '/ihme/code/beatrixh/miniconda/envs/basic/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/new_sample_single_year_age_distribution.py'

              for arg in self.args_single_yr:
                     print("tract: {}".format(arg))
                     cmd = interpreter + ' ' + script + ' ' + arg
                     label = arg.replace(' ','_')  
                     task = BashTask(cmd,
                            name = 'person_{}'.format(label),
                            num_cores = 1,
                            m_mem_free = 20,
                            max_attempts = 5,
                            max_runtime_seconds = 3600,
                            queue = 'long.q')
                     self.wflow.add_task(task)
                     self.person_jobs[task.name] = task

                     # require that the underlying structure for this tract has been saved
                     # task.add_upstream(self.underlying_str_jobs['gen_{}'.format(label)])


class UnderlyingStructureJobSwarm(object):
       
       fips_dict = {'al': 1, 'ak': 2, 'ar': 5, 'az': 4, 'ca': 6, 'co': 8, 
       'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 
       'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 
       'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 
       'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 
       'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 
       'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 
       'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}


       def __init__(self, states):
              self.states = states
              self.county_dict = load_county_dicts(states)
              self.tract_dict = load_tract_dicts(states)
              self.underlying_str_jobs = {}
              self.person_jobs = {}

              self.wflow = Workflow(workflow_args='single_{}_002'.format(states[0][:2]), 
                     name = 'gen_underlying_structure', 
                     project = 'proj_cost_effect',
                     stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     working_dir='/homes/{}'.format(user),
                     seconds_until_timeout=len(self.states) * 36000) #so far it's taken 2 hrs to do 1/2 of CA, with 8k tracts


              # self.wflow_single = Workflow(workflow_args='gen_single_wawi_0', 
              #        name = 'gen_single_year_structure', 
              #        project = 'proj_cost_effect',
              #        stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
              #        stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
              #        working_dir='/homes/{}'.format(user),
              #        seconds_until_timeout=len(self.states) * 36000) #so far it's taken 2 hrs to do 1/2 of CA, with 8k tracts


       def gen_underlying_structure(self):
              
              interpreter = '/ihme/code/beatrixh/miniconda/envs/pyomo/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/generate_underlying_structure.py'
              k = 0
              for state in self.states:
                     print("k: {}".format(k))
                     print("state: {}".format(state))
                     for county in self.county_dict[state]:
                            for tract in self.tract_dict[(state, county)]:
                                   args = [fips_dict[state], county, tract]
                                   args = [str(i) for i in args]
                                   args = ' '.join(args)
                                   cmd = interpreter + ' ' + script + ' ' + args  
                                   task = BashTask(cmd,
                                          name = 'gen_{}_{}_{}'.format(state,county,tract),
                                          num_cores = 1,
                                          m_mem_free = 15,
                                          max_attempts = 3,
                                          max_runtime_seconds = 3600,
                                          resource_scales = {'m_mem_free' : 0.3, 
                                          'max_runtime_seconds' : 2.0},
                                          queue = 'all.q')
                                   self.wflow.add_task(task)
                                   print("added {}".format(task.name))
                                   self.underlying_str_jobs[task.name] = task

                                   # if k > 0:
                                   #        prev_state = self.states.index(state) - 1
                                   #        prev_state = self.states[prev_state]
                                   #        for c in self.county_dict[prev_state]:
                                   #               for t in self.tract_dict[(prev_state, c)]:
                                   #                      dep_name = 'gen_{}_{}_{}'.format(prev_state,c,t)
                                   #                      task.add_upstream(self.underlying_str_jobs[dep_name])
                     k +=1 


       def sample_person_data(self):

              interpreter = '/ihme/code/beatrixh/miniconda/envs/basic/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/new_sample_single_year_age_distribution.py'

              j = 0
              for state in self.states:
                     print(j)
                     print(state)
                     for county in self.county_dict[state]:
                            for tract in self.tract_dict[(state, county)]:
                                   args = [fips_dict[state], county, tract]
                                   args = [str(i) for i in args]
                                   args = ' '.join(args)
                                   cmd = interpreter + ' ' + script + ' ' + args  
                                   task = BashTask(cmd,
                                          name = 'person_{}_{}_{}'.format(state,county,tract),
                                          num_cores = 1,
                                          m_mem_free = 20,
                                          max_attempts = 5,
                                          max_runtime_seconds = 3600,
                                          queue = 'all.q')
                                   self.wflow.add_task(task)
                                   self.person_jobs[task.name] = task

                                   # require that the underlying structure for this tract has been saved
                                   # task.add_upstream(self.underlying_str_jobs['gen_{}_{}_{}'.format(state,county,tract)])

                                   # # if multiple states, require that this script has already finished for all tracts for the previous state
                                   # if j > 0:
                                   #        prev_state = self.states.index(state) - 1
                                   #        prev_state = self.states[prev_state]
                                   #        for c in self.county_dict[prev_state]:
                                   #               for t in self.tract_dict[(prev_state, c)]:
                                   #                      dep_name = 'person_{}_{}_{}'.format(prev_state,c,t)
                                   #                      task.add_upstream(self.person_jobs[dep_name])
                     j += 1

       # def sample_person_data(self):

       #        interpreter = '/ihme/code/beatrixh/miniconda/envs/basic/bin/python'
       #        script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/new_sample_single_year_age_distribution.py'

       #        j = 0
       #        for state in self.states:
       #               print(k)
       #               print(state)
       #               for county in self.county_dict[state]:
       #                      for tract in self.tract_dict[(state, county)]:
       #                             args = [fips_dict[state], county, tract]
       #                             args = [str(i) for i in args]
       #                             args = ' '.join(args)
       #                             cmd = interpreter + ' ' + script + ' ' + args  
       #                             task = BashTask(cmd,
       #                                    name = 'person_{}_{}_{}'.format(state,county,tract),
       #                                    num_cores = 1,
       #                                    m_mem_free = 10,
       #                                    max_attempts = 5,
       #                                    max_runtime_seconds = 3600,
       #                                    queue = 'all.q')
       #                             self.wflow.add_task(task)
       #                             self.person_jobs[task.name] = task

       #                             # require that the underlyign structure for this county has been saved
       #                             task.add_upstream(self.underlying_str_jobs['gen_{}_{}_{}'.format(state,county,tract)])

       #                             # require that this script has already finished for all counties for the previous state
       #                             if j > 0:
       #                                    prev_state = self.states.index(state) - 1
       #                                    prev_state = self.states[prev_state]
       #                                    for c in self.county_dict[prev_state]:
       #                                           for t in self.tract_dict[(prev_state, c)]:
       #                                                  dep_name = 'person_{}_{}_{}'.format(prev_state,c,t)
       #                                                  task.add_upstream(self.person_jobs[dep_name])
       #               j += 1


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

if __name__=="__main__":
       main()

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_ia2_sing -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=20:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_ca_under -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=20:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_last_ca_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=10:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_re_fl_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=10:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_re_tx_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=30:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_re_azct_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=30:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_re_flia_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=30:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_re_msnm_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=30:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_full_nc_0 -l fthread=4 -l m_mem_free=20G -l archive= -q long.q -l h_rt=30:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py