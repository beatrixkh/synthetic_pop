from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.executors.base import ExecutorParameters

import pickle
# import shutil
import getpass
user = 'beatrixh'

fips_dict = {'al': 1, 'ak': 2, 'ar': 4, 'az': 5, 'ca': 6, 'co': 8, 'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}

def main():
       # states = ['al', 'ak', 'ar', 'az', 'ca', 'co', 'ct', 'de', 'dc', 
       #       'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 
       #       'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 
       #       'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 
       #       'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'pr']

       # states = list(set([i[:2] for i in os.listdir('/ihme/scratch/users/beatrixh/fips_dicts/')]))
       states = ['ia', 'ms', 'hi', 'mi', 'nh', 'mt', 'ok', 'la', 'oh', 'ct', 'mn', 'sd', 'nd', 'ca', 'sc', 'ky', 'ne', 'tn', 'dc', 'ma', 'pr', 'ak', 'id', 'ks', 'az', 'il', 'me', 'wv', 'md', 'nv', 'pa', 'nj', 'ny', 'fl', 'ga', 'nm', 'va', 'vt', 'or', 'wa', 'ar', 'wy', 'ut', 'al', 'co', 'de', 'wi', 'mo', 'in', 'ri']

       mojbom = UnderlyingStructureJobSwarm(states)
       mojbom.gen_underlying_structure()
       status = mojbom.workflow_pyomo.run()
       print(status)

class UnderlyingStructureJobSwarm(object):
       
       fips_dict = {'al': 1, 'ak': 2, 'ar': 4, 'az': 5, 'ca': 6, 'co': 8, 'ct': 9, 'de': 10, 'dc': 11, 'fl': 12, 'ga': 13, 'hi': 15, 'id': 16, 'il': 17, 'in': 18, 'ia': 19, 'ks': 20, 'ky': 21, 'la': 22, 'me': 23, 'md': 24, 'ma': 25, 'mi': 26, 'mn': 27, 'ms': 28, 'mo': 29, 'mt': 30, 'ne': 31, 'nv': 32, 'nh': 33, 'nj': 34, 'nm': 35, 'ny': 36, 'nc': 37, 'nd': 38, 'oh': 39, 'ok': 40, 'or': 41, 'pa': 42, 'ri': 44, 'sc': 45, 'sd': 46, 'tn': 47, 'tx': 48, 'ut': 49, 'vt': 50, 'va': 51, 'wa': 53, 'wv': 54, 'wi': 55, 'wy': 56, 'pr': 72}


       def __init__(self, states):
              self.states = states
              self.county_dict = load_county_dicts(states)
              self.tract_dict = load_tract_dicts(states)
              self.all_jobs = {}
              self.person_jobs = {}

              self.workflow_pyomo = Workflow(workflow_args='run_tracts_100', name = 'gen_underlying_structure', project = 'proj_cost_effect',
                            stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                            stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
                            working_dir='/homes/{}'.format(user),
                            seconds_until_timeout=360_000)


       def gen_underlying_structure(self):
              
              interpreter = '/ihme/code/beatrixh/miniconda/envs/pyomo/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/generate_underlying_structure.py'
              k = 0
              for state in self.states:
                     print(k)
                     print(state)
                     for county in self.county_dict[state]:
                            for tract in self.tract_dict[(state, county)]:
                                   args = [fips_dict[state], county, tract]
                                   args = [str(i) for i in args]
                                   args = ' '.join(args)
                                   cmd = interpreter + ' ' + script + ' ' + args  
                                   task = BashTask(cmd,
                                          name = 'gen_{}_{}_{}'.format(state,county,tract),
                                          num_cores = 1,
                                          m_mem_free = 10,
                                          max_attempts = 5,
                                          max_runtime_seconds = 3600,
                                          queue = 'all.q')
                                   self.workflow_pyomo.add_task(task)
                                   print("added {}".format(task.name))
                                   self.all_jobs[task.name] = task

                                   if k > 0:
                                          prev_state = self.states.index(state) - 1
                                          prev_state = self.states[prev_state]
                                          for c in self.county_dict[prev_state]:
                                                 for t in self.tract_dict[(prev_state, c)]:
                                                        dep_name = 'gen_{}_{}_{}'.format(prev_state,c,t)
                                                        task.add_upstream(self.all_jobs[dep_name])
                     k +=1 

       # def sample_person_data(self):

       #        interpreter = '/ihme/code/beatrixh/miniconda/envs/basic/bin/python'
       #        script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/new_sample_single_year_age_distribution.py'

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
       #                             self.workflow_pyomo.add_task(task)
       #                             self.person_jobs[task.name] = task
       #                             task.add_upstream(self.all_jobs['gen_{}_{}_{}'.format(state,county,tract)])

       #                             if k > 0:
       #                                    prev_state = self.states.index(state) - 1
       #                                    prev_state = self.states[prev_state]
       #                                    for c in self.county_dict[prev_state]:
       #                                           for t in self.tract_dict[(prev_state, c)]:
       #                                                  dep_name = 'person_{}_{}_{}'.format(prev_state,c,t)
       #                                                  task.add_upstream(self.person_jobs[dep_name])



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


# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_underlying_structure_pyomo -l fthread=4 -l m_mem_free=5G -l archive= -q long.q -l h_rt=50:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_underlying_structure.py