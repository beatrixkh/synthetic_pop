from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.executors.base import ExecutorParameters

import pickle
import getpass
import os
user = 'beatrixh'

all_states = ['al', 'ak', 'ar', 'az', 'ca', 'co', 'ct', 'de', 'dc', 'fl', 'ga', 
'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 
'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 
'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 
'wi', 'wy', 'pr']

def main():
       states = all_states
       # states = ['co']
       mojbom = Swarm_binning(states)
       mojbom.add_state_counties()
       status = mojbom.wflow.run()

       print(status)

class Swarm_binning(object):
       
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
              self.wflow = Workflow(workflow_args='{}_bin_co_04'.format(states[0][:2]), 
                     name = 'count_zero_pop_tracts', 
                     project = 'proj_cost_effect',
                     stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     working_dir='/homes/{}'.format(user),
                     seconds_until_timeout=len(self.states) * 60 * 10)


       def add_state_counties(self):
              
              interpreter = '/ihme/code/beatrixh/miniconda/envs/pyomo/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/binning/count_bins_head.py'
              for state in self.states:
                     counties = self.county_dict[state] #grab counties per state
                     for county in counties:
                            tracts = self.tract_dict[(state, county)] #grab tracts per county
                            rtime = len(tracts) * 2 #expected runtime
                            rtime = rtime + 600 # buffer               
                            args = state + ' ' + str(county)
                            cmd = interpreter + ' ' + script + ' ' + args  
                            task = BashTask(cmd,
                                   name = '{}_{}_bin_and_calc_n_hi'.format(state, county),
                                   num_cores = 1,
                                   m_mem_free = 10,
                                   max_attempts = 3,
                                   max_runtime_seconds = rtime,
                                   resource_scales = {'m_mem_free' : 0.3, 
                                   'max_runtime_seconds' : 2.0},
                                   queue = 'long.q')
                            self.wflow.add_task(task)
                            print("added {}".format(task.name))


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

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_bin_all -l fthread=4 -l m_mem_free=5G -l archive= -q long.q -l h_rt=03:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/binning/swarm_bin.py

