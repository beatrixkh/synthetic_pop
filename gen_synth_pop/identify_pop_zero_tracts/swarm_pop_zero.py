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

all_states = ['al', 'ak', 'ar', 'az', 'ca', 'co', 'ct', 'de', 'dc', 'fl', 'ga', 
'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 
'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 
'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 
'wi', 'wy', 'pr']

def main():
       states = [i for i in all_states if i!='pa']
       mojbom = Find_pop_zero_tracts(states)
       mojbom.add_states()
       status = mojbom.wflow.run()

       print(status)

class Find_pop_zero_tracts(object):
       
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

              self.wflow = Workflow(workflow_args='{}_find_zeros_04'.format(states[0][:2]), 
                     name = 'count_zero_pop_tracts', 
                     project = 'proj_cost_effect',
                     stderr='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     stdout='/ihme/scratch/users/{}/sgeoutput'.format(user),
                     working_dir='/homes/{}'.format(user),
                     seconds_until_timeout=len(self.states) * 60 * 10)


       def add_states(self):
              
              interpreter = '/ihme/code/beatrixh/miniconda/envs/pyomo/bin/python'
              script = '/ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/identify_pop_zero_tracts.py'
              for state in self.states:
                     args = state
                     cmd = interpreter + ' ' + script + ' ' + args  
                     task = BashTask(cmd,
                            name = 'find_pop_zero_tracts_{}'.format(state),
                            num_cores = 1,
                            m_mem_free = 10,
                            max_attempts = 3,
                            max_runtime_seconds = 60 * 10,
                            resource_scales = {'m_mem_free' : 0.3, 
                            'max_runtime_seconds' : 2.0},
                            queue = 'all.q')
                     self.wflow.add_task(task)
                     print("added {}".format(task.name))


if __name__=="__main__":
       main()

# qsub -P proj_cost_effect -o /share/temp/sgeoutput/beatrixh/output -e /share/temp/sgeoutput/beatrixh/errors -N jm_find_zeros -l fthread=4 -l m_mem_free=5G -l archive= -q all.q -l h_rt=02:00:00 -V /ihme/code/beatrixh/microsim_2020/scripts/jobmon_shell.sh /ihme/code/beatrixh/microsim_2020/census_2020/synthetic_pop/gen_synth_pop/swarm_pop_zero.py
