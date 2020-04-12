import threading
import multiprocessing as mp
import sys
import glob
import importlib
import logging
import subprocess
import os
from time import sleep

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

'''
if file mod-ed: update time, insert file name into dict for key time
set schedule as datetime.datetime object
'''

class DAGNode:
    '''
    min hr dom mon dow
    '''
    def __init__(self, identifier, callee, schedule=''):
        self.identifier = identifier
        self.callee = callee
        self.downstream = []


class DAG:
    def __init__(self, schedule, jobs):
        self.schedule = schedule
        self.jobs = self.set_downstreams(jobs)
        print(str(self))
        Runner.run(self)
    
    def set_downstreams(self, jobs):
        '''Sets downstream node pointers for all nodes in DAG'''
        n = 0
        for i in range(len(jobs) - 1):
            if n + 1 < len(jobs): n += 1
            try:
                for j in range(len(jobs[i])):
                    try:
                        jobs[i][j].downstream += list(jobs[n])
                    except:
                        jobs[i][j].downstream.append(jobs[n])
            except:
                try:
                    jobs[i].downstream += jobs[n]
                except:
                    jobs[i].downstream.append(jobs[n])
        return jobs
    
    def __str__(self):
        dagstr = ''
        for j in self.jobs:
            try:
                for i in j:
                    dagstr += f'{i.identifier}, '
                dagstr = dagstr[:-2] + '\n'
            except:
                dagstr += f'{j.identifier}\n'
        return dagstr

    def __iter__(self):
        return iter(self.jobs)


class Runner:

    dag_files = {}
    
    @staticmethod
    def run(dag):
        for node_or_nodes in dag:
            try: # iterable
                for n in node_or_nodes:
                    proc = mp.Process(target=n.callee)
                    proc.start()
                proc.join()
                print()
            except: # non-iterable
                proc = mp.Process(target=node_or_nodes.callee)
                proc.start()
                proc.join()
                print()
    
    @staticmethod
    def watch():
        while True:
            files = glob.glob("*_dag.py")
            for f in files:
                if not Runner.dag_files.get(f):
                    Runner.dag_files[f] = 0
                new_time = os.stat(f).st_mtime
                old_time = Runner.dag_files.get(f)
                if old_time < new_time:
                    Runner.dag_files[f] = new_time
                    #log.info(f'EXEC {f}')
                    mod = importlib.import_module(f[:-3])
                    p = mp.Process(target=mod.main)
                    p.run()
            sleep(5)

if __name__ == '__main__':
    watcher = mp.Process(target=Runner.watch)
    watcher.start()
    watcher.join()
