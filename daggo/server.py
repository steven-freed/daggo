import multiprocessing as mp
import glob
import importlib
import logging
import datetime
import sys
from os import path
from time import sleep
import pickle

from typing import Type, Callable, Dict, List, Union, Tuple, Set, TypeVar

import dag

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

T = TypeVar('dag.DAGNode')

class Runner:

    dags = set()

    def __init__(self) -> None:
        '''
        Check for dat file, serializes all dag file DAG objects,
        sets dags set class attribute
        '''
        self.store_dags()
        self.start_stop_watch()

    def should_run(self, job_date: Dict[str, int]) -> bool:
        today = datetime.datetime.now()
        pydow = datetime.datetime.now().weekday()
        cron_dow = (pydow + 1) % 7
        if (job_date['min'] or today.minute) == today.minute and\
            (job_date['hr'] or today.hour) == today.hour and\
            (job_date['dom'] or today.day) == today.day and\
            (job_date['mon'] or today.month) == today.month and\
            (job_date['dow'] or cron_dow) == cron_dow:
            return True
        else:
            return False

    def run(self, dag: T) -> None:
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

    def store_dags(self) -> None:
        '''Stores DAG objects from dag files'''
        files = glob.glob(f'../*_dag.py')
        for f in files:
            f = f[f.index('/') + 1:-3]
            mod = importlib.import_module(f)
            dag_from_file = None
            global_vars = [var for var in vars(mod) if var[:2] != '__' and var[-2:] != '__']
            for attr in global_vars:
                pyobj = getattr(mod, attr)
                try:
                    # duck typing
                    iter(pyobj) and pyobj.identifier and pyobj.schedule
                    dag_from_file = pyobj
                except:
                    pass
            Runner.dags.add(dag_from_file)

    def start_stop_watch(self) -> None:
        '''Watches dag objects date attribute for when to run DAG'''
        while True:
            for dag in Runner.dags:
                if self.should_run(dag.date):
                    self.run(dag)

if __name__ == '__main__':
    # fixes python imports system path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    runner = Runner()