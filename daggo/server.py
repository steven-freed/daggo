from os import path, getpid
from time import sleep
import multiprocessing as mp
import glob, importlib, datetime, sys, heapq, copy, signal, logging
from collections.abc import Iterable
from typing import Type, Callable, Dict, List, Union, Tuple, Set

import dag

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class Server:

    def __init__(self) -> None:
        '''
        Check for dat file, serializes all dag file DAG objects,
        sets dags set class attribute
        '''
        signal.signal(signal.SIGINT, self.ctr_c_handler)
        self.runners = []
        self.dags = []
        self.store_dags()
        heapq.heapify(self.dags)
        self.start_stop_watch()
    
    def ctr_c_handler(self, sig, frame):
        for p in self.runners:
            p.join()
        sys.exit(0)

    def store_dags(self) -> None:
        '''Stores DAG objects from dag files'''
        files = glob.glob(f'../*dag.py')
        for f in files:
            f = f[f.index('/') + 1:-3]
            mod = importlib.import_module(f)
            dag_from_file = None
            global_vars = [var for var in vars(mod) if var[:2] != '__' and var[-2:] != '__']
            for attr in global_vars:
                pyobj = getattr(mod, attr)
                try:
                    # duck typing
                    iter(pyobj) and pyobj.ident and pyobj.tasks
                    dag_from_file = pyobj
                except:
                    pass
            heapq.heappush(self.dags, dag_from_file) if dag_from_file else None
    
    def set_next_run(self, dag):
        '''Sets DAGs next run date'''
        pass
        #dag._set_next_run_date()
        #heapq.heappush(self.dags, dag)

    def start_stop_watch(self) -> None:
        '''Watches dag objects date attribute for when to run DAG'''
        runner = Runner()
        while True: # TODO check for file modifications using C st_mtime
            try:
                dag = heapq.heappop(self.dags)
            except IndexError:
                continue
            while datetime.datetime.now().replace(second=0, microsecond=0) != dag.next_run_date:
                time_delta = dag.next_run_date - datetime.datetime.now()
                sleep(time_delta.seconds + 1)
            dagcp = copy.deepcopy(dag)
            proc = mp.Process(target=runner, args=(dagcp,))
            self.set_next_run(dag)
            proc.start()
            self.runners.append(proc)


class Runner:
    '''Runs all DAGNode processes in a DAG object'''
    def __call__(self, dag):
        logger.debug(f'Running dag {dag.ident} in pid {getpid()}')
        for node_or_nodes in dag:
            if isinstance(node_or_nodes, Iterable):
                self.run_parallel_tasks(node_or_nodes)
                print()
            else:
                self.run_singleton_task(node_or_nodes)
                print()

    def run_singleton_task(self, task):
        '''Runs single DAGNode'''
        proc = mp.Process(target=task.task)
        proc.start()
        proc.join()

    def run_parallel_tasks(self, tasks):
        '''Runs multiple DAGNodes in parallel'''
        running_tasks = []
        for task in tasks:
            proc = mp.Process(target=task.task)
            proc.start()
            running_tasks.append(proc)
        [p.join() for p in running_tasks]


if __name__ == '__main__':
    # fixes python imports system path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    Server()
