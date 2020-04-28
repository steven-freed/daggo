from os import getpid
import multiprocessing
import logging
from collections.abc import Iterable
from typing import Type, Callable, Dict, List, Union, Tuple, Set


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class Runner(multiprocessing.Process):
    '''Runs all DAGNode processes in a DAG object'''
    def __init__(self, dag):
        super(Runner, self).__init__(name=dag.ident)
        self.dag = dag

    def run(self):
        logger.debug(f'Running dag {self.dag.ident} in pid {getpid()}')
        for node_or_nodes in self.dag:
            if isinstance(node_or_nodes, Iterable):
                self.run_parallel_tasks(node_or_nodes)
                print()
            else:
                self.run_singleton_task(node_or_nodes)
                print()

    def run_singleton_task(self, task):
        '''Runs single DAGNode'''
        proc = multiprocessing.Process(target=task.task)
        proc.start()
        proc.join()

    def run_parallel_tasks(self, tasks):
        '''Runs multiple DAGNodes in parallel'''
        running_tasks = []
        for task in tasks:
            proc = multiprocessing.Process(target=task.task)
            proc.start()
            running_tasks.append(proc)
        [p.join() for p in running_tasks]