from os import path, getpid
from time import sleep
import multiprocessing as mp
import threading as mt
from queue import Queue
import glob, importlib, datetime, sys, copy, signal, logging
from collections.abc import Iterable
from typing import Type, Callable, Dict, List, Union, Tuple, Set

import dag
import pqueue
from runner import Runner
from bus import Bus

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class Manager:
    def __init__(self) -> None:
        '''
        Check for dat file, serializes all dag file DAG objects,
        sets dags set class attribute
        '''
        signal.signal(signal.SIGINT, lambda x,y:sys.exit(0))
        self.dags = pqueue.PriorityQueue()
        self.tsq = Queue()
        self.store_dags()
        self.bus = Bus()
        self.bus.start()
        self.seat_sections()
    
    def store_dags(self) -> None:
        '''Stores DAG objects from dag files'''
        files = glob.glob(f'../DAGS/[!__]*[!__].py')
        for module in files:
            module = module[module.rindex('/') + 1:-3]
            mod = importlib.import_module(f'DAGS.{module}')
            dag_from_file = None
            global_vars = [var for var in vars(mod) if var[:2] != '__' and var[-2:] != '__']
            for attr in global_vars:
                pyobj = getattr(mod, attr)
                try:
                    # duck typing
                    iter(pyobj) and pyobj.ident and pyobj.tasks
                    dag_from_file = pyobj
                except:
                    continue
            if dag_from_file:
                self.dags.enqueue(dag_from_file)
    
    def set_next_run(self, dag):
        '''Sets DAGs next run date'''
        pass

    def seat_sections(self) -> None:
        '''Watches dag objects date attribute for when to run DAG'''
        while True: # TODO check for file modifications using C st_mtime
            dag = self.dags.dequeue()
            if dag:
                print('dequeued', dag.ident)
                while dag and dag.next_run_date != datetime.datetime.now().replace(second=0, microsecond=0):
                    time_delta = dag.next_run_date - datetime.datetime.now()
                    sleep(time_delta.seconds + 1)
                proc = Runner(dag)
                self.set_next_run(dag)
                print('running', dag.ident)
                proc.start()
                self.bus.tsq.put(proc)


if __name__ == '__main__':
    # fixes python imports system path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    Manager()
