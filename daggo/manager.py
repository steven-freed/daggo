from os import path, getpid
from time import sleep
import multiprocessing as mp
import threading as mt
from queue import Queue
import glob, importlib, datetime, sys, copy, signal, logging
from collections.abc import Iterable
from typing import Type, Callable, Dict, List, Union, Tuple, Set

import dag
from pqueue import PriorityQueue
from runner import Runner

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class Manager:

    def __init__(self) -> None:
        '''
        Check for dat file, serializes all dag file DAG objects,
        sets dags set class attribute
        '''
        signal.signal(signal.SIGINT, self.ctr_c_handler)
        self.dags = PriorityQueue()
        self.tsq = Queue()
        self.store_dags()
        self.bus = Bus()
        self.bus.start()
        #self.join_runners_thread = mt.Thread(target=self.join_runners)
        #self.join_runners_thread.start()
        self.seat_sections()
    
    def ctr_c_handler(self, sig, frame):
        self.bus.join()
        sys.exit(0)

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
                    iter(pyobj) and pyobj.ident and pyobj.tasks
                    dag_from_file = pyobj
                except:
                    pass
            self.dags.enqueue(dag_from_file) if dag_from_file else None
    
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
                dagcp = copy.deepcopy(dag)
                proc = Runner(dagcp)
                self.set_next_run(dag)
                print('running', dag.ident)
                proc.start()
                self.bus.tsq.put(proc)

class Bus(mt.Thread):
    '''Deamon thread joins all Runner Processes, aka cleans up tasks'''
    def __init__(self):
        super(Bus, self).__init__(name='Bus', daemon=True)
        self.tsq = Queue()
    
    def run(self):
        while True:
            runner = self.tsq.get()
            if runner:
                runner.join(timeout=1)
                if not runner.is_alive():
                    print('joined process', runner.name)
                else:
                    self.tsq.put(runner)


if __name__ == '__main__':
    # fixes python imports system path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    Manager()
