import multiprocessing as mp
import glob
import importlib
import logging
import datetime
import os
from time import sleep
import pickle
from typing import Type, Callable, Dict, List, Union, Tuple, Set

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DAGNode:
    '''
    Base class for DAGNodes basic requirements
    '''
    def __init__(self, identifier: str, callee: Callable) -> None:
        self.identifier = identifier
        self.callee = callee
        self.downstream = []

class DAG:
    '''
    DAG object for setting schedule, topological sort, configurations
    and serializing for the DAG Runner
    '''
    def __init__(self, identifier: str, topsort: List[Union[DAGNode, List[DAGNode]]],
                schedule: str = None,
                min: int = None, hr: int = None, dom: int = None,
                mon: int = None, dow: int = None) -> None:
        self.identifier = identifier
        self.schedule = self._set_schedule(schedule or (min, hr, dom, mon, dow))
        self.date = self._set_date(self.schedule)
        self.jobs = self._set_downstreams(topsort)
        # serializes DAG object for Runner to read
        try:
            object_set = pickle.load(open('daggo.dat', 'rb'))
        except:
            object_set = set()
            pickle.dump(object_set | {self}, open('daggo.dat', 'wb+'))

    def _set_date(self, schedule: Tuple[int]) -> Dict[str, int]:
        '''Parses tuple schedule to dict'''
        return {
            'min': schedule[0],
            'hr':  schedule[1],
            'dom': schedule[2],
            'mon': schedule[3],
            'dow': schedule[4]
        }

    def _set_schedule(self, schedule: Union[str, Tuple[int]]) -> Tuple[int]:
        '''Parses schedule string or kwargs to cron tuple: (min, hr, dom, mon, dow)'''
        try:
            return tuple([int(metric) if metric != '*' else None for metric in schedule.split(' ')])
        except:
            return schedule

    def _set_downstreams(self, jobs: List[Union[DAGNode, List[DAGNode]]]
                        ) -> List[Union[DAGNode, List[DAGNode]]]:
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
    
    def __str__(self) -> str:
        dagstr = ''
        for j in self.jobs:
            try:
                for i in j:
                    dagstr += f'{i.identifier}, '
                dagstr = dagstr[:-2] + '\n'
            except:
                dagstr += f'{j.identifier}\n'
        return dagstr

    def __iter__(self) -> iter:
        return iter(self.jobs)


class Runner:

    dags = set()

    def __init__(self) -> None:
        '''
        Check for dat file, serializes all dag file DAG objects,
        sets dags set class attribute
        '''
        self.check_dat()
        self.serialize_dags()
        self.dags = self.get_dat()
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

    def run(self, dag: DAGNode) -> None:
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
    
    def check_dat(self) -> None:
        '''Creates dat file if not exists'''
        open('daggo.dat', 'wb+')
    
    def get_dat(self) -> Set[DAGNode]:
        '''Reads dat file'''
        try:
            dags = pickle.load(open('daggo.dat', 'rb'))
        except:
            return None
        return dags

    def serialize_dags(self) -> None:
        '''Serializes DAG object for dag files'''
        self.check_dat()
        files = glob.glob("*_dag.py")
        for f in files:
            mod = importlib.import_module(f[:-3])
            mod.main()

    def start_stop_watch(self) -> None:
        '''Watches dag objects date attribute for when to run DAG'''
        while True:
            for dag in self.dags:
                if self.should_run(dag.date):
                    self.run(dag)

if __name__ == '__main__':
    runner = Runner()