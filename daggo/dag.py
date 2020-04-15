import multiprocessing as mp
import glob
import importlib
import logging
import datetime
import sys
import os
from time import sleep
from typing import Type, Callable, Dict, List, Union, Tuple, Set, TypeVar


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

T = TypeVar('DAGNode')

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
    def __init__(self, identifier: str, topsort: List[Union[T, List[T]]],
                schedule: str = None,
                min: int = None, hr: int = None, dom: int = None,
                mon: int = None, dow: int = None) -> None:
        self.identifier = identifier
        self.schedule = self._set_schedule(schedule or (min, hr, dom, mon, dow))
        self.date = self._set_date(self.schedule)
        self.jobs = self._set_downstreams(topsort)

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

    def _set_downstreams(self, jobs: List[Union[T, List[T]]]) -> List[Union[T, List[T]]]:
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