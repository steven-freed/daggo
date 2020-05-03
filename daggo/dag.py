import datetime, logging
from typing import Type, Callable, Dict, List, Union, Tuple, Set, TypeVar

from nodes.dagnode import DAGNode

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


'''Statuses'''
__INACTIVE__ = 0
__QUEUED__   = 1
__PENDING__  = 2
__RUNNING__  = 3
__FAILED__   = 4


class DAG:
    '''
    DAG object for setting schedule, topological sort, configurations
    and serializing for the DAG Runner
    '''
    def __init__(self, ident: str, topsort, start_date=datetime.date.today(),
                schedule: str = None, sched_delta=None,
                enabled=True) -> None:
        self.ident = ident
        self.start_date = start_date
        self.schedule = schedule or sched_delta
        self.enabled = enabled
        self._status = __INACTIVE__
        self.tasks = self._setDownstreams(topsort)
        self.next_run_date = datetime.datetime.now()
        if enabled:
            self._setNextRunDate()

    def _setNextRunDate(self) -> None:
        if type(self.schedule) == str:
            yr, min, hr, dom, mon = self._parseCron(self.schedule)
            self.next_run_date = datetime.datetime(yr, mon, dom, hour=hr, minute=min)
        elif isinstance(self.schedule, datetime.timedelta):
            self.next_run_date = self.next_run_date + self.schedule
        else:
            raise Exception('Invalid Schedule')
        self.next_run_date = self.next_run_date.replace(microsecond=0)

    def _parseCron(self, cron):
        # "min hr dom mon dow"
        cron = cron.split(' ')
        if len(cron) != 5: raise ValueError('Invalid Cron Schedule')
        now = datetime.datetime.now()
        for i in range(len(cron)):
            if cron[i] == '*':
                cron[i] = 0
            else:
                cron[i] = int(cron[i])
        yr = now.year
        min = cron[0] or now.minute
        hr = cron[1] or now.hour
        pydow = self._toPydow(cron[4])
        dom = cron[2] or self._dowToDom(pydow) or now.day
        mon = cron[3] or now.month
        return yr, min, hr, dom, mon

    def _dowToDom(self, dow) -> int:
        now = datetime.datetime.now()
        try:
            dom = now.day
            weekday = now.weekday
            day_delta = abs(weekday - dow)
            return datetime.timedelta(days=day_delta)
        except:
            return None

    def _toPydow(self, dow) -> int:
        '''
        Cron:   0 - 6 = Sunday - Saturday
        Python: 0 - 6 = Monday - Sunday
        '''
        try:
            return (dow - 1) % 7
        except:
            return None

    def _setDownstreams(self, topsort):
        '''Sets downstream node pointers for all nodes in DAG'''
        def visit_parallel_nodes(i, n):
            for j in range(len(topsort[i])):
                try:
                    topsort[i][j].downstream += list(topsort[n])
                except:
                    topsort[i][j].downstream.append(topsort[n])
        n = 0
        for i in range(len(topsort) - 1):
            if n + 1 < len(topsort): n += 1
            try:
                visit_parallel_nodes(i, n)
            except:
                try:
                    topsort[i].downstream += topsort[n]
                except:
                    topsort[i].downstream.append(topsort[n])
        return topsort
    
    def __str__(self) -> str:
        dagstr = ''
        for j in self.tasks:
            try:
                for i in j:
                    dagstr += f'{i.ident}, '
                dagstr = dagstr[:-2] + '\n'
            except:
                dagstr += f'{j.ident}\n'
        return dagstr
    
    def __lt__(self, other):
        return self.next_run_date < other.next_run_date

    def __le__(self, other):
        return self.next_run_date <= other.next_run_date

    def __eq__(self, other):
        return self.next_run_date == other.next_run_date

    def __ne__(self, other):
        return self.next_run_date != other.next_run_date

    def __gt__(self, other):
        return self.next_run_date > other.next_run_date

    def __ge__(self, other):
        return self.next_run_date >= other.next_run_date

    def __iter__(self) -> iter:
        return iter(self.tasks)