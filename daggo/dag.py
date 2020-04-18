import datetime, logging
from typing import Type, Callable, Dict, List, Union, Tuple, Set, TypeVar

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class DAGNode:
    '''
    Base class for DAGNodes basic requirements
    '''
    def __init__(self, ident: str, task: Callable) -> None:
        self.ident = ident
        self.task = task
        self.downstream = []


class DAG:
    '''
    DAG object for setting schedule, topological sort, configurations
    and serializing for the DAG Runner
    '''
    def __init__(self, ident: str, topsort,
                schedule: str = None,
                min: int = None, hr: int = None, dom: int = None,
                mon: int = None, dow: int = None) -> None:
        self.ident = ident
        self.cron = self._set_schedule(schedule or (min, hr, dom, mon, dow))
        self.tasks = self._set_downstreams(topsort)
        self.next_run_date = datetime.datetime.now()
        self._set_next_run_date()

    def _set_next_run_date(self) -> None:
        now = datetime.datetime.now()
        self.next_run_date = datetime.datetime(now.year,
                                self.cron['mon'] or now.month,
                                self.cron['dom'] or self._dow_to_dom(self.cron['dow']) or now.day,
                                hour=self.cron['hr'] or now.hour,
                                minute=self.cron['min'] or now.minute)

    def _dow_to_dom(self, dow) -> int:
        now = datetime.datetime.now()
        try:
            dom = now.day
            weekday = now.weekday
            day_delta = abs(weekday - dow)
            return datetime.timedelta(days=day_delta)
        except:
            return None

    def _to_pydow(self, dow) -> int:
        '''
        Cron:   0 - 6 = Sunday - Saturday
        Python: 0 - 6 = Monday - Sunday
        '''
        try:
            return (dow - 1) % 7
        except:
            return None

    def _set_schedule(self, schedule: Union[str, Tuple[int]]) -> Dict[str, int]:
        '''Parses schedule string or kwargs to cron tuple: (min, hr, dom, mon, dow)'''
        map = {
            0: 'min',
            1: 'hr',
            2: 'dom',
            3: 'mon',
            4: 'dow'
        }
        cron = {}
        try:
            schedule = schedule.split(' ')
        except:
            pass

        for i, metric in enumerate(schedule):
            if metric == '*':
                cron[map[i]] = None
            elif map[i] == 'dow':
                cron[map[i]] = self._to_pydow(metric)
            else:
                cron[map[i]] = metric
        return cron

    def _set_downstreams(self, jobs):
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

    def set_next_run_date(self):
        pass
    
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