# Daggo: Dependency Graph Job Scheduler
Designed by developers, made for humans

## Getting Started
1. create a dag job module, this must contain the word "dag" (e.g. test_dag.py)
2. create DAGNodes each with an identifier and callable task to run
3. create DAG with a topological sort of your DAGNodes and a cron schedule
```py
from time import sleep
import os

from daggo.dag import DAGNode, DAG


def task():
    print(f'\tfile 1: Node running in process {os.getpid()}')
    sleep(5)

a = DAGNode('a', task)
b = DAGNode('b', task)
c = DAGNode('c', task)
d = DAGNode('d', task)
e = DAGNode('e', task)
f = DAGNode('f', task)
# DAG will run the 0th minute of every hour, every day
dag = DAG('test_dag_id', (a, (b, c), (d, e,), f), min=0) 
```

## Concepts
### DAG
A DAG instance must be in global scope of your dag script, if declared inside a Callable (class or function)
then the DAG will not be scheduled by daggo.

*class* DAG
*params*
- ident     : str               identifier for your DAG
- topsort   : tuple or list     topological ordering of your DAG using an ordered iterable
- schedule  : str               cron schedule
- min       : int               cron minute
- hr        : int               cron hour
- dom       : int               cron day of month
- mon       : int               cron month
- dow       : int               cron day of week

#### Topological Sort for a DAG
DAG objects accept a topological sort of your dag nodes represented as an ordered iterable, though tuples are prefered because they are more memory efficient. To run two or more dag nodes in parallel you must specify those dag nodes as members of another iterable inside your topological sort.
```py
(a, b) or [a, b]
```

#### Scheduling a DAG
Scheduling a DAG can be done by using the kwarg 'schedule' which is a cron job string
```py
# runs dag the 5th minute of every hour, every day
DAG('test_dag_id', (a, b, c), schedule='5 * * * *')
```

You may also use all or a subset of date time kwargs that follow cron expression standards:
- min   (minute 0 - 59)
- hr    (hour 0 - 23)
- dom   (day of month 1 - 31)
- mon   (month 1 - 12)
- dow   (day of week 0-6 = Sunday - Saturday)
```py
# runs dag the 5th minute of every hour, every day
DAG('test_dag_id', (a, b, c), min=5)

# runs dag the 5th minute of every hour, every Monday
DAG('test_dag_id', (a, b, c), min=5, dow=1)
```
