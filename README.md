# Daggo
## Dependency Graph Job Scheduler

Designed by developers, made for humans

### Getting Started
1. create a dag job script (e.g. test_job.py)
2. create DAGNodes each with an identifier and callable task to run
3. create DAG with a cron schedule to run and a topological sort of your DAGNodes
```py
from time import sleep
import os

from daggo import DAGNode, DAG


def task():
    print(f'DAGNode running task in process {os.getpid()}')
    sleep(5)

def main():
    a = DAGNode('a', task)
    b = DAGNode('b', task)
    c = DAGNode('c', task)
    d = DAGNode('d', task)
    e = DAGNode('e', task)
    f = DAGNode('f', task)

    DAG('* * * * *', [a, (b, c), (d, e,), f]) 

if __name__ == '__main__':
    main()
```

### Concepts
#### Topological Sort for DAG
DAG objects accept a topological sort of your dag nodes represented as an iterable (list, tuple, set, etc.).
To run two or more dag nodes in parallel you must specify those dag nodes as an iterable (or optional ordering in the topological sort)
```py
(a, b) or [a, b] or {a, b}
```