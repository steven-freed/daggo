from daggo import DAGNode, DAG
from time import sleep
import os
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def task():
    log.info(f'\tfile 1: Node running in process {os.getpid()}')
    sleep(5)

def main():
    a = DAGNode('a', task)
    b = DAGNode('b', task)
    c = DAGNode('c', task)
    d = DAGNode('d', task)
    dag = DAG('my schedge', a >> [b, c] >> d) 

if __name__ == '__main__':
    main()