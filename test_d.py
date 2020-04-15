from daggo import DAGNode, DAG
from time import sleep
import os
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def task():
    log.info(f'\tfile 2: Node running in process {os.getpid()}')
    sleep(5)

def main():
    e = DAGNode('e', task)
    f = DAGNode('f', task)
    g = DAGNode('g', task)
    h = DAGNode('h', task)
    dag = DAG('my schedge', [e, f] >> g >> h)
    
if __name__ == '__main__':
    main()
