from daggo.dag import DAGNode, DAG
from time import sleep
import os

def task():
    print(f'file 1: Node running in test_daggo process {os.getpid()}')
    sleep(2)

a = DAGNode('a', task)
b = DAGNode('b', task)
dag = DAG('test_daggo', (a, b), min=43) 
