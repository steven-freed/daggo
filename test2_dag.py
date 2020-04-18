from daggo.dag import DAGNode, DAG
from time import sleep
import os

def task():
    print(f'file 1: Node running in test2_daggo process {os.getpid()}')
    sleep(5)

a = DAGNode('a', task)
b = DAGNode('b', task)
c = DAGNode('c', task)
dag = DAG('test2_daggo', ((a, b), c), min=45) 
