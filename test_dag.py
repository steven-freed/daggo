from daggo.dag import DAGNode, DAG
from time import sleep
import os

def task():
    print(f'file 1: Node running in test_daggo process {os.getpid()}')
    sleep(5)

a = DAGNode('a', task)
b = DAGNode('b', task)
c = DAGNode('c', task)
d = DAGNode('d', task)
e = DAGNode('e', task)
f = DAGNode('f', task)
dag = DAG('test_daggo', (a, (b, c), (d, e), f), min=45) 
