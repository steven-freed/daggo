from server import DAGNode, DAG
from time import sleep
import os
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class A(DAGNode):
    def __init__(self, node_id):
        super().__init__(node_id)
    
    def __start__(self):
        log.info(f'Node "{self.node_id}" running in process {os.getpid()}')
        sleep(5)

class B(DAGNode):
    def __init__(self, node_id):
        super().__init__(node_id)
    
    def __start__(self):
        log.info(f'Node "{self.node_id}" running in process {os.getpid()}')
        sleep(5)

class C(DAGNode):
    def __init__(self, node_id):
        super().__init__(node_id)
    
    def __start__(self):
        log.info(f'Node "{self.node_id}" running in process {os.getpid()}')
        sleep(5)
   
class D(DAGNode):
    def __init__(self, node_id):
        super().__init__(node_id)
    
    def __start__(self):
        log.info(f'Node "{self.node_id}" running in process {os.getpid()}')
        sleep(5)

def main():
    a = A('a')
    b = B('b')
    c = C('c')
    d = D('d')
    
    dag = DAG('my schedge', a >> [b, c] >> d) 

if __name__ == '__main__':
    main()