import threading
import multiprocessing as mp
import sys
import glob
import importlib
import logging
import subprocess
import os
from time import sleep

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

'''
if file mod-ed: update time, insert file name into dict for key time
set schedule as datetime.datetime object
'''

class DAGNode:
    '''
    min hr dom mon dow
    '''
    def __init__(self, identifier, callee, schedule=''):
        self.identifier = identifier
        self.callee = callee
        self.upstream = []

    def set_upstream(self, nodes):
        try:
            self.upstream = list(nodes)
        except TypeError:
            self.upstream = [nodes]

    def __rshift__(self, downstream):
        try:
            [node.set_upstream(self) for node in downstream]
        except:
            downstream.set_upstream(self)
        return downstream

    def __rrshift__(self, downstream):
        self.set_upstream(downstream)
        return self

    def __lshift__(self, downstream):
        try:
            [node.set_upstream(self) for node in downstream]
        except:
            downstream.set_upstream(self)
        return downstream

    def __lrshift__(self, downstream):
        self.set_upstream(downstream)
        return self


class DAG:
    def __init__(self, schedule, sinks):
        self.schedule = schedule
        self.dag_nodes = []
        self.stream = []
        if type(sinks) != list:
            sinks = [sinks]
        topsort = self._topsort(sinks)
        self.stream = self._parallelize(topsort)
        Runner.run(self)

    def _parallelize(self, topsort):
        i = 0
        sched = [-1] * len(topsort)
        sched[0] = [topsort[0]]
        deps = topsort[0].upstream
        for node in topsort[1:]:
            if node.upstream == deps:
                sched[i].append(node)
            else:
                i += 1
                deps = node.upstream
                sched[i] = [node]
        return sched[:sched.index(-1)]

    def _topsort(self, sinks):
        def visit_upstream(node, visited, stack):
            visited[node] = True
            for n in node.upstream:
                if not visited.get(n):
                    visit_upstream(n, visited, stack)
            stack.append(node)

        visited = dict.fromkeys(sinks, False)
        stack = []
        for node in sinks:
            if not visited[node]:
                visit_upstream(node, visited, stack)
        return stack


class Runner:

    dag_files = {}
    
    @staticmethod
    def run(dag):
        for node_or_nodes in dag.stream:
            if type(node_or_nodes) == list:
                for n in node_or_nodes:
                    proc = mp.Process(target=n.callee)
                    proc.start()
                proc.join()
                print()
            else:
                proc = mp.Process(target=node_or_nodes.callee)
                proc.start()
                proc.join()
                print()
    
    @staticmethod
    def watch():
        while True:
            files = glob.glob("*_dag.py")
            for f in files:
                if not Runner.dag_files.get(f):
                    Runner.dag_files[f] = 0
                new_time = os.stat(f).st_mtime
                old_time = Runner.dag_files.get(f)
                if old_time < new_time:
                    Runner.dag_files[f] = new_time
                    log.info(f'EXEC {f}')
                    mod = importlib.import_module(f[:-3])
                    p = mp.Process(target=mod.main)
                    p.run()
            sleep(5)

if __name__ == '__main__':
    watcher = mp.Process(target=Runner.watch)
    watcher.start()
    watcher.join()
