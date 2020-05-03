import threading
from dag import __FAILED__, __INACTIVE__, __PENDING__, __QUEUED__, __RUNNING__

class Bus(threading.Thread):
    '''Deamon thread joins all Runner Processes, aka cleans up tasks'''
    def __init__(self, lock, tsq, db, dags):
        super(Bus, self).__init__(name='Bus')
        self.lock = lock
        self.tsq = tsq
        self.db = db
        self.dags = dags
    
    def run(self):
        while True:
            runner = self.tsq.get()
            runner.join(timeout=1)
            if not runner.is_alive():
                dag = self.db[runner.name]
                self.dags.enqueue(dag)
                with self.lock:
                    dag.status = __QUEUED__
            else:
                self.tsq.put(runner)