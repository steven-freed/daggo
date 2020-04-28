from queue import Queue
import threading

class Bus(threading.Thread):
    '''Deamon thread joins all Runner Processes, aka cleans up tasks'''
    def __init__(self):
        super(Bus, self).__init__(name='Bus', daemon=True)
        # thread safe queue
        self.tsq = Queue()
    
    def run(self):
        while True:
            runner = self.tsq.get()
            runner.join(timeout=1)
            if not runner.is_alive():
                print('joined process', runner.name)
            else:
                self.tsq.put(runner)