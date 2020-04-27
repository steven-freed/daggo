import threading

class PriorityQueue:

    def __init__(self):
        self._lock = threading.Lock()
        self._heap =  []
        self.parent = lambda i : (i - 1) // 2
        self.left =   lambda i : (i * 2) + 1
        self.right =  lambda i : (i * 2) + 2

    def peek(self):
        with self._lock:
            return self._heap[0:]

    def dequeue(self):
        with self._lock:
            try:
                last = len(self._heap) - 1
                self._heap[0], self._heap[last] = self._heap[last], self._heap[0]
                e = self._heap.pop()
                self._heapdown(0)
            except:
                e = None
            return e

    def enqueue(self, e):
        with self._lock:
            self._heap.append(e)
            self._heapup(len(self._heap) - 1)

    def _heapdown(self, i):
        last = len(self._heap) - 1
        left_index = self.left(i)
        right_index = self.right(i)
        if left_index <= last and self._heap[i] > self._heap[left_index]:
            self._heap[i], self._heap[left_index] = self._heap[left_index], self._heap[i]
            self._heapdown(left_index)
        if right_index <= last and self._heap[i] > self._heap[right_index]:
            self._heap[i], self._heap[right_index] = self._heap[right_index], self._heap[i]
            self._heapdown(right_index)

    def _heapup(self, i):
        parent_index = self.parent(i)
        if parent_index > -1:
            if self._heap[parent_index] > self._heap[i]:
                self._heap[i], self._heap[parent_index] = self._heap[parent_index], self._heap[i]
                self._heapup(parent_index)

    def __str__(self):
        with self._lock:
            return str(self._heap)
    
    def __iter__(self):
        with self._lock:
            self._nexti = 0
        return self
    
    def __next__(self):
        with self._lock:
            try:
                return self._heap[self._nexti]
            except (StopIteration, IndexError):
                raise