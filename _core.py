import threading
import multiprocessing
import collections


class ProcessQueue():
    def __init__(self, maxsize=0):
        self._maxsize = maxsize
        self._dequeu = multiprocessing.Queue(maxsize=maxsize)

    def put(self, obj):
        self._dequeu.put(obj)

    def get(self):
        return self._dequeu.get()

    def close(self):
        self._dequeu.close()


class ThreadQueue(object):
    def __init__(self, maxsize=0):
        self._maxsize = maxsize
        self._deque = collections.deque()
        self._closed = False
        self._lock = threading.RLock()
        self._get_cond = threading.Condition(self._lock)
        self._put_cond = threading.Condition(self._lock)

    def put(self, obj):
        with self._lock:
            assert not self._closed

            # check the queue constraint
            if self._maxsize > 0:
                while len(self._deque) >= self._maxsize:
                    self._get_cond.wait()
                    assert not self._closed
            # put the element into the queue
            self._deque.append(obj)
            self._put_cond.notify()

    def get(self):
        with self._lock:
            assert not self._closed

            # check if there are elements to get
            while len(self._deque) == 0:
                self._put_cond.wait()
                assert not self._closed

            # get the element from the queue
            obj = self._deque.popleft()
            self._get_cond.notify()
            return obj

    def close(self):
        with self._lock:
            if not self._closed:
                self._deque.clear()
                self._closed = True
                self._get_cond.notify_all()
                self._put_cond.notify_all()


class LocalQueueBuffer(object):
    def __init__(self, queue, chunk_size):
        self._queue = queue  # :type: Queue
        self._chunk = []
        self._chunk_size = chunk_size

    def put(self, job):
        self._chunk.append(job)
        if len(self._chunk) >= self._chunk_size:
            self._queue.put(self._chunk)
            self._chunk = []

    def flush(self):
        if len(self._chunk):
            self._queue.put(self._chunk)
            self._chunk = []


class LocalQueueIterator(object):
    def __init__(self, queue, num_producer=None, num_producer_lock=None):
        self._queue = queue
        self._num_producer = num_producer
        self._num_producer_lock = num_producer_lock

    def __iter__(self):
        while True:
            jobs = self._queue.get()

            # end of stream
            if jobs is None:
                if self._num_producer is None:
                    break
                else:
                    with self._num_producer_lock:
                        self._num_producer.value -= 1
                        if self._num_producer.value == 0:
                            break
                        else:
                            continue

            for job in jobs:
                yield job
