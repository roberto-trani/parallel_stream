import threading
import multiprocessing
from threading import Thread
from multiprocessing import Process
from multiprocessing import cpu_count
from _core import ProcessQueue, ThreadQueue, LocalQueueBuffer, LocalQueueIterator


def _farm_emitter(emitter, outqueue, emitter_chunk_size, sh_worker_count, sh_condition):
    buffered_outqueue = LocalQueueBuffer(outqueue, emitter_chunk_size)
    try:
        emitter(buffered_outqueue)
    finally:
        buffered_outqueue.flush()
        # send a termination signal to each worker
        for _ in xrange(sh_worker_count.value):
            outqueue.put(None)

        with sh_condition:
            while sh_worker_count.value > 0:
                sh_condition.wait()


def _farm_worker(worker, worker_id, inqueue, outqueue, worker_chunk_size, sh_worker_count, sh_condition):
    buffered_outqueue = LocalQueueBuffer(outqueue, worker_chunk_size)
    try:
        worker(worker_id, LocalQueueIterator(inqueue), buffered_outqueue)
    finally:
        buffered_outqueue.flush()
        # send a termination signal to the collector
        outqueue.put(None)

        # notify the end of this worker
        with sh_condition:
            while sh_worker_count.value > 0:
                sh_condition.wait()


def _farm_collector(collector, inqueue, sh_worker_count, sh_condition):
    try:
        result = collector(LocalQueueIterator(inqueue, sh_worker_count, sh_condition))
        inqueue.put(result)
    finally:
        with sh_condition:
            sh_worker_count.value = 0
            sh_condition.notify_all()


def parallel_stream(
    emitter_fun,
    worker_fun,
    collector_fun,
    n_jobs=-1,
    emitter_output_chunk_size=1,
    worker_output_chunk_size=1,
    emitter_queue_size=0,
    collector_queue_size=0,
    fork_emitter=True,
    fork_workers=True,
    fork_collector=True
):
    """
    :param emitter_fun: a function that emits the values to process on the output queue
    :type emitter_fun: LocalQueueBuffer -> None
    :param worker_fun: a function that takes inputs from the input iterator and puts the outcomes on the output queue
    :type worker_fun: (int, LocalQueueIterator, LocalQueueBuffer) -> None
    :param collector_fun: a function that collects the outcomes from the input iterator
    :type collector_fun: (LocalQueueIterator) -> None
    :param n_jobs: the number of processes to use. If it is -1 uses the max between the number of cores and 3
    :type n_jobs: int
    :param emitter_output_chunk_size: the number of elements that will be sent at a time. It must be greater than 0
    :type emitter_output_chunk_size: int
    :param worker_output_chunk_size: the number of elements that will be sent at a time. It must be greater than 0
    :type worker_output_chunk_size: int
    :param emitter_queue_size: the size of the emitter system queue. If it is 0 a suggested value will be used, and if
    it less than 0 an unlimited queue will be used
    :type emitter_queue_size: int
    :param collector_queue_size: the size of the collector system queue. If it is 0 a suggested value will be used, and
    if it less than 0 an unlimited queue will be used
    :type collector_queue_size: int
    :param fork_emitter: True if the emitter must be executed in a separate Process, or in a Thread
    :type fork_emitter: bool
    :param fork_workers: True if the workers must be executed in a separate Process, or in a Thread
    :type fork_workers: bool
    :param fork_collector: True if the collector must be executed in a separate Process, or in a Thread
    :type fork_collector: bool
    """
    # check the output chunk size
    assert n_jobs < 0 or n_jobs >= 3
    assert emitter_output_chunk_size >= 1
    assert worker_output_chunk_size >= 1
    # compute the default value of the parameters when special values are selected
    if n_jobs < 0:
        n_jobs = max(cpu_count(), 3)

    worker_count = n_jobs - 2

    if emitter_queue_size == 0:
        emitter_queue_size = worker_count * 10
    elif emitter_queue_size < 0:
        emitter_queue_size = 0
    if collector_queue_size == 0:
        collector_queue_size = worker_count * 10
    elif collector_queue_size < 0:
        collector_queue_size = 0

    # intermediate queues
    q1 = (ProcessQueue if fork_emitter or fork_workers else ThreadQueue)(maxsize=emitter_queue_size)
    q2 = (ProcessQueue if fork_workers or fork_collector else ThreadQueue)(maxsize=collector_queue_size)
    # shared worker count and lock
    sh_worker_count = multiprocessing.Value('i', worker_count)
    sh_cond = threading.Condition() if not fork_emitter and not fork_workers and not fork_collector else multiprocessing.Condition()

    # init the processes / threads
    # constructors
    emitter_constr = Process if fork_emitter else Thread
    workers_constr = Process if fork_workers else Thread
    collector_constr = Process if fork_collector else Thread
    # init them
    emitter_proc = emitter_constr(
        name='Emitter',
        target=_farm_emitter,
        args=(emitter_fun, q1, emitter_output_chunk_size, sh_worker_count, sh_cond)
    )
    workers_procs = [
        workers_constr(
            name='Worker {}'.format(i + 1),
            target=_farm_worker,
            args=(worker_fun, i + 1, q1, q2, worker_output_chunk_size, sh_worker_count, sh_cond)
        )
        for i in xrange(worker_count)
    ]
    collector_proc = collector_constr(
        name='Collector',
        target=_farm_collector,
        args=(collector_fun, q2, sh_worker_count, sh_cond)
    )

    # start them
    for process in [emitter_proc] + workers_procs + [collector_proc]:
        process.daemon = True  # it lives only if the parent process lives too
        process.start()

    # join them
    try:
        # wait the emitter_proc
        emitter_proc.join()
        # wait each worker
        for w in workers_procs:
            w.join()
        collector_proc.join()

        # the result is in q2
        result = q2.get()
        # close the two queues
        q1.close()
        q2.close()
        # return the result
        return result

    except (KeyboardInterrupt, Exception):
        for _ in xrange(sh_worker_count.value):
            q1.put(None)
            q2.put(None)
        q1.close()
        q2.close()

        # kill all the processes/threads that are still alive
        if emitter_proc.is_alive() and fork_emitter:
            emitter_proc.terminate()
        for w in workers_procs:
            if w.is_alive() and fork_workers:
                w.terminate()
        if collector_proc.is_alive() and fork_collector:
            collector_proc.terminate()
        raise

