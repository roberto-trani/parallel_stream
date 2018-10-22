from parallel_stream import parallel_stream

# test
if __name__ == '__main__':
    import time

    def _get_emitter(num_messages):
        def _emitter(q):
            for i in xrange(0, num_messages):
                q.put(i)
        return _emitter

    def _worker(_, it, q):
        for i in it:
            q.put(2 * i)

    def _collector(it):
        s = 0
        for _ in it:
            s += 1
        return s

    tests = [
        {'n_jobs': 3},
        {'n_jobs': 5},
        {'n_jobs': 7},
        {'n_jobs': 3, 'emitter_output_chunk_size': 1},
        {'n_jobs': 3, 'emitter_output_chunk_size': 5},
        {'n_jobs': 5, 'emitter_output_chunk_size': 5},
        {'n_jobs': 5, 'worker_output_chunk_size': 1},
        {'n_jobs': 5, 'worker_output_chunk_size': 5},
        {'n_jobs': 7, 'emitter_output_chunk_size': 1, 'worker_output_chunk_size': 10},
        {'n_jobs': 7, 'worker_output_chunk_size': 1, 'collector_queue_size': 1},
        {'n_jobs': 3, 'emitter_output_chunk_size': 1, 'fork_emitter': False},
        {'n_jobs': 5, 'emitter_output_chunk_size': 1, 'fork_workers': False},
        {'n_jobs': 3, 'emitter_output_chunk_size': 1, 'fork_collector': False},
        {'n_jobs': 3, 'emitter_output_chunk_size': 1, 'fork_emitter': False, 'fork_collector': False},
        {'n_jobs': 3, 'emitter_output_chunk_size': 1, 'fork_emitter': False, 'fork_workers': False, 'fork_collector': False},
    ]

    for i, kargs in enumerate(tests):
        print "TEST {}) {}".format(i+1, kargs),
        outcome = "OK"
        start_time = time.time()
        for _ in range(3):
            for j in xrange(1, 150+1):
                expected_output = j
                output = parallel_stream(_get_emitter(expected_output), _worker, _collector, **kargs)
                if expected_output != output:
                    outcome = "NO the output is {} instead of {}".format(output, expected_output)
                    break
        print "\t {} {:.2f}".format(outcome, time.time() - start_time)
