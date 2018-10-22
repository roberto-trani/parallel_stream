def get_emitter_from_iterable(iterable):
    assert hasattr(iterable, "__iter__")

    def emitter(outqueue):
        for job in iterable:
            outqueue.put(job)

    return emitter


def get_collector_to_list(list_object=None):
    assert list_object is None or isinstance(list_object, list)

    def collector(inqueue):
        if list_object is None:
            list_object = list()
        for value in inqueue:
            list_object.append(value)
        return list_object

    return collector


def get_collector_pairs_to_dict(dict_object=None):
    assert dict_object is None or isinstance(dict_object, dict)

    def collector(inqueue):
        if dict_object is None:
            dict_object = dict()
        for key, value in inqueue:
            dict_object[key] = value
        return dict_object

    return collector
