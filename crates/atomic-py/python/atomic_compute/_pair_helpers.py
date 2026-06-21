"""Partition-level aggregation helpers shipped to workers via cloudpickle.

Each function here is called on the driver to produce a closure that is then
cloudpickle-serialized and sent inside a PythonTaskPayload to workers. Workers
execute the closure against their local partition slice and return a compact
partial result, avoiding large intermediate data transfers to the driver.
"""


def make_partial_reduce_fn(f):
    """Return a partition fn that does local reduce_by_key, used as a Map op on workers."""
    def _partial_reduce(partition):
        acc = {}
        for pair in partition:
            k, v = pair[0], pair[1]
            acc[k] = f(acc[k], v) if k in acc else v
        return list(acc.items())
    return _partial_reduce


def make_partial_group_fn():
    """Return a partition fn that does local group_by_key, used as a Map op on workers."""
    def _partial_group(partition):
        acc = {}
        for pair in partition:
            k, v = pair[0], pair[1]
            if k in acc:
                acc[k].append(v)
            else:
                acc[k] = [v]
        return list(acc.items())
    return _partial_group


def make_join_fn(right_json):
    """Return a partition fn that does inner join against a pre-serialized right side."""
    import json as _json
    right = {}
    for p in _json.loads(right_json):
        k = p[0]
        if k not in right:
            right[k] = []
        right[k].append(p[1])

    def _join(left_partition):
        result = []
        for pair in left_partition:
            lk, lv = pair[0], pair[1]
            if lk in right:
                for rv in right[lk]:
                    result.append([lk, [lv, rv]])
        return result
    return _join


def make_left_outer_join_fn(right_json):
    """Return a partition fn that does left outer join against a pre-serialized right side."""
    import json as _json
    right = {}
    for p in _json.loads(right_json):
        k = p[0]
        if k not in right:
            right[k] = []
        right[k].append(p[1])

    def _left_outer_join(left_partition):
        result = []
        for pair in left_partition:
            lk, lv = pair[0], pair[1]
            if lk in right:
                for rv in right[lk]:
                    result.append([lk, [lv, rv]])
            else:
                result.append([lk, [lv, None]])
        return result
    return _left_outer_join
