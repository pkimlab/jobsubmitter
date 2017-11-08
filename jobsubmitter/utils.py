def iterate_parameters(global_params=None, **parameter_grid):
    """.

    Parameters
    ----------
    parameter_grid : dict
        Keys are parameters. Values are lists of values that the parameters can take.

    Examples
    --------
    >>> from pprint import pprint
    >>> iterable = iterate_parameters(a=[1, 2], b=[3, 4])
    >>> pprint(sorted(iterable, key=lambda x: (x['a'], x['b'])))
    [{'a': 1, 'b': 3}, {'a': 1, 'b': 4}, {'a': 2, 'b': 3}, {'a': 2, 'b': 4}]
    """
    # Don't modify dictionaries in-place
    if global_params is None:
        params = dict()
    else:
        params = global_params.copy()
    # Terminal case
    if not parameter_grid:
        yield params
        return
    # Recurse
    key, values = parameter_grid.popitem()
    for value in values:
        params[key] = value
        try:
            yield from iterate_parameters(params.copy(), **parameter_grid)
        except StopIteration:
            continue
