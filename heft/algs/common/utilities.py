def mapping_as_vector(mapping):
    """
    mapping MUST be a dictionary
    converts position to a single vector by joining mapping and ordering structure
    all workflow tasks is sorted by task.id and thus it provides idempotentity for multiple runs
    """
    mapp_string = [node_name for task_id, node_name in sorted(mapping.items(), key=lambda x: x[0])]
    return mapp_string


def cannot_be_zero(number, replace_for_zero=0.000001):
    return replace_for_zero if round(abs(number), 6) < 0.000001 else number


def gather_info(logbook, stats, g, pop, need_to_print=True):
    data = stats.compile(pop) if stats is not None else None
    if logbook is not None:
        logbook.record(gen=g, evals=len(pop), **data)
        if need_to_print:
            print(logbook.stream)
    return data







