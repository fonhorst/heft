from algs.gsa.operators import Position


def schedule_to_position(schedule):
    """
    this function converts valid schedule
    to mapping and ordering strings
    """
    items = lambda: iter((item, node) for node, items in schedule.mapping.items() for item in items)
    if not all(i.is_unstarted() for i, _ in items()):
        raise ValueError("Schedule is not valid. Not all elements have unstarted state.")

    mapping = {i.job.id: n.name for i, n in items()}
    ordering = sorted([i for i, _ in items()], key=lambda x: x.start_time)
    ordering = [el.job.id for el in ordering]
    return Position(mapping, ordering)